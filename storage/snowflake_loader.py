# storage/snowflake_loader.py
"""Consumes enriched messages from Kafka and loads them into Snowflake."""

import json
import logging
import uuid
import snowflake.connector
from kafka import KafkaConsumer
from config.decorator import retry
from config.config_loader import load_config

logger = logging.getLogger(__name__)


class SnowflakeLoader:
    """Loads enriched articles from Kafka into Snowflake."""

    input_topic: str
    consumer: KafkaConsumer

    def __init__(self) -> None:
        """Initialize config, Kafka, and Snowflake connections."""

        config = load_config()
        bootstrap: list[str] = config["kafka"]["bootstrap_servers"]
        topics: dict[str, str] = config["kafka"]["topics"]

        sf: dict = config["snowflake"]
        self.input_topic = topics["enriched"]
        self.connect_snowflake(sf)
        self.connect_kafka(bootstrap)

    @retry(max_attempts=3, delay=1.0, backoff=2.0)
    def connect_kafka(self, bootstrap: list[str]) -> None:
        """Connect Kafka consumer to the enriched topic."""

        self.consumer = KafkaConsumer(
            self.input_topic,
            bootstrap_servers=bootstrap,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="snowflake-loader-group",
            max_poll_interval_ms=600000,
            max_poll_records=50,
        )
        logger.info("Kafka consumer ready.")

    @retry(max_attempts=3, delay=1.0, backoff=2.0)
    def connect_snowflake(self, sf: dict) -> None:
        """Open Snowflake connection and cursor."""

        self.conn = snowflake.connector.connect(
            account=sf["account"],
            user=sf["user"],
            password=sf["password"],
            warehouse=sf["warehouse"],
            database=sf["database"],
            schema=sf["schema"],
        )
        self.cur = self.conn.cursor()
        logger.info("Snowflake connection ready.")

    def run(self) -> int:
        """Consume messages from Kafka and insert articles into Snowflake in batches."""
        logger.info("Listening on %s", self.input_topic)
        count = 0
        batch: list[dict] = []
        batch_size = 50

        while True:
            records = self.consumer.poll(timeout_ms=600000)
            if not records:
                if batch:
                    count += self.write_snowflake_batch(batch)
                    batch = []
                break
            for tp, messages in records.items():
                for message in messages:
                    batch.append(message.value)
                    if len(batch) >= batch_size:
                        count += self.write_snowflake_batch(batch)
                        logger.info("Batch complete, %d total articles loaded", count)
                        batch = []

        if batch:
            count += self.write_snowflake_batch(batch)
            logger.info("Final batch loaded, total %d articles", count)

        logger.info("Loaded %d articles to Snowflake", count)
        return count

    def write_snowflake_batch(self, batch: list[dict]) -> int:
        """Insert a batch of enriched articles into Snowflake."""
        count = 0
        for article in batch:
            try:
                self.insert_article(article)
                count += 1
            except Exception as e:
                logger.error(
                    "Failed to insert article %s: %s", article.get("title", "")[:60], e
                )
        return count

    def insert_article(self, article: dict) -> None:
        """Insert enriched article into Snowflake with URL deduplication."""
        title = article.get("title", "")
        url = article.get("original_url", "")

        self.cur.execute(
            """
            INSERT INTO threat_articles (
                id, title, source, original_url, published_at,
                threat_actors, malware, locations, persons,
                organizations, attack_techniques, relevance_score, enriched_at
            )
            SELECT
                %s, %s, %s, %s, %s::TIMESTAMP_TZ,
                PARSE_JSON(%s), PARSE_JSON(%s), PARSE_JSON(%s), PARSE_JSON(%s),
                PARSE_JSON(%s), PARSE_JSON(%s), %s, %s::TIMESTAMP_TZ
            WHERE NOT EXISTS (
                SELECT 1
                FROM threat_articles
                WHERE original_url = %s
            )
            """,
            (
                str(uuid.uuid4()),
                title,
                article.get("source", ""),
                url,
                article.get("published_at", ""),
                json.dumps(article.get("threat_actors", [])),
                json.dumps(article.get("malware", [])),
                json.dumps(article.get("locations", [])),
                json.dumps(article.get("persons", [])),
                json.dumps(article.get("organizations", [])),
                json.dumps(article.get("attack_techniques", [])),
                article.get("relevance_score", 0.0),
                article.get("_enriched_at", ""),
                url,
            ),
        )

        if self.cur.rowcount == 1:
            logger.info("Inserted: %s", title[:60])
        else:
            logger.info("Duplicate skipped: %s", title[:60])

    def close(self) -> None:
        """Close Kafka consumer and Snowflake connection."""
        self.consumer.close()
        self.cur.close()
        self.conn.close()
        logger.info("Snowflake loader closed.")
