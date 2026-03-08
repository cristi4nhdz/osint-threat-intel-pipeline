# processing/enrichment_consumer.py
"""Consumes articles from kafka, enriches them, and publishes the results"""

import json
import logging
from datetime import datetime, timezone
from kafka import KafkaConsumer, KafkaProducer
from config.decorator import retry
from config.config_loader import load_config
from processing.entity_extractor import EntityExtractor

logger = logging.getLogger(__name__)


class EnrichmentConsumer:
    """Consume articles, enriches them, and publishes them to kafka."""

    min_relevance_score: float
    input_topic: str
    output_topic: str
    extractor: EntityExtractor
    consumer: KafkaConsumer
    producer: KafkaProducer

    def __init__(self, min_relevance_score: float = 0.2) -> None:
        """Initialize Kafka consumer, producer, and entity extractor."""
        config = load_config()
        bootstrap: list[str] = config["kafka"]["bootstrap_servers"]
        topics: dict[str, str] = config["kafka"]["topics"]

        self.min_relevance_score = min_relevance_score
        self.input_topic = topics["news"]
        self.output_topic = topics["enriched"]
        self.extractor = EntityExtractor()

        self.connect_kafka(bootstrap)

        logger.info("Enrichment consumer ready.")

    @retry(max_attempts=3, delay=1.0, backoff=2.0)
    def connect_kafka(self, bootstrap: list[str]) -> None:
        """Connect to Kafka broker with retry on failure."""
        self.consumer = KafkaConsumer(
            self.input_topic,
            bootstrap_servers=bootstrap,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="enrichment-consumer-group",
        )

        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
        )

    @retry(max_attempts=3, delay=1.0, backoff=2.0)
    def publish(self, value: dict, key: str | None = None) -> None:
        """Send a message to the output topic with timestamp and logging."""
        stamped = {**value, "_enriched_at": datetime.now(timezone.utc).isoformat()}
        self.producer.send(self.output_topic, value=stamped, key=key).get(timeout=10)
        logger.info("Published enriched article: %s", stamped.get("title", key))

    def flush(self) -> None:
        """Flush all pending messages to Kafka."""
        self.producer.flush()

    def run(self) -> None:
        """Listens for articles in Kafka, enriches them, and sends them to output topic"""
        logger.info("Listening on %s", self.input_topic)
        for message in self.consumer:
            article: dict = message.value
            try:
                enriched: dict = self.extractor.extract(article)

                if enriched["relevance_score"] < self.min_relevance_score:
                    logger.info(
                        "Skipped (low relevance %s): %s",
                        enriched["relevance_score"],
                        enriched["title"],
                    )
                    continue

                self.publish(enriched, key=enriched["original_url"])

                self.flush()

            except Exception as e:
                logger.error("Failed to process article: %s", e)
                continue

    def close(self) -> None:
        """Close the Kafka consumer and producer."""
        self.consumer.close()
        self.producer.close()
        logger.info("Enrichment consumer closed.")
