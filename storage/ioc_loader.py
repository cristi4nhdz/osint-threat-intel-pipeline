# storage/ioc_loader.py
"""Loads IOCs from Kafka into Snowflake and Neo4j, linking to existing actor/malware nodes."""

import json
import logging
from datetime import datetime, timezone
from kafka import KafkaConsumer
from neo4j import GraphDatabase
import snowflake.connector
from config.config_loader import load_config
from processing.actor_data import ACTOR_NORMALIZE, ACTOR_ORIGINS

logger = logging.getLogger(__name__)

# Known actor names that ThreatFox sometimes puts in the malware_family field
ACTOR_NAMES_LOWER = {k.lower() for k in ACTOR_NORMALIZE.values()} | set(
    ACTOR_ORIGINS.keys()
)

IOC_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS THREAT_INTEL.PUBLIC.THREAT_IOCS (
    id STRING PRIMARY KEY,
    ioc_type STRING,
    ioc_value STRING,
    threat_type STRING,
    malware_family STRING,
    malware_alias STRING,
    threat_actor STRING,
    confidence INT,
    tags ARRAY,
    reporter STRING,
    reference STRING,
    first_seen TIMESTAMP_TZ,
    last_seen TIMESTAMP_TZ,
    source STRING,
    inserted_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
)
"""

IOC_INSERT = """
INSERT INTO THREAT_INTEL.PUBLIC.THREAT_IOCS
    (id, ioc_type, ioc_value, threat_type, malware_family, malware_alias,
     threat_actor, confidence, tags, reporter, reference, first_seen, last_seen, source)
SELECT
    %(id)s, %(ioc_type)s, %(ioc_value)s, %(threat_type)s,
    %(malware_family)s, %(malware_alias)s, %(threat_actor)s, %(confidence)s,
    PARSE_JSON(%(tags)s), %(reporter)s, %(reference)s,
    TRY_TO_TIMESTAMP_TZ(%(first_seen)s),
    TRY_TO_TIMESTAMP_TZ(%(last_seen)s),
    %(source)s
WHERE NOT EXISTS (
    SELECT 1 FROM THREAT_INTEL.PUBLIC.THREAT_IOCS WHERE id = %(id)s
)
"""


def parse_timestamp(raw: str) -> str:
    """Normalize a timestamp string for Snowflake, return empty on failure."""
    if not raw:
        return ""
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).isoformat()
    except (ValueError, TypeError):
        pass
    try:
        return (
            datetime.strptime(raw, "%Y-%m-%d %H:%M:%S %Z")
            .replace(tzinfo=timezone.utc)
            .isoformat()
        )
    except (ValueError, TypeError):
        return raw


def normalize_malware(name: str) -> str:
    """Clean malware family names from Abuse.ch format."""
    if not name:
        return ""
    if "." in name:
        name = name.split(".", 1)[-1]
    cleaned = name.replace("_", " ").strip().title()
    if cleaned.lower() in ACTOR_NAMES_LOWER:
        return ""
    return cleaned


def extract_actor(ioc: dict) -> str:
    """Extract threat actor from IOC malware_family field or tags."""
    raw_malware = ioc.get("malware_family", "")
    if raw_malware:
        if "." in raw_malware:
            raw_malware = raw_malware.split(".", 1)[-1]
        clean = raw_malware.replace("_", " ").strip()
        actor = ACTOR_NORMALIZE.get(clean.lower())
        if actor:
            return actor

    for tag in ioc.get("tags", []) or []:
        actor = ACTOR_NORMALIZE.get(tag.lower().strip())
        if actor:
            return actor
    return ""


def create_ioc_graph(tx, ioc: dict) -> None:
    """Merge an IOC node in Neo4j and link it to Malware and ThreatActor nodes."""
    ioc_value = ioc.get("ioc_value", "")
    if not ioc_value:
        return
    tx.run(
        """
        MERGE (i:IOC {value: $value})
        SET i.type = $type,
            i.source = $source,
            i.confidence = $confidence,
            i.first_seen = $first_seen,
            i.tags = $tags
        """,
        value=ioc_value,
        type=ioc.get("ioc_type", ""),
        source=ioc.get("source", ""),
        confidence=ioc.get("confidence", 0),
        first_seen=ioc.get("first_seen", ""),
        tags=ioc.get("tags", []) or [],
    )

    malware = ioc.get("malware_family", "")
    if malware:
        tx.run(
            """
            MERGE (i:IOC {value: $ioc_value})
            MERGE (m:Malware {name: $malware})
            MERGE (i)-[:DELIVERS]->(m)
            """,
            ioc_value=ioc_value,
            malware=malware,
        )

    actor = ioc.get("threat_actor", "")
    if actor:
        tx.run(
            """
            MERGE (i:IOC {value: $ioc_value})
            MERGE (a:ThreatActor {name: $actor})
            MERGE (i)-[:ATTRIBUTED_TO]->(a)
            """,
            ioc_value=ioc_value,
            actor=actor,
        )


class IOCLoader:
    """Consumes IOC messages from Kafka and writes them to Snowflake and Neo4j."""

    consumer: KafkaConsumer
    sf_conn: snowflake.connector.SnowflakeConnection
    neo4j_driver: GraphDatabase.driver

    def __init__(self) -> None:
        """Initialize Snowflake, Neo4j, and Kafka connections."""
        config = load_config()
        sf = config["snowflake"]
        neo = config["neo4j"]
        kafka = config["kafka"]

        self.sf_conn = snowflake.connector.connect(
            account=sf["account"],
            user=sf["user"],
            password=sf["password"],
            database=sf["database"],
            schema=sf["schema"],
            warehouse=sf["warehouse"],
        )
        self.setup_table()

        self.neo4j_driver = GraphDatabase.driver(
            neo["uri"], auth=(neo["user"], neo["password"])
        )

        self.consumer = KafkaConsumer(
            kafka["topics"]["iocs"],
            bootstrap_servers=kafka["bootstrap_servers"],
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="ioc-loader-group",
            max_poll_interval_ms=600000,
            max_poll_records=100,
        )

        logger.info("IOC loader ready.")

    def setup_table(self) -> None:
        """Create the THREAT_IOCS table if it does not exist."""
        cursor = self.sf_conn.cursor()
        try:
            cursor.execute(IOC_TABLE_DDL)
            logger.info("THREAT_IOCS table ready.")
        finally:
            cursor.close()

    def load(self) -> None:
        """Poll Kafka and write IOCs to Snowflake and Neo4j in batches."""
        sf_count = 0
        neo_count = 0
        batch: list[dict] = []
        batch_size = 100

        while True:
            records = self.consumer.poll(timeout_ms=600000)
            if not records:
                if batch:
                    sf_count += self.write_snowflake_batch(batch)
                    neo_count += self.write_neo4j_batch(batch)
                    batch = []
                break
            for tp, messages in records.items():
                for message in messages:
                    ioc = message.value
                    ioc["threat_actor"] = extract_actor(ioc)
                    ioc["malware_family"] = normalize_malware(
                        ioc.get("malware_family", "")
                    )
                    batch.append(ioc)
                    if len(batch) >= batch_size:
                        sf_count += self.write_snowflake_batch(batch)
                        neo_count += self.write_neo4j_batch(batch)
                        logger.info(
                            "Batch complete, %d SF, %d Neo4j total",
                            sf_count,
                            neo_count,
                        )
                        batch = []
        logger.info("Loaded %d IOCs to Snowflake, %d to Neo4j.", sf_count, neo_count)

    def write_snowflake_batch(self, batch: list[dict]) -> int:
        """Insert a batch of IOC records into Snowflake."""
        count = 0
        cursor = self.sf_conn.cursor()
        try:
            for ioc in batch:
                try:
                    cursor.execute(
                        IOC_INSERT,
                        {
                            "id": ioc.get("id", ""),
                            "ioc_type": ioc.get("ioc_type", ""),
                            "ioc_value": ioc.get("ioc_value", ""),
                            "threat_type": ioc.get("threat_type", ""),
                            "malware_family": ioc.get("malware_family", ""),
                            "malware_alias": ioc.get("malware_alias", ""),
                            "threat_actor": ioc.get("threat_actor", ""),
                            "confidence": ioc.get("confidence", 0),
                            "tags": json.dumps(ioc.get("tags", [])),
                            "reporter": ioc.get("reporter", ""),
                            "reference": ioc.get("reference", ""),
                            "first_seen": parse_timestamp(ioc.get("first_seen", "")),
                            "last_seen": parse_timestamp(ioc.get("last_seen", "")),
                            "source": ioc.get("source", ""),
                        },
                    )
                    count += 1
                except Exception as e:
                    logger.error("Snowflake insert failed for %s: %s", ioc.get("id"), e)
        finally:
            cursor.close()
        return count

    def write_neo4j_batch(self, batch: list[dict]) -> int:
        """Insert a batch of IOC records into Neo4j."""
        count = 0
        with self.neo4j_driver.session() as session:
            for ioc in batch:
                try:
                    session.execute_write(create_ioc_graph, ioc)
                    count += 1
                except Exception as e:
                    logger.error("Neo4j insert failed for %s: %s", ioc.get("id"), e)
        return count

    def close(self) -> None:
        """Close Kafka consumer, Neo4j driver, and Snowflake connection."""
        self.consumer.close()
        self.neo4j_driver.close()
        self.sf_conn.close()
        logger.info("IOC loader closed.")
