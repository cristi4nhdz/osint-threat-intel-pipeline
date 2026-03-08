# ingestion/base_producer.py
"""Connects to the Kafka server, and sends data to the correct topics."""

import json
import logging
from datetime import datetime, timezone
from kafka import KafkaProducer
from config.decorator import retry
from config.config_loader import load_config

logger = logging.getLogger(__name__)


class BaseProducer:
    """Base class for Kafka producers."""

    topics: dict[str, str]
    producer: KafkaProducer

    def __init__(self) -> None:
        """Loads config and initializes Kafka producer."""
        config = load_config()
        self.topics = config["kafka"]["topics"]
        self.connect_kafka(config["kafka"]["bootstrap_servers"])
        logger.info("Kafka producer connected.")

    @retry(max_attempts=3, delay=1.0, backoff=2.0)
    def connect_kafka(self, bootstrap: list[str]) -> None:
        """Connect to Kafka broker with retry on failure."""
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
        )

    @retry(max_attempts=3, delay=1.0, backoff=2.0)
    def publish(self, topic: str, message: dict, key: str | None = None) -> None:
        """Stamp and publish a message to a Kafka topic."""
        stamped = {**message, "_ingested_at": datetime.now(timezone.utc).isoformat()}
        self.producer.send(topic, value=stamped, key=key).get(timeout=10)
        logger.info("Published to %s: %s", topic, stamped.get("title", key))

    def flush(self) -> None:
        """Flush all pending messages to Kafka."""
        self.producer.flush()

    def close(self) -> None:
        """Close the Kafka producer."""
        self.producer.close()
        logger.info("Kafka producer closed.")
