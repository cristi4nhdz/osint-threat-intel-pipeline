# storage/s3_archiver.py
"""Archives Kafka messages to S3 in JSON batches for downstream processing."""

import json
import logging
from datetime import datetime
from typing import Optional
import boto3
from kafka import KafkaConsumer
from config.config_loader import load_config
from config.decorator import retry

logger = logging.getLogger(__name__)


class S3Archiver:
    """Archives Kafka messages to S3 using logic matching SnowflakeLoader."""

    def __init__(self, topic: str, bucket: Optional[str] = None):
        """Initialize S3 archiver for a specific topic."""
        config = load_config()

        self.topic = topic
        self.bucket = bucket or config["aws"]["bucket_name"]

        # Initialize S3 client
        self.s3 = boto3.client(
            "s3",
            region_name=config["aws"]["region"],
            aws_access_key_id=config["aws"]["access_key_id"],
            aws_secret_access_key=config["aws"]["secret_access_key"],
        )

        kafka_config = config["kafka"]
        self.connect_kafka(kafka_config["bootstrap_servers"])

        logger.info(
            "S3 Archiver ready for topic %s → bucket %s", self.topic, self.bucket
        )

    @retry(max_attempts=3, delay=1.0, backoff=2.0)
    def connect_kafka(self, bootstrap: list[str]) -> None:
        """Connect Kafka consumer with specific batch and interval settings."""
        self.consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=bootstrap,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id=f"s3-archiver-{self.topic}",
            max_poll_interval_ms=300000,
            max_poll_records=50,
        )
        logger.info("Kafka consumer ready.")

    def run(self) -> None:
        """Consume messages and archive in batches using a batch counter."""
        logger.info("Listening on %s", self.topic)
        total_archived = 0
        batch: list[dict] = []
        batch_size = 50
        batch_count = 0

        try:
            while True:
                records = self.consumer.poll(timeout_ms=300000)

                if not records:
                    if batch:
                        batch_count += 1
                        total_archived += self.write_s3_batch(batch, batch_count)
                        batch = []
                    break  # Exit if no more records after long poll

                for tp, messages in records.items():
                    for message in messages:
                        batch.append(
                            {
                                "offset": message.offset,
                                "timestamp": message.timestamp,
                                "key": (
                                    message.key.decode("utf-8") if message.key else None
                                ),
                                "value": message.value,
                            }
                        )

                        if len(batch) >= batch_size:
                            batch_count += 1
                            total_archived += self.write_s3_batch(batch, batch_count)
                            logger.info(
                                "Batch complete, %d total messages archived",
                                total_archived,
                            )
                            batch = []

            # Final cleanup
            if batch:
                batch_count += 1
                total_archived += self.write_s3_batch(batch, batch_count)
                logger.info("Final batch loaded, total %d messages", total_archived)

        except KeyboardInterrupt:
            logger.info("Shutting down")
        finally:
            logger.info("Archived %d messages from %s", total_archived, self.topic)
            self.close()

    def write_s3_batch(self, batch: list[dict], batch_count: int) -> int:
        """Upload a batch of messages to S3 using batch count for unique key."""
        if not batch:
            return 0

        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        date_path = datetime.utcnow().strftime("%Y/%m/%d")
        key = f"raw/{self.topic}/{date_path}/{timestamp}_batch{batch_count}.json"

        try:
            self.s3.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=json.dumps(batch, indent=2),
                ContentType="application/json",
            )
            logger.info("Successfully archived batch of %d to %s", len(batch), key)
            return len(batch)
        except Exception as e:
            logger.error("S3 upload failed: %s", e)
            raise

    def close(self):
        """Close Kafka consumer."""
        self.consumer.close()
        logger.info("S3 Archiver connection closed.")
