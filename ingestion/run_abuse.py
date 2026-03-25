# ingestion/run_abuse.py
"""Runs the Abuse.ch IOC ingestion pipeline."""

import time
import logging
from ingestion.abuse_producer import AbuseProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    handlers=[
        logging.FileHandler(r"ingestion\logs\ingestion_logs.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


def main() -> None:
    """Fetch and publish Abuse.ch IOCs to Kafka."""
    logger.info("Starting Abuse.ch IOC ingestion pipeline")
    producer = None

    try:
        try:
            producer = AbuseProducer()
        except Exception as e:
            logger.error("Failed to initialize AbuseProducer: %s", e)
            raise
        count = producer.fetch_and_publish()
        logger.info("Published %d IOCs", count)
    except Exception as e:
        logger.error("Pipeline failed: %s", e)
        raise
    finally:
        if producer:
            producer.close()


if __name__ == "__main__":
    main()
