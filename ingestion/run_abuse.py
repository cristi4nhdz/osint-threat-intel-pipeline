# ingestion/run_abuse.py
"""Runs the Abuse.ch IOC ingestion pipeline."""

import time
import logging
from config.config_loader import load_config
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
    """Fetch and publish Abuse.ch IOCs to Kafka in intervals."""
    logger.info("Starting Abuse.ch IOC ingestion pipeline")
    config = load_config()
    poll_interval = config["news"]["poll_interval_seconds"]
    producer = None

    try:
        try:
            producer = AbuseProducer()
        except Exception as e:
            logger.error("Failed to initialize AbuseProducer: %s", e)
            raise
        while True:
            count = producer.fetch_and_publish()
            logger.info(
                "Published %d IOCs, sleeping %ds",
                count,
                poll_interval,
            )
            time.sleep(poll_interval)
    except KeyboardInterrupt:
        logger.info("Shutting down")
    except Exception as e:
        logger.error("Pipeline failed: %s", e)
        raise
    finally:
        if producer:
            producer.close()


if __name__ == "__main__":
    main()
