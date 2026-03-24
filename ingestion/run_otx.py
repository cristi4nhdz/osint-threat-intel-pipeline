# ingestion/run_otx.py
"""Runs the AlienVault OTX ingestion pipeline."""

import logging
from ingestion.otx_producer import OTXProducer

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
    """Fetch and publish AlienVault OTX pulses to Kafka."""
    logger.info("Starting OTX ingestion pipeline")
    producer = None

    try:
        try:
            producer = OTXProducer()
        except Exception as e:
            logger.error("Failed to initialize OTXProducer: %s", e)
            raise
        count = producer.fetch_and_publish(max_pulses=1000)
        logger.info("Published %d pulses", count)
    except Exception as e:
        logger.error("Pipeline failed : %s", e)
        raise
    finally:
        if producer:
            producer.close()


if __name__ == "__main__":
    main()
