# ingestion/run_mitre.py
"""Runs the MITRE ATT&CK ingestion pipeline."""

import logging
import os
from ingestion.mitre_producer import MitreProducer

os.makedirs("/app/logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    handlers=[
        logging.FileHandler("/app/logs/ingestion.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


def main() -> None:
    """Fetch and publish MITRE ATT&CK threat group"""
    logger.info("Starting MITRE ATT&CK ingestion")
    producer = None

    try:
        producer = MitreProducer()
    except Exception as e:
        logger.error("Failed to initialize MitreProducer: %s", e)
        raise
    try:
        count = producer.fetch_and_publish()
        logger.info("MITRE ingestion complete. %d groups published", count)
    except Exception as e:
        logger.error("Pipeline failed : %s", e)
    finally:
        if producer:
            producer.close()


if __name__ == "__main__":
    main()
