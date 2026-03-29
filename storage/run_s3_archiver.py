# storage/run_s3_archive.py
"""Runs the S3 archival pipeline."""

import logging
import os
from storage.s3_archiver import S3Archiver

os.makedirs("/app/logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    handlers=[
        logging.FileHandler("/app/logs/storage.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


def main() -> None:
    """Archive raw Kafka messages to S3 for disaster recovery."""
    logger.info("Starting S3 archival")
    archiver = None
    archivers = []

    topics = ["osint.news", "osint.mitre", "osint.enriched", "osint.iocs"]

    try:
        for topic in topics:
            archiver = S3Archiver(topic)
            archivers.append(archiver)
            archiver.run()
    except KeyboardInterrupt:
        logger.info("Shutting down")
    except Exception as e:
        logger.info("Pipeline failed: %s", e)
        raise
    finally:
        for archiver in archivers:
            archiver.close()


if __name__ == "__main__":
    main()
