# ingestion/run_rss.py
"""Runs the RSS feed ingestion pipeline."""

import logging
from ingestion.rss_producer import RSSProducer

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
    """Fetch and publish RSS feed articles to Kafka."""
    logger.info("Starting RSS feed ingestion pipeline")

    producer = None

    try:
        try:
            producer = RSSProducer()
        except Exception as e:
            logger.error("Failed to initialize RSSProducer: %s", e)
            raise
        count = producer.fetch_and_publish(max_per_feed=20)
        logger.info("Published %d articles", count)
    except Exception as e:
        logger.error("Pipeline failed: %s", e)
        raise
    finally:
        if producer:
            producer.close()


if __name__ == "__main__":
    main()
