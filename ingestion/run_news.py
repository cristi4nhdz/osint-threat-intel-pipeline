# ingestion/run_news.py
"""Runs the news ingestion pipeline."""

import logging
from ingestion.news_producer import NewsProducer

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
    """Fetch and publish articles to Kafka."""
    logger.info("Starting news ingestion pipeline")
    producer = None

    try:
        try:
            producer = NewsProducer()
        except Exception as e:
            logger.error("Failed to initialize NewsProducer: %s", e)
            raise
        count = producer.fetch_and_publish()
        logger.info("Published %d articles", count)
    except Exception as e:
        logger.error("Pipeline failed: %s", e)
        raise
    finally:
        if producer:
            producer.close()


if __name__ == "__main__":
    main()
