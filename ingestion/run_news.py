# ingestion/run_news.py
"""Runs the news ingestion pipeline."""

import time
import logging
from config.config_loader import load_config
from ingestion.news_producer import NewsProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    handlers=[logging.FileHandler("ingestion\logs.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


def main() -> None:
    """Fetch and publish articles to Kafka in intervals."""
    logger.info("Starting news ingestion pipeline")
    config = load_config()
    poll_interval = config["news"]["poll_interval_seconds"]
    producer = None

    try:
        try:
            producer = NewsProducer()
        except Exception as e:
            logger.error("Failed to initialize NewsProducer: %s", e)
            raise
        while True:
            count = producer.fetch_and_publish()
            logger.info(
                "Published %d articles, sleeping %ds",
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
