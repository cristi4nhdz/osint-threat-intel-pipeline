# ingestion/run_rss.py
"""Runs the RSS feed ingestion pipeline."""

import time
import logging
from config.config_loader import load_config
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
    """Fetch and publish RSS feed articles to Kafka in intervals."""
    logger.info("Starting RSS feed ingestion pipeline")
    config = load_config()
    poll_interval = config["news"]["poll_interval_seconds"]
    producer = None

    try:
        try:
            producer = RSSProducer()
        except Exception as e:
            logger.error("Failed to initialize RSSProducer: %s", e)
            raise
        while True:
            count = producer.fetch_and_publish(max_per_feed=20)
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
