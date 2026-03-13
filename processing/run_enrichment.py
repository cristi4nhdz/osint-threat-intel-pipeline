# processing/run_enrichment.py
"""Runs the enrichment processing pipeline"""

import logging
from processing.enrichment_consumer import EnrichmentConsumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    handlers=[
        logging.FileHandler(r"processing\logs\processing_logs.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


def main() -> None:
    """Runs the enrichment consumer pipeline"""
    logger.info("Starting enrichment pipeline")
    consumer = None

    try:
        try:
            consumer = EnrichmentConsumer(min_relevance_score=0.2)
        except Exception as e:
            logger.error("Failed to initialize EnrichmentConsumer: %s", e)
            raise
        consumer.run()
    except Exception as e:
        logger.error("Pipeline failed: %s", e)
        raise
    finally:
        if consumer:
            consumer.close()


if __name__ == "__main__":
    main()
