# storage/run_ioc_loader.py
"""Runs the IOC loader pipeline — consumes from Kafka and writes to Snowflake + Neo4j."""

import logging
import os
from storage.ioc_loader import IOCLoader

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
    """Run the IOC loader."""
    logger.info("Starting IOC loader pipeline")
    loader = None

    try:
        loader = IOCLoader()
        loader.load()
    except KeyboardInterrupt:
        logger.info("Shutting down")
    except Exception as e:
        logger.error("IOC loader failed: %s", e)
        raise
    finally:
        if loader:
            loader.close()


if __name__ == "__main__":
    main()
