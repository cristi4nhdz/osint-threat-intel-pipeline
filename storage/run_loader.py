# storage/run_loader.py
"""Runs the Snowflake storage pipeline."""

import logging
import os
from storage.snowflake_loader import SnowflakeLoader

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
    """Load processed messages from Kafka into Snowflake."""
    logger.info("Starting Snowflake loader")
    loader = None

    try:
        loader = SnowflakeLoader()
        loader.run()
    except KeyboardInterrupt:
        logger.info("Shutting down")
    except Exception as e:
        logger.info("Pipeline failed: %s", e)
        raise
    finally:
        if loader:
            loader.close()


if __name__ == "__main__":
    main()
