# storage/run_loader.py
"""Runs the Snowflake storage pipeline."""

import logging
from storage.snowflake_loader import SnowflakeLoader

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    handlers=[
        logging.FileHandler(r"storage\logs\storage_logs.log"),
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
