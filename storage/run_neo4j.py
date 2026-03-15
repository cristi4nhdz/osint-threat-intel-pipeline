# storage/run_neo4j.py
"""Runs the Neo4j graph-build pipeline."""

import logging
from storage.neo4j_loader import Neo4jLoader

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
    """Load processed data from Kafka into the Neo4j knowledge graph."""
    logger.info("Starting Neo4j graph builder")
    loader = None

    try:
        loader = Neo4jLoader()
    except Exception as e:
        logger.error("Failed to initialize Neo4jLoader: %s", e)
        raise
    try:
        loader.build_graph()
        logger.info("Graph build complete.")
    except Exception as e:
        logger.error("Pipeline failed : %s", e)
    finally:
        if loader:
            loader.close()


if __name__ == "__main__":
    main()
