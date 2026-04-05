# storage/run_embedding_loader.py
"""Runs the embedding pipeline."""

import logging
import os
import requests
from config.config_loader import load_config
from storage.embeddings import EmbeddingLoader

os.makedirs("/app/logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    handlers=[
        logging.FileHandler("/app/logs/embedding.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

config = load_config()
WEAVIATE_URL = config["weaviate"]["url"]
RAG_API_URL = config["rag"]["api_url"]


def main() -> None:
    "Load embeddings into the vector store."
    logger.info("Starting embedding loader")
    loader = None

    try:
        loader = EmbeddingLoader(weaviate_url=WEAVIATE_URL)
        loader.run()
        try:
            r = requests.post(f"{RAG_API_URL}/reconnect", timeout=5)
            logger.info("RAG API reconnected: %s", r.json())
        except Exception as e:
            logger.warning("Could not reconnect RAG API: %s", e)
    except KeyboardInterrupt:
        logger.info("Shutting down")
    except Exception as e:
        logger.error("Pipeline failed: %s", e)
        raise
    finally:
        if loader:
            loader.close()


if __name__ == "__main__":
    main()
