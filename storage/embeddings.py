# storage/embeddings.py
"""Embedding pipeline for Snowflake threat_articles."""

import json
import logging
import snowflake.connector
from config.config_loader import load_config
from storage.vector_store import VectorStore

logger = logging.getLogger(__name__)


def parse_json_field(value) -> list:
    """Parse a JSON array field from Snowflake, handling various formats."""
    if not value:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            return [v.strip() for v in value.split(",") if v.strip()]
    return []


def chunk_article(article: dict) -> list[dict]:
    """Generate a single chunk from the article title with metadata."""
    title = article.get("title", "").strip()
    if not title:
        return []

    actors = article.get("threat_actors", []) or []
    malware = article.get("malware", []) or []
    locations = article.get("locations", []) or []
    source = article.get("source", "").strip()

    parts = [f"Title: {title}"]
    if source:
        parts.append(f"Source: {source}")
    if actors:
        parts.append(f"Threat Actors: {', '.join(actors)}")
    if malware:
        parts.append(f"Malware: {', '.join(malware)}")
    if locations:
        parts.append(f"Locations: {', '.join(locations)}")

    enriched_text = "\n".join(parts)

    return [
        {
            "text": enriched_text,
            "article_id": article.get("id", "unknown"),
            "article_url": article.get("original_url", ""),
            "source": source,
            "actors": actors,
            "malware": malware,
            "locations": locations,
            "chunk_index": 0,
            "total_chunks": 1,
        }
    ]


def fetch_articles_from_snowflake() -> list[dict]:
    """Fetch enriched articles from Snowflake threat_articles table."""
    config = load_config()
    sf = config["snowflake"]

    logger.info("Connecting to Snowflake")
    conn = snowflake.connector.connect(
        account=sf["account"],
        user=sf["user"],
        password=sf["password"],
        database=sf["database"],
        schema=sf["schema"],
        warehouse=sf["warehouse"],
    )

    cursor = conn.cursor()
    cursor.execute("USE DATABASE THREAT_INTEL")
    cursor.execute("USE SCHEMA PUBLIC")
    cursor.execute("""
        SELECT id, title, source, original_url, threat_actors, malware, locations
        FROM threat_articles
    """)

    articles = []
    for row in cursor:
        articles.append(
            {
                "id": row[0],
                "title": row[1] or "",
                "source": row[2] or "",
                "original_url": row[3] or "",
                "threat_actors": parse_json_field(row[4]),
                "malware": parse_json_field(row[5]),
                "locations": parse_json_field(row[6]),
            }
        )

    cursor.close()
    conn.close()
    logger.info("Fetched %d articles from Snowflake", len(articles))
    return articles


class EmbeddingLoader:
    """Loads article embeddings from Snowflake into Weaviate."""

    def __init__(self, weaviate_url: str = None) -> None:
        self.vector_store = VectorStore(weaviate_url=weaviate_url)

    def run(self) -> None:
        """Fetch articles and load embeddings in batches."""
        logger.info("Starting embedding loader")

        articles = fetch_articles_from_snowflake()
        if not articles:
            logger.warning("No articles found in Snowflake")
            return

        initial_count = self.vector_store.count()
        total_chunks = 0
        batch_size = 50

        for i in range(0, len(articles), batch_size):
            batch = articles[i : i + batch_size]
            batch_added = 0
            for article in batch:
                chunks = chunk_article(article)
                if chunks:
                    batch_added += self.vector_store.add_chunks(chunks)
            total_chunks += batch_added
            logger.info(
                "Batch %d: +%d chunks, Total: %d",
                i // batch_size + 1,
                batch_added,
                total_chunks,
            )

        logger.info(
            "Complete: %d articles, %d chunks, store %d -> %d",
            len(articles),
            total_chunks,
            initial_count,
            self.vector_store.count(),
        )

    def close(self) -> None:
        """Close the vector store."""
        self.vector_store.close()
