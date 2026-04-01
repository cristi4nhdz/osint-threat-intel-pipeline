# api/rag_api.py
"""RAG API for semantic search over threat intelligence."""

import logging
from datetime import datetime
import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from storage.vector_store import VectorStore
from config.config_loader import load_config
import uvicorn

os.makedirs("/app/logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    handlers=[
        logging.FileHandler("/app/logs/rag_api.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Threat Intelligence RAG API")

config = load_config()
WEAVIATE_URL = config["weaviate"]["url"]

vector_store = VectorStore(weaviate_url=WEAVIATE_URL)
logger.info("Vector store ready with %d chunks", vector_store.count())

query_count = 0
startup_time = datetime.now()


class Query(BaseModel):
    """Semantic search query model."""

    question: str
    n_results: int = 5


class SearchResult(BaseModel):
    """Semantic search result model."""

    text: str
    source: str
    url: str
    actors: list[str]
    malware: list[str]
    similarity: float


class QueryResponse(BaseModel):
    """Semantic search response model."""

    query: str
    results: list[SearchResult]
    total_results: int


@app.get("/")
def root():
    """Return API metadata and health info."""
    return {
        "message": "Threat Intelligence RAG API",
        "total_chunks": vector_store.count(),
        "queries_served": query_count,
        "uptime_hours": round(
            (datetime.now() - startup_time).total_seconds() / 3600, 2
        ),
        "endpoints": {"search": "/search", "stats": "/stats"},
    }


@app.get("/stats")
def get_stats():
    """Return vector store and API usage statistics."""
    stats = vector_store.get_stats()
    status = "ready" if stats["total_chunks"] > 0 else "waiting"

    return {
        "total_chunks": stats["total_chunks"],
        "collection_name": stats["collection_name"],
        "queries_served": query_count,
        "uptime_hours": round(
            (datetime.now() - startup_time).total_seconds() / 3600, 2
        ),
        "last_updated": startup_time.isoformat(),
        "status": status,
    }


@app.post("/reconnect")
def reconnect():
    """Reconnect the vector store."""
    global vector_store
    vector_store.close()
    vector_store = VectorStore(weaviate_url=WEAVIATE_URL)
    logger.info("Reconnected to Weaviate — %d chunks", vector_store.count())
    return {"status": "ok", "total_chunks": vector_store.count()}


@app.post("/search", response_model=QueryResponse)
def search(query: Query):
    """Execute semantic search query."""
    global query_count

    if not query.question.strip():
        raise HTTPException(status_code=400, detail="Question cannot be empty")

    try:
        query_count += 1
        logger.info("Query #%d: %s", query_count, query.question)

        if vector_store.count() == 0:
            raise HTTPException(
                status_code=503,
                detail="Vector store is empty — waiting for embeddings to load",
            )

        results = vector_store.search(query.question, n_results=query.n_results)
        search_results = []

        for doc, meta, dist in zip(
            results["documents"], results["metadatas"], results["distances"]
        ):
            actors = meta.get("actors", "").split(",") if meta.get("actors") else []
            malware = meta.get("malware", "").split(",") if meta.get("malware") else []
            search_results.append(
                SearchResult(
                    text=doc,
                    source=meta.get("source", "Unknown"),
                    url=meta.get("article_url", ""),
                    actors=[a for a in actors if a],
                    malware=[m for m in malware if m],
                    similarity=round(1 - dist, 3),
                )
            )

        return QueryResponse(
            query=query.question,
            results=search_results,
            total_results=len(search_results),
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Search failed: %s", e)
        raise HTTPException(status_code=500, detail=f"Search failed: {e}")


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
