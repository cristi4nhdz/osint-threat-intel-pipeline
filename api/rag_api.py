# api/rag_api.py
"""RAG API for semantic search over threat intelligence."""

import logging
from datetime import datetime
import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from storage.vector_store import VectorStore
from services.llm_client import LLMClient
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
RETRIEVE_K = config["retrieval"]["retrieve_k"]
ANALYSIS_K = config["retrieval"]["analysis_k"]
DISPLAY_K = config["retrieval"]["display_k"]
MIN_SIMILARITY = config["retrieval"]["min_similarity"]
MAX_PER_SOURCE = config["retrieval"]["max_per_source"]

vector_store = VectorStore(weaviate_url=WEAVIATE_URL)
llm_client = LLMClient()

logger.info("Vector store ready with %d chunks", vector_store.count())

query_count = 0
startup_time = datetime.now()


class Query(BaseModel):
    """Semantic search query model."""

    question: str
    n_results: int = DISPLAY_K


class SearchResult(BaseModel):
    """Semantic search result model."""

    text: str
    source: str
    url: str
    actors: list[str]
    malware: list[str]
    similarity: float
    match_label: str


class AnalysisResponse(BaseModel):
    """Grounded threat analysis."""

    title: str
    status: str
    summary: str
    recent_activity: list[str]
    why_it_matters: str
    key_entities: dict[str, list[str]]
    grounding_mode: str


class QueryResponse(BaseModel):
    """Semantic search and grounded analysis response model."""

    query: str
    analysis: AnalysisResponse | None
    results: list[SearchResult]
    total_results: int
    retrieved_count: int
    displayed_count: int


def relevance_label(score: float) -> str:
    """Convert numeric similarity into analyst-friendly labels."""
    if score >= 0.50:
        return "strong match"
    if score >= 0.45:
        return "relevant"
    return "mentioned"


def filter_results_by_query(results: list[dict], query: str) -> list[dict]:
    """Prefer results that explicitly match the query in text or actor metadata."""
    query_lower = query.strip().lower()
    if not query_lower:
        return results

    filtered = []
    for result in results:
        text_match = query_lower in result.get("text", "").lower()
        actor_match = any(
            query_lower in actor.lower() for actor in result.get("actors", [])
        )
        if text_match or actor_match:
            filtered.append(result)

    return filtered if filtered else results


def select_results(
    results: list[dict],
) -> tuple[list[dict], list[dict]]:
    """Select top-k for generation and top results for display."""
    ranked = sorted(results, key=lambda r: r["similarity"], reverse=True)

    for item in ranked:
        item["match_label"] = relevance_label(item["similarity"])

    filtered = [r for r in ranked if r["similarity"] >= MIN_SIMILARITY]
    display = filtered[:DISPLAY_K] if filtered else ranked[:DISPLAY_K]

    analysis: list[dict] = []
    per_source_counts: dict[str, int] = {}

    source_pool = filtered if filtered else ranked

    for item in source_pool:
        source_key = item["url"] or item["source"] or "unknown"
        if per_source_counts.get(source_key, 0) >= MAX_PER_SOURCE:
            continue
        analysis.append(item)
        per_source_counts[source_key] = per_source_counts.get(source_key, 0) + 1
        if len(analysis) >= ANALYSIS_K:
            break

    return analysis, display


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
        "endpoints": {
            "search": "/search",
            "stats": "/stats",
            "reconnect": "/reconnect",
        },
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

        raw_results = vector_store.search(query.question, n_results=RETRIEVE_K)

        normalized: list[dict] = []
        for doc, meta, dist in zip(
            raw_results["documents"],
            raw_results["metadatas"],
            raw_results["distances"],
        ):
            actors = meta.get("actors", "").split(",") if meta.get("actors") else []
            malware = meta.get("malware", "").split(",") if meta.get("malware") else []

            normalized.append(
                {
                    "text": doc,
                    "source": meta.get("source", "Unknown"),
                    "url": meta.get("article_url", ""),
                    "actors": [a for a in actors if a],
                    "malware": [m for m in malware if m],
                    "similarity": round(1 - dist, 3),
                }
            )

        normalized = filter_results_by_query(normalized, query.question)

        analysis_context, display_results = select_results(normalized)

        strong_or_relevant = [
            r
            for r in analysis_context
            if r.get("match_label") in ["strong match", "relevant"]
        ]

        try:
            if not strong_or_relevant:
                analysis_payload = AnalysisResponse(
                    title="Threat Analysis",
                    status="low_confidence",
                    summary="The retrieved intelligence contains only weak references to this query. Findings should be interpreted with caution.",
                    recent_activity=[],
                    why_it_matters="Low-confidence matches increase the risk of incorrect attribution.",
                    key_entities={
                        "actors": [],
                        "malware": [],
                        "locations": [],
                        "sources": [],
                    },
                    grounding_mode="retrieved_context_only",
                )
            else:
                generated = llm_client.generate_analysis(
                    query.question, strong_or_relevant
                )
                analysis_payload = AnalysisResponse(**generated)

        except Exception as llm_error:
            logger.warning("Generation unavailable: %s", llm_error)
            analysis_payload = AnalysisResponse(
                title="Threat Analysis",
                status="unavailable",
                summary="Generation failed. Showing supporting intelligence only.",
                recent_activity=[],
                why_it_matters="",
                key_entities={
                    "actors": [],
                    "malware": [],
                    "locations": [],
                    "sources": [],
                },
                grounding_mode="retrieved_context_only",
            )

        search_results = [
            SearchResult(
                text=item["text"],
                source=item["source"],
                url=item["url"],
                actors=item["actors"],
                malware=item["malware"],
                similarity=item["similarity"],
                match_label=item["match_label"],
            )
            for item in display_results[: query.n_results]
        ]

        return QueryResponse(
            query=query.question,
            analysis=analysis_payload,
            results=search_results,
            total_results=len(search_results),
            retrieved_count=len(normalized),
            displayed_count=len(search_results),
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Search failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
