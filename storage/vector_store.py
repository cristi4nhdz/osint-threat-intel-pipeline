# storage/vector_store.py
"""Weaviate vector store for RAG retrieval."""

import logging
import os
import weaviate
from weaviate.classes.config import Property, DataType, Configure
from weaviate.classes.query import MetadataQuery
from sentence_transformers import SentenceTransformer

logger = logging.getLogger(__name__)

COLLECTION_NAME = "OsintArticle"


class VectorStore:
    """Manages vector embeddings in Weaviate."""

    def __init__(self, weaviate_url: str) -> None:
        """Initialize Weaviate client and embedding model."""
        logger.info("Initializing Weaviate vector store")

        weaviate_url = weaviate_url or os.getenv(
            "WEAVIATE_URL", "http://localhost:8080"
        )

        # Parse host and port from URL
        url_clean = weaviate_url.replace("http://", "").replace("https://", "")
        if ":" in url_clean:
            host, port = url_clean.split(":")
            port = int(port)
        else:
            host = url_clean
            port = 8080

        self.client = weaviate.connect_to_local(host=host, port=port)
        self._ensure_collection()

        logger.info("Loading embedding model")
        self.model = SentenceTransformer("all-mpnet-base-v2")

        logger.info("Vector store ready. Current chunks: %d", self.count())

    def _ensure_collection(self) -> None:
        """Create collection if it doesn't exist."""
        if not self.client.collections.exists(COLLECTION_NAME):
            self.client.collections.create(
                name=COLLECTION_NAME,
                properties=[
                    Property(name="text", data_type=DataType.TEXT),
                    Property(name="article_id", data_type=DataType.TEXT),
                    Property(name="article_url", data_type=DataType.TEXT),
                    Property(name="source", data_type=DataType.TEXT),
                    Property(name="chunk_index", data_type=DataType.INT),
                    Property(name="total_chunks", data_type=DataType.INT),
                    Property(name="actors", data_type=DataType.TEXT),
                    Property(name="malware", data_type=DataType.TEXT),
                    Property(name="locations", data_type=DataType.TEXT),
                ],
                vectorizer_config=Configure.Vectorizer.none(),
            )
            logger.info("Created collection: %s", COLLECTION_NAME)

        self.collection = self.client.collections.get(COLLECTION_NAME)

    def count(self) -> int:
        """Return the number of chunks in the collection."""
        result = self.collection.aggregate.over_all(total_count=True)
        return result.total_count or 0

    def add_chunks(self, chunks: list[dict]) -> int:
        """Embed and store article chunks with metadata."""
        if not chunks:
            return 0

        # Build chunk IDs
        chunk_ids = {f"{c['article_id']}-chunk-{c['chunk_index']}" for c in chunks}

        # Check existing by fetching all and filtering
        existing_ids = set()
        try:
            for obj in self.collection.iterator():
                obj_id = f"{obj.properties['article_id']}-chunk-{obj.properties['chunk_index']}"
                if obj_id in chunk_ids:
                    existing_ids.add(obj_id)
        except Exception as e:
            logger.warning("Could not check existing chunks: %s", e)

        new_chunks = [
            c
            for c in chunks
            if f"{c['article_id']}-chunk-{c['chunk_index']}" not in existing_ids
        ]
        if not new_chunks:
            return 0

        texts = [chunk["text"] for chunk in new_chunks]
        embeddings = self.model.encode(texts, show_progress_bar=False)

        with self.collection.batch.dynamic() as batch:
            for chunk, embedding in zip(new_chunks, embeddings):
                properties = {
                    "text": chunk["text"],
                    "article_id": str(chunk["article_id"]),
                    "article_url": chunk.get("article_url", ""),
                    "source": chunk.get("source", ""),
                    "chunk_index": chunk.get("chunk_index", 0),
                    "total_chunks": chunk.get("total_chunks", 1),
                    "actors": (
                        ",".join(chunk.get("actors", [])) if chunk.get("actors") else ""
                    ),
                    "malware": (
                        ",".join(chunk.get("malware", []))
                        if chunk.get("malware")
                        else ""
                    ),
                    "locations": (
                        ",".join(chunk.get("locations", []))
                        if chunk.get("locations")
                        else ""
                    ),
                }
                batch.add_object(properties=properties, vector=embedding.tolist())

        logger.info("Added %d new chunks to vector store", len(new_chunks))
        return len(new_chunks)

    def search(self, query: str, n_results: int = 5) -> dict:
        """Retrieve the most relevant chunks for a query."""
        if self.count() == 0:
            raise ValueError("Vector store is empty — run embeddings first")

        query_embedding = self.model.encode([query])[0]

        results = self.collection.query.near_vector(
            near_vector=query_embedding.tolist(),
            limit=n_results,
            return_metadata=MetadataQuery(distance=True),
        )

        documents = []
        metadatas = []
        distances = []

        for obj in results.objects:
            documents.append(obj.properties.get("text", ""))
            metadatas.append(
                {
                    "article_id": obj.properties.get("article_id", ""),
                    "article_url": obj.properties.get("article_url", ""),
                    "source": obj.properties.get("source", ""),
                    "chunk_index": obj.properties.get("chunk_index", 0),
                    "total_chunks": obj.properties.get("total_chunks", 1),
                    "actors": obj.properties.get("actors", ""),
                    "malware": obj.properties.get("malware", ""),
                    "locations": obj.properties.get("locations", ""),
                }
            )
            distances.append(obj.metadata.distance if obj.metadata.distance else 0.0)

        return {
            "documents": documents,
            "metadatas": metadatas,
            "distances": distances,
        }

    def get_stats(self) -> dict:
        """Return statistics about the vector store."""
        return {
            "total_chunks": self.count(),
            "collection_name": COLLECTION_NAME,
        }

    def clear(self) -> None:
        """Delete all data from the vector store."""
        self.client.collections.delete(COLLECTION_NAME)
        self._ensure_collection()
        logger.info("Collection cleared")

    def close(self) -> None:
        """Close the Weaviate client."""
        self.client.close()
        logger.info("Vector store closed")
