#!/usr/bin/env python3
"""
warlock_ingester - Qdrant Utilities

This module provides utilities for working with Qdrant vector database.
"""

from typing import List, Dict, Any, Optional
import logging

from src.errors import VectorStoreError

logger = logging.getLogger(__name__)


def _require_qdrant_client():
    try:
        from qdrant_client import QdrantClient
        from qdrant_client.models import VectorParams, Distance, Filter, FieldCondition, MatchValue
    except Exception as exc:
        raise VectorStoreError(
            "Qdrant operations require qdrant-client. Install with: pip install qdrant-client"
        ) from exc
    return QdrantClient, VectorParams, Distance, Filter, FieldCondition, MatchValue


def initialize_qdrant_client(qdrant_url: str = "http://127.0.0.1:6333"):
    """Initialize and return a Qdrant client."""
    QdrantClient, _, _, _, _, _ = _require_qdrant_client()
    try:
        client = QdrantClient(url=qdrant_url)
        client.get_collections()
        logger.info("Successfully connected to Qdrant at %s", qdrant_url)
        return client
    except Exception as e:
        logger.error("Failed to connect to Qdrant at %s: %s", qdrant_url, e)
        raise VectorStoreError(f"Failed to connect to Qdrant at {qdrant_url}") from e


def create_collection(
    qdrant_client,
    collection_name: str,
    vector_dim: int = 768,
    distance: Any = None,
) -> None:
    """Create a collection if it does not exist."""
    _, VectorParams, Distance, _, _, _ = _require_qdrant_client()

    if collection_exists(qdrant_client, collection_name):
        return

    if distance is None:
        distance = Distance.COSINE

    try:
        qdrant_client.create_collection(
            collection_name=collection_name,
            vectors_config=VectorParams(size=vector_dim, distance=distance),
        )
        logger.info("Created collection %s with %s dimensions", collection_name, vector_dim)
    except Exception as e:
        logger.error("Failed to create collection %s: %s", collection_name, e)
        raise VectorStoreError(f"Failed to create collection {collection_name}") from e


def collection_exists(qdrant_client, collection_name: str) -> bool:
    """Check if a collection exists in Qdrant."""
    try:
        collections = qdrant_client.get_collections()
        collection_names = [col.name for col in collections.collections]
        return collection_name in collection_names
    except Exception as e:
        logger.error("Error checking collection existence for %s: %s", collection_name, e)
        raise VectorStoreError(f"Error checking collection existence for {collection_name}") from e


def create_alias(qdrant_client, collection_name: str, alias_name: str) -> None:
    """Create an alias for a collection."""
    try:
        qdrant_client.create_alias(collection_name=collection_name, alias_name=alias_name)
        logger.info("Created alias %s for collection %s", alias_name, collection_name)
    except Exception as e:
        logger.error("Failed to create alias %s for collection %s: %s", alias_name, collection_name, e)
        raise VectorStoreError(f"Failed to create alias {alias_name}") from e


def switch_alias(qdrant_client, old_alias: str, new_collection: str) -> None:
    """Switch an alias to point to a new collection."""
    try:
        aliases = qdrant_client.get_alias(alias_name=old_alias)
        old_collection = aliases.collection_name
        qdrant_client.delete_alias(alias_name=old_alias)
        qdrant_client.create_alias(collection_name=new_collection, alias_name=old_alias)
        logger.info("Switched alias %s from %s to %s", old_alias, old_collection, new_collection)
    except Exception as e:
        logger.error("Failed to switch alias %s: %s", old_alias, e)
        raise VectorStoreError(f"Failed to switch alias {old_alias}") from e


def search_in_collection(
    qdrant_client,
    collection_name: str,
    query_vector: List[float],
    top_k: int = 10,
    kb_name: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Search for similar vectors in a collection."""
    _, _, _, Filter, FieldCondition, MatchValue = _require_qdrant_client()
    try:
        query_filter = None
        if kb_name:
            query_filter = Filter(
                must=[FieldCondition(key="kb", match=MatchValue(value=kb_name))]
            )

        response = qdrant_client.query_points(
            collection_name=collection_name,
            query=query_vector,
            limit=top_k,
            with_payload=True,
            query_filter=query_filter,
        )
        points = response.points if hasattr(response, "points") else response
        normalized = []
        for p in points:
            normalized.append(
                {
                    "id": str(getattr(p, "id", "")),
                    "score": float(getattr(p, "score", 0.0) or 0.0),
                    "payload": getattr(p, "payload", {}) or {},
                }
            )
        return normalized
    except Exception as e:
        logger.error("Search failed in collection %s: %s", collection_name, e)
        raise VectorStoreError(f"Search failed in collection {collection_name}") from e


def get_collection_info(qdrant_client, collection_name: str) -> Dict[str, Any]:
    """Get information about a collection."""
    try:
        collection_info = qdrant_client.get_collection(collection_name)
        return collection_info.dict()
    except Exception as e:
        logger.error("Failed to get collection info for %s: %s", collection_name, e)
        raise VectorStoreError(f"Failed to get collection info for {collection_name}") from e


def list_collections(qdrant_client) -> List[str]:
    """List all collections in Qdrant."""
    try:
        collections = qdrant_client.get_collections()
        return [col.name for col in collections.collections]
    except Exception as e:
        logger.error("Failed to list collections: %s", e)
        raise VectorStoreError("Failed to list collections") from e


if __name__ == "__main__":
    print("Testing Qdrant utilities...")
    try:
        client = initialize_qdrant_client()
        print("Qdrant initialized successfully")
    except Exception as e:
        print(f"Failed to initialize Qdrant: {e}")
