#!/usr/bin/env python3
"""
warlock_ingester - Qdrant Utilities

This module provides utilities for working with Qdrant vector database.
"""

from typing import List, Dict, Any, Optional
from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance, Filter, FieldCondition, MatchValue
import logging

logger = logging.getLogger(__name__)

def initialize_qdrant_client(qdrant_url: str = "http://127.0.0.1:6333") -> QdrantClient:
    """
    Initialize and return a Qdrant client.
    
    Args:
        qdrant_url (str): URL of the Qdrant instance
        
    Returns:
        QdrantClient: Initialized client
    """
    try:
        client = QdrantClient(url=qdrant_url)
        # Test the connection
        client.get_collections()
        logger.info(f"Successfully connected to Qdrant at {qdrant_url}")
        return client
    except Exception as e:
        logger.error(f"Failed to connect to Qdrant at {qdrant_url}: {e}")
        raise

def create_collection(qdrant_client: QdrantClient, collection_name: str,
                     vector_dim: int = 768, distance: Distance = Distance.COSINE) -> bool:
    """
    Create a new collection in Qdrant.
    
    Args:
        qdrant_client (QdrantClient): Qdrant client instance
        collection_name (str): Name of the collection to create
        vector_dim (int): Dimension of vectors (default: 768)
        distance (str): Distance metric (default: "Cosine")
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        if collection_exists(qdrant_client, collection_name):
            return True
        qdrant_client.create_collection(
            collection_name=collection_name,
            vectors_config=VectorParams(size=vector_dim, distance=distance)
        )
        logger.info(f"Created collection {collection_name} with {vector_dim} dimensions")
        return True
    except Exception as e:
        logger.error(f"Failed to create collection {collection_name}: {e}")
        return False

def collection_exists(qdrant_client: QdrantClient, collection_name: str) -> bool:
    """
    Check if a collection exists in Qdrant.
    
    Args:
        qdrant_client (QdrantClient): Qdrant client instance
        collection_name (str): Name of the collection to check
        
    Returns:
        bool: True if collection exists, False otherwise
    """
    try:
        collections = qdrant_client.get_collections()
        collection_names = [col.name for col in collections.collections]
        return collection_name in collection_names
    except Exception as e:
        logger.error(f"Error checking collection existence: {e}")
        return False

def create_alias(qdrant_client: QdrantClient, collection_name: str, alias_name: str) -> bool:
    """
    Create an alias for a collection.
    
    Args:
        qdrant_client (QdrantClient): Qdrant client instance
        collection_name (str): Name of the collection
        alias_name (str): Alias name to create
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        qdrant_client.create_alias(collection_name=collection_name, alias_name=alias_name)
        logger.info(f"Created alias {alias_name} for collection {collection_name}")
        return True
    except Exception as e:
        logger.error(f"Failed to create alias {alias_name} for collection {collection_name}: {e}")
        return False

def switch_alias(qdrant_client: QdrantClient, old_alias: str, new_collection: str) -> bool:
    """
    Switch an alias to point to a new collection.
    
    Args:
        qdrant_client (QdrantClient): Qdrant client instance
        old_alias (str): Alias name to switch
        new_collection (str): New collection to point to
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Get current alias info
        aliases = qdrant_client.get_alias(alias_name=old_alias)
        old_collection = aliases.collection_name
        
        # Remove old alias
        qdrant_client.delete_alias(alias_name=old_alias)
        
        # Create new alias
        qdrant_client.create_alias(collection_name=new_collection, alias_name=old_alias)
        
        logger.info(f"Switched alias {old_alias} from {old_collection} to {new_collection}")
        return True
    except Exception as e:
        logger.error(f"Failed to switch alias {old_alias}: {e}")
        return False

def search_in_collection(qdrant_client: QdrantClient, collection_name: str,
                        query_vector: List[float], top_k: int = 10,
                        kb_name: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Search for similar vectors in a collection.
    
    Args:
        qdrant_client (QdrantClient): Qdrant client instance
        collection_name (str): Name of the collection to search in
        query_vector (List[float]): Query vector
        top_k (int): Number of results to return
        
    Returns:
        List[Dict[str, Any]]: Search results
    """
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
            normalized.append({
                "id": str(getattr(p, "id", "")),
                "score": float(getattr(p, "score", 0.0) or 0.0),
                "payload": getattr(p, "payload", {}) or {}
            })
        return normalized
    except Exception as e:
        logger.error(f"Search failed in collection {collection_name}: {e}")
        return []

def get_collection_info(qdrant_client: QdrantClient, collection_name: str) -> Dict[str, Any]:
    """
    Get information about a collection.
    
    Args:
        qdrant_client (QdrantClient): Qdrant client instance
        collection_name (str): Name of the collection
        
    Returns:
        Dict[str, Any]: Collection information
    """
    try:
        collection_info = qdrant_client.get_collection(collection_name)
        return collection_info.dict()
    except Exception as e:
        logger.error(f"Failed to get collection info for {collection_name}: {e}")
        return {}

def list_collections(qdrant_client: QdrantClient) -> List[str]:
    """
    List all collections in Qdrant.
    
    Args:
        qdrant_client (QdrantClient): Qdrant client instance
        
    Returns:
        List[str]: List of collection names
    """
    try:
        collections = qdrant_client.get_collections()
        return [col.name for col in collections.collections]
    except Exception as e:
        logger.error(f"Failed to list collections: {e}")
        return []

if __name__ == "__main__":
    # Test the Qdrant utilities
    print("Testing Qdrant utilities...")
    
    try:
        client = initialize_qdrant_client()
        print("Qdrant initialized successfully")
    except Exception as e:
        print(f"Failed to initialize Qdrant: {e}")
