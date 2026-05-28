#!/usr/bin/env python3
"""Vector store service boundary for Qdrant operations."""

from dataclasses import dataclass
from typing import Any, Optional, Sequence

from src.errors import VectorStoreError
from src.qdrant_utils import create_collection


@dataclass
class VectorWritePlan:
    collection_name: str
    model_name: str
    vector_dim: int


class VectorStoreService:
    """Encapsulates collection compatibility checks and upserts."""

    @staticmethod
    def extract_collection_vector_dim(collection_info: Any) -> Optional[int]:
        try:
            vectors = collection_info.config.params.vectors
        except Exception:
            return None

        if hasattr(vectors, "size"):
            return int(vectors.size)
        if isinstance(vectors, dict):
            for value in vectors.values():
                if hasattr(value, "size"):
                    return int(value.size)
                if isinstance(value, dict) and "size" in value:
                    return int(value["size"])
        return None

    @staticmethod
    def collection_name_for_dimension(vector_dim: int) -> str:
        return f"local_wiki_{int(vector_dim)}"

    def ensure_collection_compatible(self, qdrant_client, collection_name: str, model_name: str, vector_dim: int, logger) -> None:
        try:
            collections = qdrant_client.get_collections()
            existing_names = {col.name for col in collections.collections}
        except Exception as exc:
            raise VectorStoreError("Failed to list Qdrant collections") from exc

        if collection_name not in existing_names:
            create_collection(qdrant_client, collection_name=collection_name, vector_dim=vector_dim)
            logger.info("Created collection %s with vector dim %s", collection_name, vector_dim)
            return

        try:
            info = qdrant_client.get_collection(collection_name)
        except Exception as exc:
            raise VectorStoreError(f"Failed to load collection info for {collection_name}") from exc

        existing_dim = self.extract_collection_vector_dim(info)
        if existing_dim is not None and existing_dim != vector_dim:
            recommended = self.collection_name_for_dimension(vector_dim)
            raise VectorStoreError(
                f"Collection {collection_name} has dim {existing_dim} but embedder {model_name} returned dim {vector_dim}. "
                f"Use a separate collection (e.g. {recommended}) or recreate the collection."
            )

    def upsert_points(self, qdrant_client, collection_name: str, points: Sequence[Any], batch_size: int) -> int:
        write_batch_size = max(1, batch_size)
        points_written = 0
        for i in range(0, len(points), write_batch_size):
            batch = points[i:i + write_batch_size]
            try:
                qdrant_client.upsert(collection_name=collection_name, points=batch)
            except Exception as exc:
                raise VectorStoreError(
                    f"Qdrant upsert failed for collection {collection_name} (batch_size={len(batch)})"
                ) from exc
            points_written += len(batch)
        return points_written

    def fetch_existing_point_ids(self, qdrant_client, collection_name: str, point_ids: Sequence[str], batch_size: int = 256) -> set:
        """Return subset of point IDs that exist in Qdrant."""
        if not point_ids:
            return set()

        existing_ids = set()
        read_batch_size = max(1, batch_size)
        for i in range(0, len(point_ids), read_batch_size):
            batch_ids = list(point_ids[i:i + read_batch_size])
            try:
                records = qdrant_client.retrieve(
                    collection_name=collection_name,
                    ids=batch_ids,
                    with_payload=False,
                    with_vectors=False,
                )
            except Exception as exc:
                raise VectorStoreError(
                    f"Failed to reconcile remote point IDs for collection {collection_name}"
                ) from exc

            for rec in records:
                existing_ids.add(str(getattr(rec, "id", "")))
        return existing_ids
