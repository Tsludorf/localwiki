#!/usr/bin/env python3
"""Registry service helpers for batching hot-path operations."""

from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence, Set

from src.errors import RegistryError


@dataclass
class ChunkCitation:
    chunk_id: str
    chunk_text: str
    metadata: Dict[str, object]


class RegistryService:
    """High-level registry helper that reduces SQLite churn."""

    def __init__(self, registry):
        self.registry = registry

    def get_chunk_citations(self, chunk_ids: Sequence[str]) -> Dict[str, ChunkCitation]:
        try:
            return self.registry.get_chunk_citations(chunk_ids)
        except Exception as exc:
            raise RegistryError("Failed to load chunk citations") from exc

    def existing_vector_chunk_ids(self, chunk_ids: Sequence[str], collection_name: str) -> Set[str]:
        try:
            return self.registry.existing_vector_chunk_ids(chunk_ids, collection_name)
        except Exception as exc:
            raise RegistryError("Failed to query existing vector points") from exc

    def existing_vector_chunk_ids_by_status(
        self,
        chunk_ids: Sequence[str],
        collection_name: str,
        statuses: Optional[List[str]] = None,
    ) -> Set[str]:
        try:
            return self.registry.existing_vector_chunk_ids(chunk_ids, collection_name, statuses=statuses)
        except Exception as exc:
            raise RegistryError("Failed to query existing vector points") from exc

    def get_vector_points_for_chunks(self, chunk_ids: Sequence[str], collection_name: str) -> Dict[str, List[object]]:
        try:
            return self.registry.get_vector_points_for_chunks(chunk_ids, collection_name)
        except Exception as exc:
            raise RegistryError("Failed to load vector point records") from exc

    def update_vector_point_statuses(self, point_ids: Sequence[str], status: str) -> None:
        try:
            self.registry.update_vector_point_statuses(list(point_ids), status)
        except Exception as exc:
            raise RegistryError("Failed to update vector point statuses") from exc

    def add_vector_points(self, vector_points: Iterable[object]) -> None:
        try:
            self.registry.add_vector_points(list(vector_points))
        except Exception as exc:
            raise RegistryError("Failed to persist vector points") from exc
