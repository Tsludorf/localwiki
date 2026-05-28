#!/usr/bin/env python3
"""Typed error hierarchy for ingestion boundaries."""


class IngestionError(Exception):
    """Base class for ingestion-related failures."""


class ConfigError(IngestionError):
    """Configuration resolution or validation failure."""


class RegistryError(IngestionError):
    """Local registry persistence/query failure."""


class ParserProcessingError(IngestionError):
    """Parsing or parse-to-document transformation failure."""


class EmbeddingError(IngestionError):
    """Embedding provider request/response failure."""


class VectorStoreError(IngestionError):
    """Vector store request/compatibility failure."""
