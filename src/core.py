#!/usr/bin/env python3
"""
warlock_ingester - A local data ingestion system for building knowledge bases using Qdrant vector storage.
"""

import os
import sys
import sqlite3
import hashlib
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
import logging
import uuid
import json
import re
from collections import Counter
from src.parsers import ParserFactory
from src.embedding_service import EmbeddingService, build_embedding_text, resolve_embed_model
from src.registry import RegistryService
from src.vector_store_service import VectorStoreService


EXTENSION_MIME_MAP = {
    ".txt": "text/plain",
    ".md": "text/markdown",
    ".json": "application/json",
    ".jsonl": "application/x-ndjson",
    ".ndjson": "application/x-ndjson",
    ".xml": "application/xml",
    ".html": "text/html",
    ".htm": "text/html",
    ".csv": "text/csv",
    ".pdf": "application/pdf",
    ".bz2": "application/x-bzip2",
    ".zim": "application/x-zim",
}


def detect_source_type_and_mime(file_path: Path) -> Tuple[str, str]:
    """Detect source_type and mime_type for a file path."""
    suffix = file_path.suffix.lower()
    mime_type = EXTENSION_MIME_MAP.get(suffix, "application/octet-stream")

    if suffix == ".zim":
        return "zim", mime_type
    if suffix == ".pdf":
        return "pdf", mime_type
    if suffix in {".jsonl", ".ndjson", ".xml", ".bz2"}:
        return "wikidump", mime_type
    if mime_type.startswith("text/") or suffix in {".json"}:
        return "text", mime_type
    return "file", mime_type

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/tmp/warlock_ingester.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

DEFAULT_EMBEDDER = "bge-m3:latest"

EMBEDDING_MODEL_PROFILES: Dict[str, Dict[str, Any]] = {
    "all-minilm:latest": {
        "dimensions": 384,
        "recommended_use": "Fast ingestion/testing",
        "chunk_tokens": 128,
        "chunk_overlap": 32,
        "embed_batch_size": 64,
        "max_chunk_chars": 512,
        "qdrant_batch_size": 256,
        "collection": "local_wiki_384",
    },
    "embeddinggemma:latest": {
        "dimensions": 768,
        "recommended_use": "Balanced general-purpose RAG",
        "chunk_tokens": 600,
        "chunk_overlap": 100,
        "embed_batch_size": 64,
        "max_chunk_chars": 4000,
        "qdrant_batch_size": 256,
        "collection": "local_wiki_768",
    },
    "bge-m3:latest": {
        "dimensions": 1024,
        "recommended_use": "High-quality semantic wiki / long-context retrieval",
        "chunk_tokens": 800,
        "chunk_overlap": 150,
        "embed_batch_size": 16,
        "max_chunk_chars": 6000,
        "qdrant_batch_size": 128,
        "collection": "local_wiki_1024",
    },
}

DEFAULT_EMBED_BATCH_SIZE = EMBEDDING_MODEL_PROFILES["bge-m3:latest"]["embed_batch_size"]
DEFAULT_QDRANT_BATCH_SIZE = EMBEDDING_MODEL_PROFILES["bge-m3:latest"]["qdrant_batch_size"]
DEFAULT_CHUNK_TOKENS = EMBEDDING_MODEL_PROFILES["bge-m3:latest"]["chunk_tokens"]
DEFAULT_CHUNK_OVERLAP = EMBEDDING_MODEL_PROFILES["bge-m3:latest"]["chunk_overlap"]
DEFAULT_MAX_CHUNK_CHARS = EMBEDDING_MODEL_PROFILES["bge-m3:latest"]["max_chunk_chars"]
EMBED_CONTEXT_FALLBACK_TOKENS = [600, 384, 300, 192, 128]

_EMBEDDING_SERVICE = EmbeddingService(
    default_embedder=DEFAULT_EMBEDDER,
    default_chunk_tokens=DEFAULT_CHUNK_TOKENS,
    default_max_chunk_chars=DEFAULT_MAX_CHUNK_CHARS,
)
_VECTOR_STORE_SERVICE = VectorStoreService()

def _coalesce_positive_int(override: Optional[int], default_value: int) -> int:
    if isinstance(override, int) and override > 0:
        return override
    return int(default_value)


def _canonical_profile_key_for_model(model_name: str) -> Optional[str]:
    model_low = (model_name or "").lower()
    if "minilm" in model_low:
        return "all-minilm:latest"
    if "embeddinggemma" in model_low:
        return "embeddinggemma:latest"
    if "bge-m3" in model_low or "bgem3" in model_low:
        return "bge-m3:latest"
    return None


def get_embedding_model_profile(embedder: str) -> Dict[str, Any]:
    model_name = _resolve_embed_model(embedder or os.getenv("OLLAMA_EMBED_MODEL", DEFAULT_EMBEDDER))
    canonical_key = _canonical_profile_key_for_model(model_name)
    if canonical_key and canonical_key in EMBEDDING_MODEL_PROFILES:
        profile = dict(EMBEDDING_MODEL_PROFILES[canonical_key])
    else:
        profile = {
            "dimensions": None,
            "recommended_use": "Custom embedder",
            "chunk_tokens": DEFAULT_CHUNK_TOKENS,
            "chunk_overlap": DEFAULT_CHUNK_OVERLAP,
            "embed_batch_size": DEFAULT_EMBED_BATCH_SIZE,
            "max_chunk_chars": DEFAULT_MAX_CHUNK_CHARS,
            "qdrant_batch_size": DEFAULT_QDRANT_BATCH_SIZE,
            "collection": "",
        }
    profile["model_name"] = model_name
    return profile


def resolve_model_ingestion_config(
    embedder: str,
    *,
    embed_batch_size: Optional[int] = None,
    qdrant_batch_size: Optional[int] = None,
    chunk_tokens: Optional[int] = None,
    chunk_overlap: Optional[int] = None,
    max_chunk_chars: Optional[int] = None,
) -> Dict[str, Any]:
    profile = get_embedding_model_profile(embedder)
    return {
        "model_name": profile["model_name"],
        "dimensions": profile["dimensions"],
        "recommended_use": profile["recommended_use"],
        "collection": profile["collection"],
        "embed_batch_size": _coalesce_positive_int(embed_batch_size, profile["embed_batch_size"]),
        "qdrant_batch_size": _coalesce_positive_int(qdrant_batch_size, profile["qdrant_batch_size"]),
        "chunk_tokens": _coalesce_positive_int(chunk_tokens, profile["chunk_tokens"]),
        "chunk_overlap": _coalesce_positive_int(chunk_overlap, profile["chunk_overlap"]),
        "max_chunk_chars": _coalesce_positive_int(max_chunk_chars, profile["max_chunk_chars"]),
    }


def _estimate_token_count(text: str) -> int:
    return len(re.findall(r"\S+", text or ""))


def _token_caps_for_retry(initial_cap: int) -> List[int]:
    ordered: List[int] = [max(1, initial_cap)]
    for cap in EMBED_CONTEXT_FALLBACK_TOKENS:
        if cap < ordered[0] and cap not in ordered:
            ordered.append(cap)
    return [cap for cap in ordered if cap > 0]


def _is_context_length_error(response_text: str) -> bool:
    low = (response_text or "").lower()
    return "context length" in low or "input length exceeds" in low

@dataclass
class Source:
    """Represents a data source."""
    source_id: str
    source_type: str
    root_uri: str
    display_name: str
    added_at: str
    settings_json: str = "{}"

@dataclass
class SourceItem:
    """Represents a single item in a source (file, page, etc)."""
    source_item_id: str
    source_id: str
    uri: str
    display_uri: str
    mime_type: str
    size_bytes: int
    mtime: str
    content_hash: str
    status: str = "pending"

@dataclass
class CanonicalDocument:
    """Represents a canonical document after parsing."""
    doc_id: str
    source_item_id: str
    title: str
    parser: str
    parser_version: str
    version_hash: str
    text_hash: str
    metadata_json: str
    created_at: str
    updated_at: str

@dataclass
class Chunk:
    """Represents a chunk of text from a document."""
    chunk_id: str
    doc_id: str
    chunk_index: int
    section: str
    char_start: int
    char_end: int
    token_estimate: int
    text_hash: str
    citation_json: str

@dataclass
class VectorPoint:
    """Represents a vector point in Qdrant."""
    point_id: str
    chunk_id: str
    collection_name: str
    alias_name: str
    embedding_model: str
    embedding_dim: int
    vector_hash: str
    status: str

@dataclass
class IngestionRun:
    """Represents an ingestion run."""
    ingest_run_id: str
    source_id: str
    started_at: str
    finished_at: str = ""
    status: str = "running"
    settings_json: str = "{}"
    stats_json: str = "{}"

@dataclass
class ParserError:
    """Represents a parser error."""
    error_id: str
    ingest_run_id: str
    source_item_id: str
    parser: str
    error_type: str
    message: str
    traceback: str
    created_at: str

class IngestionRegistry:
    """Manages the SQLite registry for tracking ingestion state."""
    
    def __init__(self, db_path: str = "localwiki_registry.db"):
        self.db_path = db_path
        self.init_database()
        
    def init_database(self):
        """Initialize the SQLite database with required tables."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Sources table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sources (
                source_id TEXT PRIMARY KEY,
                source_type TEXT NOT NULL,
                root_uri TEXT NOT NULL,
                display_name TEXT NOT NULL,
                added_at TEXT NOT NULL,
                settings_json TEXT DEFAULT '{}'
            )
        ''')
        
        # Source items table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS source_items (
                source_item_id TEXT PRIMARY KEY,
                source_id TEXT NOT NULL,
                uri TEXT NOT NULL,
                display_uri TEXT NOT NULL,
                mime_type TEXT,
                size_bytes INTEGER,
                mtime TEXT,
                content_hash TEXT,
                status TEXT DEFAULT 'pending',
                FOREIGN KEY (source_id) REFERENCES sources (source_id)
            )
        ''')
        
        # Canonical documents table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS canonical_documents (
                doc_id TEXT PRIMARY KEY,
                source_item_id TEXT NOT NULL,
                title TEXT NOT NULL,
                parser TEXT NOT NULL,
                parser_version TEXT NOT NULL,
                version_hash TEXT NOT NULL,
                text_hash TEXT NOT NULL,
                metadata_json TEXT DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY (source_item_id) REFERENCES source_items (source_item_id)
            )
        ''')
        
        # Chunks table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS chunks (
                chunk_id TEXT PRIMARY KEY,
                doc_id TEXT NOT NULL,
                chunk_index INTEGER NOT NULL,
                section TEXT,
                char_start INTEGER,
                char_end INTEGER,
                token_estimate INTEGER,
                text_hash TEXT NOT NULL,
                citation_json TEXT DEFAULT '{}',
                FOREIGN KEY (doc_id) REFERENCES canonical_documents (doc_id)
            )
        ''')
        
        # Vector points table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS vector_points (
                point_id TEXT PRIMARY KEY,
                chunk_id TEXT NOT NULL,
                collection_name TEXT NOT NULL,
                alias_name TEXT,
                embedding_model TEXT NOT NULL,
                embedding_dim INTEGER NOT NULL,
                vector_hash TEXT NOT NULL,
                status TEXT NOT NULL,
                FOREIGN KEY (chunk_id) REFERENCES chunks (chunk_id)
            )
        ''')
        cursor.execute(
            "UPDATE vector_points SET status = 'confirmed' WHERE status = 'completed'"
        )
        
        # Ingestion runs table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS ingestion_runs (
                ingest_run_id TEXT PRIMARY KEY,
                source_id TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                status TEXT DEFAULT 'running',
                settings_json TEXT DEFAULT '{}',
                stats_json TEXT DEFAULT '{}',
                FOREIGN KEY (source_id) REFERENCES sources (source_id)
            )
        ''')
        
        # Parser errors table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS parser_errors (
                error_id TEXT PRIMARY KEY,
                ingest_run_id TEXT NOT NULL,
                source_item_id TEXT NOT NULL,
                parser TEXT NOT NULL,
                error_type TEXT NOT NULL,
                message TEXT NOT NULL,
                traceback TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (ingest_run_id) REFERENCES ingestion_runs (ingest_run_id),
                FOREIGN KEY (source_item_id) REFERENCES source_items (source_item_id)
            )
        ''')
        
        conn.commit()
        conn.close()
        
    def add_source(self, source: Source) -> None:
        """Add a source to the registry."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO sources 
            (source_id, source_type, root_uri, display_name, added_at, settings_json)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (source.source_id, source.source_type, source.root_uri, source.display_name, source.added_at, source.settings_json))
        conn.commit()
        conn.close()
        logger.info(f"Added source: {source.source_id}")
        
    def get_source(self, source_id: str) -> Optional[Source]:
        """Get a source from the registry."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM sources WHERE source_id = ?', (source_id,))
        row = cursor.fetchone()
        conn.close()
        
        if row:
            return Source(*row)
        return None

    def list_sources(self) -> List[Source]:
        """List all sources in the registry."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM sources')
        rows = cursor.fetchall()
        conn.close()
        return [Source(*row) for row in rows]

    def delete_all_sources(self) -> None:
        """Delete all source-related records from the registry."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('DELETE FROM vector_points')
        cursor.execute('DELETE FROM chunks')
        cursor.execute('DELETE FROM canonical_documents')
        cursor.execute('DELETE FROM source_items')
        cursor.execute('DELETE FROM parser_errors')
        cursor.execute('DELETE FROM ingestion_runs')
        cursor.execute('DELETE FROM sources')
        conn.commit()
        conn.close()
        logger.info("Deleted all sources and related registry records")
        
    def add_source_item(self, source_item: SourceItem) -> None:
        """Add a source item to the registry."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO source_items 
            (source_item_id, source_id, uri, display_uri, mime_type, size_bytes, mtime, content_hash, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (source_item.source_item_id, source_item.source_id, source_item.uri, 
              source_item.display_uri, source_item.mime_type, source_item.size_bytes,
              source_item.mtime, source_item.content_hash, source_item.status))
        conn.commit()
        conn.close()
        logger.info(f"Added source item: {source_item.source_item_id}")
        
    def add_canonical_document(self, doc: CanonicalDocument) -> None:
        """Add a canonical document to the registry."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO canonical_documents 
            (doc_id, source_item_id, title, parser, parser_version, version_hash, 
             text_hash, metadata_json, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (doc.doc_id, doc.source_item_id, doc.title, doc.parser, doc.parser_version,
              doc.version_hash, doc.text_hash, doc.metadata_json, doc.created_at, doc.updated_at))
        conn.commit()
        conn.close()
        logger.info(f"Added canonical document: {doc.doc_id}")
        
    def add_chunk(self, chunk: Chunk) -> None:
        """Add a chunk to the registry."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO chunks 
            (chunk_id, doc_id, chunk_index, section, char_start, char_end, token_estimate, text_hash, citation_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (chunk.chunk_id, chunk.doc_id, chunk.chunk_index, chunk.section, chunk.char_start,
              chunk.char_end, chunk.token_estimate, chunk.text_hash, chunk.citation_json))
        conn.commit()
        conn.close()
        logger.info(f"Added chunk: {chunk.chunk_id}")
        
    def add_vector_point(self, point: VectorPoint) -> None:
        """Add a vector point to the registry."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO vector_points 
            (point_id, chunk_id, collection_name, alias_name, embedding_model, embedding_dim, vector_hash, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (point.point_id, point.chunk_id, point.collection_name, point.alias_name,
              point.embedding_model, point.embedding_dim, point.vector_hash, point.status))
        conn.commit()
        conn.close()
        logger.info(f"Added vector point: {point.point_id}")

    def add_vector_points(self, points: List[VectorPoint]) -> None:
        """Batch add vector points in a single transaction."""
        if not points:
            return
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.executemany(
            '''
            INSERT OR REPLACE INTO vector_points
            (point_id, chunk_id, collection_name, alias_name, embedding_model, embedding_dim, vector_hash, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''',
            [
                (
                    point.point_id,
                    point.chunk_id,
                    point.collection_name,
                    point.alias_name,
                    point.embedding_model,
                    point.embedding_dim,
                    point.vector_hash,
                    point.status,
                )
                for point in points
            ],
        )
        conn.commit()
        conn.close()
        logger.info("Added %s vector points in batch", len(points))

    def get_chunk_citations(self, chunk_ids: List[str]) -> Dict[str, Dict[str, Any]]:
        """Batch load citation payloads for chunks."""
        if not chunk_ids:
            return {}
        unique_ids = list(dict.fromkeys(chunk_ids))
        placeholders = ",".join(["?"] * len(unique_ids))
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT chunk_id, citation_json FROM chunks WHERE chunk_id IN ({placeholders})",
            unique_ids,
        )
        rows = cursor.fetchall()
        conn.close()

        citations: Dict[str, Dict[str, Any]] = {}
        for chunk_id, citation_json in rows:
            chunk_text = ""
            chunk_meta: Dict[str, Any] = {}
            if citation_json:
                try:
                    payload = json.loads(citation_json) if citation_json.startswith("{") else {}
                    chunk_text = payload.get("chunk_text", "")
                    chunk_meta = payload.get("metadata", {})
                except Exception:
                    chunk_text = ""
                    chunk_meta = {}
            citations[chunk_id] = {"chunk_text": chunk_text, "metadata": chunk_meta}
        return citations

    def existing_vector_chunk_ids(
        self,
        chunk_ids: List[str],
        collection_name: str,
        statuses: Optional[List[str]] = None,
    ) -> set:
        """Return set of chunk IDs that already have points in collection."""
        if not chunk_ids:
            return set()
        unique_ids = list(dict.fromkeys(chunk_ids))
        placeholders = ",".join(["?"] * len(unique_ids))
        status_sql = ""
        params: List[Any] = [collection_name] + unique_ids
        if statuses:
            status_placeholders = ",".join(["?"] * len(statuses))
            status_sql = f" AND status IN ({status_placeholders})"
            params.extend(statuses)
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT chunk_id FROM vector_points WHERE collection_name = ? AND chunk_id IN ({placeholders}){status_sql}",
            params,
        )
        rows = cursor.fetchall()
        conn.close()
        return {row[0] for row in rows}

    def get_vector_points_for_chunks(self, chunk_ids: List[str], collection_name: str) -> Dict[str, List[VectorPoint]]:
        """Return vector point records keyed by chunk ID for a collection."""
        if not chunk_ids:
            return {}
        unique_ids = list(dict.fromkeys(chunk_ids))
        placeholders = ",".join(["?"] * len(unique_ids))
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT point_id, chunk_id, collection_name, alias_name, embedding_model, embedding_dim, vector_hash, status "
            f"FROM vector_points WHERE collection_name = ? AND chunk_id IN ({placeholders})",
            [collection_name] + unique_ids,
        )
        rows = cursor.fetchall()
        conn.close()

        by_chunk: Dict[str, List[VectorPoint]] = {}
        for row in rows:
            point = VectorPoint(*row)
            by_chunk.setdefault(point.chunk_id, []).append(point)
        return by_chunk

    def update_vector_point_statuses(self, point_ids: List[str], status: str) -> None:
        """Batch update vector point status."""
        if not point_ids:
            return
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.executemany(
            "UPDATE vector_points SET status = ? WHERE point_id = ?",
            [(status, point_id) for point_id in point_ids],
        )
        conn.commit()
        conn.close()

    def vector_point_exists(self, chunk_id: str, collection_name: str) -> bool:
        """Check whether a chunk already has a vector in a collection."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            'SELECT 1 FROM vector_points WHERE chunk_id = ? AND collection_name = ? LIMIT 1',
            (chunk_id, collection_name),
        )
        exists = cursor.fetchone() is not None
        conn.close()
        return exists

    def add_parser_error(self, error: ParserError) -> None:
        """Persist a parser/processing error for diagnostics."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute(
            '''
            INSERT OR REPLACE INTO parser_errors
            (error_id, ingest_run_id, source_item_id, parser, error_type, message, traceback, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                error.error_id,
                error.ingest_run_id,
                error.source_item_id,
                error.parser,
                error.error_type,
                error.message,
                error.traceback,
                error.created_at,
            ),
        )
        conn.commit()
        conn.close()

    def get_source_item(self, source_item_id: str) -> Optional[SourceItem]:
        """Fetch a source item by ID."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM source_items WHERE source_item_id = ?', (source_item_id,))
        row = cursor.fetchone()
        conn.close()
        if row:
            return SourceItem(*row)
        return None
        
    def add_ingestion_run(self, run: IngestionRun) -> None:
        """Add an ingestion run to the registry."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO ingestion_runs 
            (ingest_run_id, source_id, started_at, finished_at, status, settings_json, stats_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (run.ingest_run_id, run.source_id, run.started_at, run.finished_at,
              run.status, run.settings_json, run.stats_json))
        conn.commit()
        conn.close()
        logger.info(f"Added ingestion run: {run.ingest_run_id}")
        
    def get_ingestion_run(self, run_id: str) -> Optional[IngestionRun]:
        """Get an ingestion run from the registry."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM ingestion_runs WHERE ingest_run_id = ?', (run_id,))
        row = cursor.fetchone()
        conn.close()
        
        if row:
            return IngestionRun(*row)
        return None
        
    def update_ingestion_run_status(self, run_id: str, status: str, finished_at: str = None) -> None:
        """Update the status of an ingestion run."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        if finished_at:
            cursor.execute('''
                UPDATE ingestion_runs SET status = ?, finished_at = ? WHERE ingest_run_id = ?
            ''', (status, finished_at, run_id))
        else:
            cursor.execute('''
                UPDATE ingestion_runs SET status = ? WHERE ingest_run_id = ?
            ''', (status, run_id))
        conn.commit()
        conn.close()
        logger.info(f"Updated ingestion run {run_id} status to {status}")
        
    def get_source_items(self, source_id: str, status: str = None) -> List[SourceItem]:
        """Get all source items for a source."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        if status:
            cursor.execute('SELECT * FROM source_items WHERE source_id = ? AND status = ?', (source_id, status))
        else:
            cursor.execute('SELECT * FROM source_items WHERE source_id = ?', (source_id,))
            
        rows = cursor.fetchall()
        conn.close()
        
        items = []
        for row in rows:
            items.append(SourceItem(*row))
        return items
        
    def update_source_item_status(self, item_id: str, status: str) -> None:
        """Update the status of a source item."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('UPDATE source_items SET status = ? WHERE source_item_id = ?', (status, item_id))
        conn.commit()
        conn.close()
        logger.info(f"Updated source item {item_id} status to {status}")
        
    def get_unprocessed_items(self, source_id: str) -> List[SourceItem]:
        """Get all unprocessed items for a source."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM source_items WHERE source_id = ? AND status != ?', (source_id, 'completed'))
        rows = cursor.fetchall()
        conn.close()
        
        items = []
        for row in rows:
            items.append(SourceItem(*row))
        return items
        
    def get_doc_by_content_hash(self, text_hash: str) -> Optional[CanonicalDocument]:
        """Get a document by its content hash."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM canonical_documents WHERE text_hash = ?', (text_hash,))
        row = cursor.fetchone()
        conn.close()
        
        if row:
            return CanonicalDocument(*row)
        return None
        
    def get_chunk_by_text_hash(self, text_hash: str) -> Optional[Chunk]:
        """Get a chunk by its text hash."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM chunks WHERE text_hash = ?', (text_hash,))
        row = cursor.fetchone()
        conn.close()
        
        if row:
            return Chunk(*row)
        return None

class Chunker:
    """Handles token-window chunking with overlap."""

    def __init__(self, max_tokens: int = DEFAULT_CHUNK_TOKENS, overlap_tokens: int = DEFAULT_CHUNK_OVERLAP, min_tokens: int = 300):
        if max_tokens <= 0:
            raise ValueError("max_tokens must be > 0")
        if overlap_tokens < 0:
            raise ValueError("overlap_tokens must be >= 0")
        if overlap_tokens >= max_tokens:
            raise ValueError("overlap_tokens must be < max_tokens")
        self.max_tokens = max_tokens
        self.overlap_tokens = overlap_tokens
        self.min_tokens = max(1, min_tokens)

    def chunk_text(self, text: str, section: str = "", metadata: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """
        Break text into token windows with overlap.
        
        Args:
            text (str): The text to chunk
            section (str): Section identifier
            metadata (Dict[str, Any]): Metadata associated with the text
            
        Returns:
            List[Dict[str, Any]]: List of chunks with their metadata
        """
        normalized = (text or "").strip()
        if not normalized:
            return []

        token_matches = list(re.finditer(r"\S+", text))
        if not token_matches:
            return []

        chunks = []
        chunk_index = 0
        total_tokens = len(token_matches)
        start_token = 0

        while start_token < total_tokens:
            end_token = min(start_token + self.max_tokens, total_tokens)
            remaining_tokens = total_tokens - end_token
            if remaining_tokens and remaining_tokens < self.min_tokens and end_token < total_tokens:
                end_token = total_tokens

            char_start = token_matches[start_token].start()
            char_end = token_matches[end_token - 1].end()
            chunk_text = text[char_start:char_end].strip()
            if chunk_text:
                chunks.append({
                    "chunk_text": chunk_text,
                    "chunk_index": chunk_index,
                    "section": section,
                    "char_start": char_start,
                    "char_end": char_end,
                    "token_estimate": end_token - start_token,
                    "metadata": metadata or {},
                })

            if end_token >= total_tokens:
                break

            next_start = max(0, end_token - self.overlap_tokens)
            if next_start <= start_token:
                next_start = end_token
            start_token = next_start
            chunk_index += 1

        return chunks

class BaseParser:
    """Base class for parsers."""
    
    def __init__(self, name: str, version: str = "1.0.0"):
        self.name = name
        self.version = version
        
    def parse(self, source_item: SourceItem) -> Tuple[str, Dict[str, Any]]:
        """
        Parse a source item and return the text content and metadata.
        
        Returns:
            Tuple[str, Dict[str, Any]]: (text_content, metadata_dict)
        """
        raise NotImplementedError("Subclasses should implement this method")

class DocumentManager:
    """Manages document creation and versioning."""
    
    def __init__(self, registry: IngestionRegistry):
        self.registry = registry
        
    def generate_hash(self, text: str) -> str:
        """Generate a hash for content."""
        return hashlib.sha256(text.encode('utf-8')).hexdigest()
        
    def create_document(self, source_item: SourceItem, text: str, parser_name: str,
                       metadata: Dict[str, Any] = None, title: str = None,
                       doc_key: str = None) -> CanonicalDocument:
        """Create a canonical document from a parsed source item."""
        effective_key = doc_key or source_item.uri
        doc_id = f"doc_{self.generate_hash(effective_key)}"
        text_hash = self.generate_hash(text)
        
        # Check if document with same hash already exists
        existing_doc = self.registry.get_doc_by_content_hash(text_hash)
        if existing_doc:
            logger.info(f"Document with same content already exists: {existing_doc.doc_id}")
            return existing_doc
            
        # Generate new document
        now = str(int(time.time()))
        document = CanonicalDocument(
            doc_id=doc_id,
            source_item_id=source_item.source_item_id,
            title=title or Path(source_item.uri).name,
            parser=parser_name,
            parser_version="1.0.0",
            version_hash="1.0.0",  # In a real implementation this would be version control
            text_hash=text_hash,
            metadata_json=json.dumps(metadata) if metadata else "{}",
            created_at=now,
            updated_at=now
        )
        
        self.registry.add_canonical_document(document)
        return document

def _is_low_value_chunk(chunk_text: str, title: str = "") -> bool:
    normalized = (chunk_text or "").strip()
    if not normalized:
        return True
    if len(normalized) < 200:
        return True

    low = normalized.lower()
    title_low = (title or "").strip().lower()

    skip_prefixes = (
        "category:",
        "portal:",
        "template:",
        "index of",
    )
    if title_low.startswith(skip_prefixes):
        return True

    nav_markers = (
        "jump to navigation",
        "jump to search",
        "navigation menu",
        "main page",
        "contents",
        "this page was last edited",
        "retrieved from",
        "stub this article",
    )
    if sum(1 for marker in nav_markers if marker in low) >= 2:
        return True

    lines = [line.strip() for line in normalized.splitlines() if line.strip()]
    if lines:
        tableish_lines = 0
        codeish_lines = 0
        repeated_token_candidates: List[str] = []
        for line in lines:
            if line.count("|") >= 2 or line.count("\t") >= 2 or line.count(",") >= 8:
                tableish_lines += 1
            if re.search(r"\b[A-Z]{2,}\d{2,}\b", line):
                codeish_lines += 1
            repeated_token_candidates.extend(re.findall(r"[A-Za-z0-9_\-]{3,}", line.lower()))

        line_count = len(lines)
        tableish_ratio = tableish_lines / line_count
        codeish_ratio = codeish_lines / line_count

        if repeated_token_candidates:
            counter = Counter(repeated_token_candidates)
            top_20 = counter.most_common(20)
            top_repeat_total = sum(count for _, count in top_20)
            repeat_ratio = top_repeat_total / max(1, len(repeated_token_candidates))
        else:
            repeat_ratio = 0.0

        if (tableish_ratio > 0.45 and repeat_ratio > 0.35) or (codeish_ratio > 0.35 and repeat_ratio > 0.40):
            return True

    return False


def _run_ingestion_pipeline_impl(source_id: str, registry: IngestionRegistry,
                                 qdrant_client, embedder: str, kb_name: str = "wiki_kb",
                                 limit: int = 10000,
                                 collection_name: Optional[str] = None,
                                 changed_only: bool = False,
                                 embed_batch_size: Optional[int] = None,
                                 qdrant_batch_size: Optional[int] = None,
                                 chunk_tokens: Optional[int] = None,
                                 chunk_overlap: Optional[int] = None,
                                 max_chunk_chars: Optional[int] = None) -> Dict[str, int]:
    from src.ingestion_service import IngestionService

    service = IngestionService(
        logger=logger,
        parser_factory=ParserFactory,
        source_item_cls=SourceItem,
        ingestion_run_cls=IngestionRun,
        parser_error_cls=ParserError,
        document_manager_cls=DocumentManager,
        chunker_cls=Chunker,
        chunk_cls=Chunk,
        json_dumps_fn=json.dumps,
        detect_source_type_and_mime=detect_source_type_and_mime,
        resolve_model_ingestion_config=resolve_model_ingestion_config,
        low_value_chunk_predicate=_is_low_value_chunk,
        embed_chunks_and_store=embed_chunks_and_store,
    )
    return service.run(
        source_id=source_id,
        registry=registry,
        qdrant_client=qdrant_client,
        embedder=embedder,
        kb_name=kb_name,
        limit=limit,
        collection_name=collection_name,
        changed_only=changed_only,
        embed_batch_size=embed_batch_size,
        qdrant_batch_size=qdrant_batch_size,
        chunk_tokens=chunk_tokens,
        chunk_overlap=chunk_overlap,
        max_chunk_chars=max_chunk_chars,
    )


def run_ingestion_pipeline(source_id: str, registry: IngestionRegistry,
                          qdrant_client, embedder: str, kb_name: str = "wiki_kb",
                          limit: int = 10000,
                          collection_name: Optional[str] = None,
                          changed_only: bool = False,
                          embed_batch_size: Optional[int] = None,
                          qdrant_batch_size: Optional[int] = None,
                          chunk_tokens: Optional[int] = None,
                          chunk_overlap: Optional[int] = None,
                          max_chunk_chars: Optional[int] = None) -> Dict[str, int]:
    return _run_ingestion_pipeline_impl(
        source_id=source_id,
        registry=registry,
        qdrant_client=qdrant_client,
        embedder=embedder,
        kb_name=kb_name,
        limit=limit,
        collection_name=collection_name,
        changed_only=changed_only,
        embed_batch_size=embed_batch_size,
        qdrant_batch_size=qdrant_batch_size,
        chunk_tokens=chunk_tokens,
        chunk_overlap=chunk_overlap,
        max_chunk_chars=max_chunk_chars,
    )

def _resolve_embed_model(embedder: Optional[str]) -> str:
    return resolve_embed_model(embedder, DEFAULT_EMBEDDER)


def _truncate_text_for_embedding(text: str, max_tokens: int, max_chars: int) -> Tuple[str, bool]:
    from src.embedding_service import truncate_text_for_embedding

    return truncate_text_for_embedding(text, max_tokens, max_chars)


def _collection_name_for(kb_name: str, model_name: str, collection_name: Optional[str] = None) -> str:
    if collection_name:
        return collection_name
    profile_key = _canonical_profile_key_for_model(model_name)
    if profile_key:
        return EMBEDDING_MODEL_PROFILES[profile_key]["collection"]
    return ""


def resolve_collection_name(embedder: str, collection_name: Optional[str] = None, kb_name: str = "") -> str:
    if collection_name:
        return collection_name
    profile = get_embedding_model_profile(embedder)
    return profile.get("collection", "")


def infer_embedding_dimension(embedder: str, ollama_url: Optional[str] = None) -> int:
    try:
        import httpx
    except Exception as exc:
        raise RuntimeError("Dimension inference requires httpx. Install with: pip install httpx") from exc

    model_name = _resolve_embed_model(embedder or os.getenv("OLLAMA_EMBED_MODEL", DEFAULT_EMBEDDER))
    url = (ollama_url or os.getenv("OLLAMA_URL", "http://127.0.0.1:11434")).rstrip("/") + "/api/embed"
    with httpx.Client(timeout=60.0) as client:
        response = client.post(url, json={"model": model_name, "input": ["dimension probe"]})
        response.raise_for_status()
        data = response.json()
        embeddings = data.get("embeddings") or []
        if not embeddings or not embeddings[0]:
            raise RuntimeError(f"Unable to infer embedding dimension for model {model_name}")
        return len(embeddings[0])


def collection_name_for_dimension(vector_dim: int) -> str:
    return f"local_wiki_{int(vector_dim)}"


def _extract_collection_vector_dim(collection_info: Any) -> Optional[int]:
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


def _recommended_collection_name(model_name: str, vector_dim: int) -> str:
    return collection_name_for_dimension(vector_dim)


def _ensure_collection_compatible(qdrant_client, collection_name: str, model_name: str, vector_dim: int) -> None:
    _VECTOR_STORE_SERVICE.ensure_collection_compatible(
        qdrant_client,
        collection_name=collection_name,
        model_name=model_name,
        vector_dim=vector_dim,
        logger=logger,
    )


def _prepare_embed_items(
    texts: List[str],
    max_tokens: int,
    max_chars: int,
) -> Tuple[List[str], List[int], int, int]:
    from src.embedding_service import prepare_embed_items

    return prepare_embed_items(texts, max_tokens=max_tokens, max_chars=max_chars)


def _embed_texts(
    texts: List[str],
    model_name: str,
    ollama_url: str,
    batch_size: int = DEFAULT_EMBED_BATCH_SIZE,
    max_tokens: Optional[int] = None,
    max_chars: Optional[int] = None,
) -> Tuple[List[List[float]], List[int]]:
    return _EMBEDDING_SERVICE.embed_texts(
        texts=texts,
        model_name=model_name,
        ollama_url=ollama_url,
        batch_size=batch_size,
        max_tokens=max_tokens,
        max_chars=max_chars,
        logger=logger,
    )


def _build_embedding_text(
    chunk_text: str,
    chunk_meta: Dict[str, Any],
    document: CanonicalDocument,
    source_item: SourceItem,
    source: Source,
) -> str:
    return build_embedding_text(chunk_text, chunk_meta, document, source_item, source)


def embed_chunks_and_store(qdrant_client, chunks: List[Chunk], embedder: str,
                          document: CanonicalDocument, registry: IngestionRegistry,
                          source_item: SourceItem, source: Source, kb_name: str,
                          collection_name: Optional[str] = None,
                          embed_batch_size: int = DEFAULT_EMBED_BATCH_SIZE,
                          qdrant_batch_size: int = DEFAULT_QDRANT_BATCH_SIZE,
                          embed_max_tokens: Optional[int] = None,
                          max_chunk_chars: Optional[int] = None) -> Dict[str, int]:
    """
    Embed chunks and store in Qdrant.
    """
    stats = {"chunks_embedded": 0, "points_stored": 0}
    if not chunks:
        return stats

    model_name = _resolve_embed_model(embedder or os.getenv("OLLAMA_EMBED_MODEL", DEFAULT_EMBEDDER))
    ollama_url = os.getenv("OLLAMA_URL", "http://127.0.0.1:11434")
    resolved_collection_name = _collection_name_for(kb_name, model_name, collection_name)
    precheck_collection_name = resolved_collection_name or collection_name
    registry_service = RegistryService(registry)

    chunk_ids = [chunk.chunk_id for chunk in chunks]
    citation_map = registry_service.get_chunk_citations(chunk_ids)
    existing_chunk_ids = set()
    if precheck_collection_name:
        existing_chunk_ids = registry_service.existing_vector_chunk_ids_by_status(
            chunk_ids,
            precheck_collection_name,
            statuses=["confirmed"],
        )
        vector_points_by_chunk = registry_service.get_vector_points_for_chunks(chunk_ids, precheck_collection_name)
        point_to_chunk: Dict[str, str] = {}
        unresolved_point_ids: List[str] = []
        for chunk_id, records in vector_points_by_chunk.items():
            if chunk_id in existing_chunk_ids:
                continue
            for record in records:
                if record.status in {"pending", "upserted_remote"}:
                    unresolved_point_ids.append(record.point_id)
                    point_to_chunk[record.point_id] = chunk_id

        if unresolved_point_ids:
            remote_existing_ids = _VECTOR_STORE_SERVICE.fetch_existing_point_ids(
                qdrant_client,
                collection_name=precheck_collection_name,
                point_ids=unresolved_point_ids,
            )
            confirmed_now = [point_id for point_id in unresolved_point_ids if point_id in remote_existing_ids]
            if confirmed_now:
                registry_service.update_vector_point_statuses(confirmed_now, "confirmed")
                existing_chunk_ids.update(point_to_chunk[point_id] for point_id in confirmed_now)

    prepared_items: List[Dict[str, Any]] = []
    precheck_skipped_existing = 0
    skipped_missing_text = 0
    for chunk in chunks:
        if precheck_collection_name and chunk.chunk_id in existing_chunk_ids:
            precheck_skipped_existing += 1
            continue

        citation = citation_map.get(chunk.chunk_id, {})
        chunk_text = citation.get("chunk_text", "") if citation else ""
        chunk_meta = citation.get("metadata", {}) if citation else {}
        if not chunk_text:
            skipped_missing_text += 1
            continue
        embed_text = _build_embedding_text(chunk_text, chunk_meta, document, source_item, source)
        prepared_items.append(
            {
                "chunk": chunk,
                "embed_text": embed_text,
                "chunk_text": chunk_text,
                "chunk_meta": chunk_meta,
            }
        )

    logger.info(
        "Embed prep summary doc=%s source_item=%s input_chunks=%s precheck_skipped=%s missing_text=%s prepared=%s precheck_collection=%s",
        document.doc_id,
        source_item.source_item_id,
        len(chunks),
        precheck_skipped_existing,
        skipped_missing_text,
        len(prepared_items),
        precheck_collection_name or "auto",
    )

    if not prepared_items:
        logger.info("No new chunks to embed for this document")
        return stats

    logger.info(f"Encoding {len(prepared_items)} chunks for document {document.doc_id}")
    embed_texts = [item["embed_text"] for item in prepared_items]
    embeddings, kept_indices = _embed_texts(
        embed_texts,
        model_name,
        ollama_url,
        batch_size=embed_batch_size,
        max_tokens=embed_max_tokens,
        max_chars=max_chunk_chars,
    )
    embedded_items = [prepared_items[idx] for idx in kept_indices]

    if not embedded_items:
        logger.info("No embeddable chunks left after preprocessing for document %s", document.doc_id)
        return stats

    vector_dim = len(embeddings[0])
    collection_name = resolved_collection_name or collection_name_for_dimension(vector_dim)
    _ensure_collection_compatible(qdrant_client, collection_name, model_name, vector_dim)

    embedded_chunk_ids = [item["chunk"].chunk_id for item in embedded_items]
    vector_points_by_chunk = registry_service.get_vector_points_for_chunks(embedded_chunk_ids, collection_name)
    unresolved_point_ids: List[str] = []
    for _, records in vector_points_by_chunk.items():
        for record in records:
            if record.status in {"pending", "upserted_remote"}:
                unresolved_point_ids.append(record.point_id)

    if unresolved_point_ids:
        remote_existing_ids = _VECTOR_STORE_SERVICE.fetch_existing_point_ids(
            qdrant_client,
            collection_name=collection_name,
            point_ids=unresolved_point_ids,
        )
        confirmed_now = [point_id for point_id in unresolved_point_ids if point_id in remote_existing_ids]
        if confirmed_now:
            registry_service.update_vector_point_statuses(confirmed_now, "confirmed")

    already_written_chunk_ids = registry_service.existing_vector_chunk_ids_by_status(
        embedded_chunk_ids,
        collection_name,
        statuses=["confirmed"],
    )

    filtered_items: List[Dict[str, Any]] = []
    filtered_embeddings: List[List[float]] = []
    postcheck_skipped_existing = 0
    for item, vector in zip(embedded_items, embeddings):
        chunk = item["chunk"]
        if chunk.chunk_id in already_written_chunk_ids:
            postcheck_skipped_existing += 1
            continue
        filtered_items.append(item)
        filtered_embeddings.append(vector)

    logger.info(
        "Embed filter summary doc=%s collection=%s embedded_candidates=%s postcheck_skipped=%s final_to_write=%s",
        document.doc_id,
        collection_name,
        len(embedded_items),
        postcheck_skipped_existing,
        len(filtered_items),
    )

    if not filtered_items:
        logger.info("All prepared chunks already exist in collection %s", collection_name)
        return stats

    embedded_items = filtered_items
    embeddings = filtered_embeddings

    logger.info(
        "Embedding target collection=%s model=%s vector_dim=%s batch_size=%s",
        collection_name,
        model_name,
        vector_dim,
        embed_batch_size,
    )

    points = []
    point_records: List[VectorPoint] = []
    from qdrant_client.models import PointStruct
    for item, vector in zip(embedded_items, embeddings):
        chunk = item["chunk"]
        chunk_text = item["chunk_text"]
        chunk_meta = item.get("chunk_meta", {})
        content_hash = hashlib.sha256(chunk_text.encode("utf-8")).hexdigest()
        point_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{document.doc_id}:{chunk.chunk_index}:{content_hash}"))
        payload = {
            "text": chunk_text,
            "title": chunk_meta.get("title") or document.title,
            "source_uri": chunk_meta.get("source_uri") or source_item.uri,
            "source_type": chunk_meta.get("source_type") or source.source_type or "text",
            "chunk_index": chunk.chunk_index,
            "kb": kb_name,
            "source_id": source.source_id,
            "document_id": document.doc_id,
            "chunk_id": chunk.chunk_id,
            "embedding_model": model_name,
            "content_hash": content_hash,
        }
        points.append(PointStruct(id=point_id, vector=vector, payload=payload))

        point_records.append(VectorPoint(
            point_id=point_id,
            chunk_id=chunk.chunk_id,
            collection_name=collection_name,
            alias_name="",
            embedding_model=model_name,
            embedding_dim=len(vector),
            vector_hash=content_hash,
            status="pending"
        ))

    write_batch_size = max(1, qdrant_batch_size)
    registry_service.add_vector_points(point_records)
    points_written = _VECTOR_STORE_SERVICE.upsert_points(
        qdrant_client,
        collection_name=collection_name,
        points=points,
        batch_size=write_batch_size,
    )
    point_ids = [record.point_id for record in point_records]
    registry_service.update_vector_point_statuses(point_ids, "upserted_remote")
    registry_service.update_vector_point_statuses(point_ids, "confirmed")

    stats["chunks_embedded"] = len(embedded_items)
    stats["points_stored"] = points_written
    logger.info(
        "Stored %s vectors in Qdrant collection %s (qdrant_batch_size=%s)",
        points_written,
        collection_name,
        write_batch_size,
    )
    return stats

if __name__ == "__main__":
    # Basic usage example
    registry = IngestionRegistry()
    print("warlock_ingester core components initialized")
