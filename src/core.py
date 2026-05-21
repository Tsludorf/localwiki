#!/usr/bin/env python3
"""
warlock_ingester - A local data ingestion system for building knowledge bases using Qdrant vector storage.
"""

import os
import sys
import sqlite3
import hashlib
import time
import traceback
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, asdict
import logging
import uuid
import json
import httpx
import re
from collections import Counter
from src.parsers import ParserFactory
from src.qdrant_utils import create_collection


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

DEFAULT_EMBED_BATCH_SIZE = 64
DEFAULT_QDRANT_BATCH_SIZE = 256
DEFAULT_CHUNK_TOKENS = 600
DEFAULT_CHUNK_OVERLAP = 100
DEFAULT_MAX_CHUNK_CHARS = 4000
DEFAULT_MINILM_EMBED_MAX_TOKENS = 600
EMBED_CONTEXT_FALLBACK_TOKENS = [600, 384, 300, 192, 128]


def _estimate_token_count(text: str) -> int:
    return len(re.findall(r"\S+", text or ""))


def _token_caps_for_retry(initial_cap: int) -> List[int]:
    ordered: List[int] = [max(1, initial_cap)]
    for cap in EMBED_CONTEXT_FALLBACK_TOKENS:
        if cap not in ordered:
            ordered.append(cap)
    ordered = sorted(set(ordered), reverse=True)
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


def run_ingestion_pipeline(source_id: str, registry: IngestionRegistry,
                          qdrant_client, embedder: str, kb_name: str = "wiki_kb",
                          limit: int = 10000,
                          collection_name: Optional[str] = None,
                          changed_only: bool = False,
                          embed_batch_size: int = DEFAULT_EMBED_BATCH_SIZE,
                          qdrant_batch_size: int = DEFAULT_QDRANT_BATCH_SIZE,
                          chunk_tokens: int = DEFAULT_CHUNK_TOKENS,
                          chunk_overlap: int = DEFAULT_CHUNK_OVERLAP) -> Dict[str, int]:
    """
    Run the full ingestion pipeline for a source.
    """
    logger.info(f"Starting ingestion pipeline for source: {source_id}")
    run_started_at = time.time()
    model_name = _resolve_embed_model(embedder or os.getenv("OLLAMA_EMBED_MODEL", "all-minilm"))
    effective_limit = limit if (limit and limit > 0) else None
    print(f"[Ingestion Start] source={source_id} model={model_name} kb={kb_name}")
    print(
        "[Ingestion Config] "
        f"chunk_tokens={chunk_tokens} "
        f"chunk_overlap={chunk_overlap} "
        f"embed_batch_size={embed_batch_size} "
        f"qdrant_batch_size={qdrant_batch_size} "
        f"max_chunk_chars={DEFAULT_MAX_CHUNK_CHARS} "
        f"changed_only={changed_only} "
        f"limit={effective_limit if effective_limit is not None else 'unlimited'} "
        f"collection_override={collection_name or 'auto'}"
    )
    stats: Dict[str, int] = {
        "documents_discovered": 0,
        "documents_parsed": 0,
        "chunks_prepared": 0,
        "chunks_created": 0,
        "chunks_embedded": 0,
        "points_stored": 0,
        "elapsed_seconds": 0,
        "avg_chunks_per_sec": 0,
        "status": "running",
    }
    
    # Start new ingestion run
    
    
    run_id = f"run_{uuid.uuid4().hex}"
    source = registry.get_source(source_id)
    
    if not source:
        logger.error(f"Source {source_id} not found")
        return stats
        
    run = IngestionRun(
        ingest_run_id=run_id,
        source_id=source_id,
        started_at=str(int(time.time())),
        status="running"
    )
    registry.add_ingestion_run(run)
    
    had_item_errors = False
    processed_items = 0
    failed_items = 0

    try:
        discovery_seen = 0
        discovery_registered = 0
        discovery_skipped_unchanged = 0
        discovered_item_ids = set()

        # Discover source files and register them before processing
        if source.source_type == "folder":
            for file_path in Path(source.root_uri).rglob("*"):
                if not file_path.is_file():
                    continue

                discovery_seen += 1

                stat = file_path.stat()
                _, mime_type = detect_source_type_and_mime(file_path)
                item_id = f"item_{hashlib.md5(str(file_path).encode('utf-8')).hexdigest()}"
                discovered_item_ids.add(item_id)
                mtime = str(int(stat.st_mtime))
                existing = registry.get_source_item(item_id)
                if (
                    changed_only
                    and existing
                    and existing.status == "completed"
                    and existing.size_bytes == stat.st_size
                    and existing.mtime == mtime
                ):
                    discovery_skipped_unchanged += 1
                    continue
                source_item = SourceItem(
                    source_item_id=item_id,
                    source_id=source.source_id,
                    uri=str(file_path),
                    display_uri=str(file_path.relative_to(source.root_uri)),
                    mime_type=mime_type,
                    size_bytes=stat.st_size,
                    mtime=mtime,
                    content_hash=f"{stat.st_size}:{mtime}",
                    status="pending"
                )
                registry.add_source_item(source_item)
                discovery_registered += 1
        elif source.source_type in {"zim", "wikidump", "file", "text", "pdf"}:
            source_file = Path(source.root_uri)
            if source_file.exists() and source_file.is_file():
                discovery_seen += 1
                stat = source_file.stat()
                _, mime_type = detect_source_type_and_mime(source_file)
                item_id = f"item_{hashlib.md5(str(source_file).encode('utf-8')).hexdigest()}"
                discovered_item_ids.add(item_id)
                mtime = str(int(stat.st_mtime))
                existing = registry.get_source_item(item_id)
                if (
                    changed_only
                    and existing
                    and existing.status == "completed"
                    and existing.size_bytes == stat.st_size
                    and existing.mtime == mtime
                ):
                    discovery_skipped_unchanged += 1
                    logger.info("Skipping unchanged source item: %s", source_file)
                else:
                    source_item = SourceItem(
                        source_item_id=item_id,
                        source_id=source.source_id,
                        uri=str(source_file),
                        display_uri=source_file.name,
                        mime_type=mime_type,
                        size_bytes=stat.st_size,
                        mtime=mtime,
                        content_hash=f"{stat.st_size}:{mtime}",
                        status="pending",
                    )
                    registry.add_source_item(source_item)
                    discovery_registered += 1
            else:
                logger.error(f"Source file not found: {source.root_uri}")

        logger.info(
            "Discovery summary for source=%s: seen=%s registered=%s skipped_unchanged=%s changed_only=%s",
            source_id,
            discovery_seen,
            discovery_registered,
            discovery_skipped_unchanged,
            changed_only,
        )

        # Get source items to process
        items = registry.get_unprocessed_items(source_id)
        if changed_only:
            before_filter = len(items)
            items = [item for item in items if item.source_item_id in discovered_item_ids]
            logger.info(
                "Changed-only queue filter applied: before=%s after=%s filtered_out=%s",
                before_filter,
                len(items),
                before_filter - len(items),
            )
        status_counts = Counter(item.status for item in items)
        logger.info(
            "Queued items for processing: total=%s status_breakdown=%s",
            len(items),
            dict(status_counts),
        )
        
        # Process each item
        for item in items:
            try:
                parser = None
                logger.info(f"Processing item: {item.uri}")
                chunks_before_item = stats["chunks_created"]
                chunks_prepared_before_item = stats["chunks_prepared"]
                embedded_before_item = stats["chunks_embedded"]
                vectors_before_item = stats["points_stored"]
                
                parser = ParserFactory.create_parser_for_item(item)
                parsed_docs = parser.parse_documents(item)
                logger.info(
                    "Parser output for item=%s parser=%s docs=%s",
                    item.source_item_id,
                    getattr(parser, "parser_name", "unknown"),
                    len(parsed_docs),
                )
                if effective_limit is not None:
                    remaining = effective_limit - stats["documents_discovered"]
                    if remaining <= 0:
                        break
                    parsed_docs = parsed_docs[:remaining]
                stats["documents_discovered"] += len(parsed_docs)
                doc_manager = DocumentManager(registry)
                parser_name = getattr(parser, "parser_name", "text")

                item_docs_empty = 0
                item_docs_low_value = 0
                item_chunks_raw = 0
                item_chunks_truncated = 0
                item_chunks_low_value = 0

                for parsed_doc in parsed_docs:
                    text = parsed_doc.get("text", "")
                    if not text.strip():
                        item_docs_empty += 1
                        continue
                    metadata = parsed_doc.get("metadata", {})
                    title = parsed_doc.get("title") or Path(item.uri).name
                    if _is_low_value_chunk(text, title=title):
                        item_docs_low_value += 1
                        continue
                    source_uri = parsed_doc.get("source_uri") or item.uri
                    source_type = parsed_doc.get("source_type") or metadata.get("source_type") or source.source_type or "text"
                    display_uri = metadata.get("display_uri") or item.display_uri or Path(item.uri).name
                    doc_key = f"{source_uri}:{title}:{hashlib.sha256(text.encode('utf-8')).hexdigest()}"

                    stats["documents_parsed"] += 1

                    document = doc_manager.create_document(
                        item,
                        text,
                        parser_name,
                        metadata,
                        title=title,
                        doc_key=doc_key,
                    )

                    chunker = Chunker(max_tokens=chunk_tokens, overlap_tokens=chunk_overlap, min_tokens=300)
                    chunks_list = chunker.chunk_text(text, "", metadata)
                    item_chunks_raw += len(chunks_list)

                    chunked_data = []
                    for chunk_data in chunks_list:
                        if len(chunk_data["chunk_text"]) > DEFAULT_MAX_CHUNK_CHARS:
                            chunk_data["chunk_text"] = chunk_data["chunk_text"][:DEFAULT_MAX_CHUNK_CHARS]
                            item_chunks_truncated += 1
                        if _is_low_value_chunk(chunk_data["chunk_text"], title=title):
                            item_chunks_low_value += 1
                            continue
                    # Create chunk and register it
                        chunk_id = f"chunk_{hashlib.md5((document.doc_id + ':' + str(chunk_data['chunk_index']) + ':' + chunk_data['chunk_text']).encode('utf-8')).hexdigest()}"
                        chunk = Chunk(
                            chunk_id=chunk_id,
                            doc_id=document.doc_id,
                            chunk_index=chunk_data['chunk_index'],
                            section=chunk_data['section'],
                            char_start=chunk_data['char_start'],
                            char_end=chunk_data['char_end'],
                            token_estimate=chunk_data['token_estimate'],
                            text_hash=doc_manager.generate_hash(chunk_data['chunk_text']),
                            citation_json=json.dumps({
                                "metadata": {
                                    **(chunk_data.get('metadata') or {}),
                                    "title": title,
                                    "source_uri": source_uri,
                                    "source_type": source_type,
                                    "display_uri": display_uri,
                                },
                                "chunk_text": chunk_data['chunk_text']
                            })
                        )
                        
                        registry.add_chunk(chunk)
                        chunked_data.append(chunk)
                    stats["chunks_prepared"] += len(chunked_data)

                    embed_stats = embed_chunks_and_store(
                        qdrant_client,
                        chunked_data,
                        embedder,
                        document,
                        registry,
                        item,
                        source,
                        kb_name,
                        collection_name=collection_name,
                        embed_batch_size=embed_batch_size,
                        qdrant_batch_size=qdrant_batch_size,
                    )
                    stats["chunks_created"] += embed_stats["chunks_embedded"]
                    stats["chunks_embedded"] += embed_stats["chunks_embedded"]
                    stats["points_stored"] += embed_stats["points_stored"]

                if effective_limit is not None and stats["documents_discovered"] >= effective_limit:
                    logger.info("Reached ingest limit (%s documents discovered)", effective_limit)
                    break

                registry.update_source_item_status(item.source_item_id, "completed")
                processed_items += 1
                elapsed = max(0.001, time.time() - run_started_at)
                chunks_per_sec = stats["chunks_created"] / elapsed
                item_chunks = stats["chunks_created"] - chunks_before_item
                item_chunks_prepared = stats["chunks_prepared"] - chunks_prepared_before_item
                item_embedded = stats["chunks_embedded"] - embedded_before_item
                item_vectors = stats["points_stored"] - vectors_before_item
                logger.info(
                    "Item summary id=%s docs_discovered=%s docs_empty=%s docs_low_value=%s chunks_raw=%s chunks_truncated=%s chunks_low_value=%s chunks_prepared=%s chunks_created=%s chunks_embedded=%s vectors_written=%s",
                    item.source_item_id,
                    len(parsed_docs),
                    item_docs_empty,
                    item_docs_low_value,
                    item_chunks_raw,
                    item_chunks_truncated,
                    item_chunks_low_value,
                    item_chunks_prepared,
                    item_chunks,
                    item_embedded,
                    item_vectors,
                )
                print(
                    "[Ingestion Progress] "
                    f"docs={stats['documents_parsed']} "
                    f"chunks_prepared={stats['chunks_prepared']} "
                    f"chunks_created={stats['chunks_created']} "
                    f"embedded={stats['chunks_embedded']} "
                    f"vectors={stats['points_stored']} "
                    f"item_chunks_prepared={item_chunks_prepared} "
                    f"item_chunks_created={item_chunks} "
                    f"chunks/s={chunks_per_sec:.2f}"
                )
                
            except Exception as e:
                logger.error(f"Error processing item {item.uri}: {str(e)}")
                had_item_errors = True
                failed_items += 1
                registry.update_source_item_status(item.source_item_id, "failed")
                # Record parser error
                error = ParserError(
                    error_id=f"err_{uuid.uuid4().hex}",
                    ingest_run_id=run_id,
                    source_item_id=item.source_item_id,
                    parser=getattr(locals().get("parser"), "parser_name", "unknown"),
                    error_type="ProcessingError",
                    message=str(e),
                    traceback=traceback.format_exc(),
                    created_at=str(int(time.time()))
                )
                registry.add_parser_error(error)
                
        if had_item_errors:
            registry.update_ingestion_run_status(run_id, "failed", str(int(time.time())))
            raise RuntimeError(
                f"Ingestion failed for source {source_id}: processed={processed_items}, failed={failed_items}"
            )

        # Mark ingestion run as complete
        registry.update_ingestion_run_status(run_id, "completed", str(int(time.time())))
        logger.info(f"Ingestion pipeline complete for source: {source_id}")
        elapsed = max(0.001, time.time() - run_started_at)
        stats["elapsed_seconds"] = int(elapsed)
        stats["avg_chunks_per_sec"] = round(stats["chunks_created"] / elapsed, 2)
        stats["status"] = "completed"
        print(
            "[Ingestion Complete] "
            f"source={source_id} model={model_name} "
            f"chunks_prepared={stats['chunks_prepared']} "
            f"chunks_created={stats['chunks_created']} "
            f"chunks/s={stats['avg_chunks_per_sec']:.2f}"
        )
        return stats
        
    except Exception as e:
        logger.error(f"Error in ingestion pipeline: {str(e)}")
        registry.update_ingestion_run_status(run_id, "failed", str(int(time.time())))
        elapsed = max(0.001, time.time() - run_started_at)
        stats["elapsed_seconds"] = int(elapsed)
        stats["avg_chunks_per_sec"] = round(stats["chunks_created"] / elapsed, 2)
        stats["status"] = "failed"
        return stats

def _resolve_embed_model(embedder: str) -> str:
    if embedder.startswith("ollama:"):
        return embedder.split(":", 1)[1]
    model = embedder or os.getenv("OLLAMA_EMBED_MODEL", "all-minilm:latest")
    if ":" not in model:
        return f"{model}:latest"
    return model


def _truncate_text_for_embedding(text: str, max_tokens: int, max_chars: int) -> Tuple[str, bool]:
    candidate = (text or "").strip()
    if not candidate:
        return "", False

    token_matches = list(re.finditer(r"\S+", candidate))
    truncated = False
    if max_tokens > 0 and len(token_matches) > max_tokens:
        end_char = token_matches[max_tokens - 1].end()
        candidate = candidate[:end_char].strip()
        truncated = True

    if max_chars > 0 and len(candidate) > max_chars:
        candidate = candidate[:max_chars].strip()
        truncated = True

    return candidate, truncated


def _collection_name_for(kb_name: str, model_name: str, collection_name: Optional[str] = None) -> str:
    if collection_name:
        return collection_name
    return ""


def resolve_collection_name(embedder: str, collection_name: Optional[str] = None, kb_name: str = "") -> str:
    if collection_name:
        return collection_name
    model_name = _resolve_embed_model(embedder or os.getenv("OLLAMA_EMBED_MODEL", "all-minilm:latest"))
    model_low = model_name.lower()
    if "minilm" in model_low:
        return "local_wiki_384"
    if "embeddinggemma" in model_low:
        return "local_wiki_768"
    if "bge-m3" in model_low or "bgem3" in model_low:
        return "local_wiki_1024"
    return ""


def infer_embedding_dimension(embedder: str, ollama_url: Optional[str] = None) -> int:
    model_name = _resolve_embed_model(embedder or os.getenv("OLLAMA_EMBED_MODEL", "all-minilm:latest"))
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
    collections = qdrant_client.get_collections()
    existing_names = {col.name for col in collections.collections}
    if collection_name not in existing_names:
        create_collection(qdrant_client, collection_name=collection_name, vector_dim=vector_dim)
        logger.info("Created collection %s with vector dim %s", collection_name, vector_dim)
        return

    info = qdrant_client.get_collection(collection_name)
    existing_dim = _extract_collection_vector_dim(info)
    if existing_dim is not None and existing_dim != vector_dim:
        recommended = _recommended_collection_name(model_name, vector_dim)
        raise RuntimeError(
            f"Collection {collection_name} has dim {existing_dim} but embedder {model_name} returned dim {vector_dim}. "
            f"Use a separate collection (e.g. {recommended}) or recreate the collection."
        )


def _prepare_embed_items(
    texts: List[str],
    max_tokens: int,
    max_chars: int,
) -> Tuple[List[str], List[int], int, int]:
    prepared_texts: List[str] = []
    kept_indices: List[int] = []
    truncated_count = 0
    dropped_count = 0

    for idx, text in enumerate(texts):
        prepared, truncated = _truncate_text_for_embedding(text, max_tokens=max_tokens, max_chars=max_chars)
        if not prepared:
            dropped_count += 1
            continue
        prepared_texts.append(prepared)
        kept_indices.append(idx)
        if truncated:
            truncated_count += 1

    return prepared_texts, kept_indices, truncated_count, dropped_count


def _embed_texts(
    texts: List[str],
    model_name: str,
    ollama_url: str,
    batch_size: int = DEFAULT_EMBED_BATCH_SIZE,
) -> Tuple[List[List[float]], List[int]]:
    if not texts:
        return [], []

    url = ollama_url.rstrip("/") + "/api/embed"
    batch_size = max(1, batch_size)
    configured_max_tokens = int(os.getenv("LOCALWIKI_EMBED_MAX_TOKENS", "0"))
    default_max_tokens = DEFAULT_MINILM_EMBED_MAX_TOKENS if "minilm" in model_name.lower() else DEFAULT_CHUNK_TOKENS
    initial_max_tokens = configured_max_tokens if configured_max_tokens > 0 else default_max_tokens
    initial_max_chars = int(os.getenv("LOCALWIKI_EMBED_MAX_CHARS", str(DEFAULT_MAX_CHUNK_CHARS)))
    token_caps = _token_caps_for_retry(initial_max_tokens)
    timeout_seconds = float(os.getenv("LOCALWIKI_EMBED_TIMEOUT_SECONDS", "600"))

    all_embeddings: List[List[float]] = []
    all_kept_indices: List[int] = []

    with httpx.Client(timeout=timeout_seconds) as client:
        for i in range(0, len(texts), batch_size):
            batch_texts = texts[i:i + batch_size]
            batch_global_indices = list(range(i, i + len(batch_texts)))
            batch_index = (i // batch_size) + 1
            last_error = ""
            attempted_caps: List[int] = []
            succeeded = False

            for cap in token_caps:
                attempted_caps.append(cap)
                cap_chars = max(256, int(initial_max_chars * (cap / max(1, initial_max_tokens))))
                prepared_texts, kept_local_indices, truncated_count, dropped_count = _prepare_embed_items(
                    batch_texts,
                    max_tokens=cap,
                    max_chars=cap_chars,
                )
                if not prepared_texts:
                    logger.warning(
                        "Embedding batch %s dropped entirely after preprocessing (model=%s cap_tokens=%s cap_chars=%s)",
                        batch_index,
                        model_name,
                        cap,
                        cap_chars,
                    )
                    succeeded = True
                    break

                prepared_char_lengths = [len(t) for t in prepared_texts]
                prepared_token_lengths = [_estimate_token_count(t) for t in prepared_texts]
                if truncated_count or dropped_count:
                    logger.info(
                        "Embedding batch %s preprocessing: truncated=%s dropped=%s kept=%s (model=%s cap_tokens=%s cap_chars=%s)",
                        batch_index,
                        truncated_count,
                        dropped_count,
                        len(prepared_texts),
                        model_name,
                        cap,
                        cap_chars,
                    )
                logger.info(
                    "Embedding batch %s: size=%s chars_total=%s chars[min=%s max=%s] est_tokens_total=%s est_tokens[min=%s max=%s] cap_tokens=%s cap_chars=%s",
                    batch_index,
                    len(prepared_texts),
                    sum(prepared_char_lengths),
                    min(prepared_char_lengths),
                    max(prepared_char_lengths),
                    sum(prepared_token_lengths),
                    min(prepared_token_lengths),
                    max(prepared_token_lengths),
                    cap,
                    cap_chars,
                )

                payload = {"model": model_name, "input": prepared_texts}
                response = client.post(url, json=payload)
                if response.status_code >= 400:
                    detail = (response.text or "").strip()
                    if len(detail) > 500:
                        detail = detail[:500] + "..."
                    last_error = detail
                    if response.status_code == 400 and _is_context_length_error(detail) and cap != token_caps[-1]:
                        next_cap = token_caps[token_caps.index(cap) + 1]
                        logger.warning(
                            "Embedding batch %s hit context limit at cap_tokens=%s; retrying with cap_tokens=%s",
                            batch_index,
                            cap,
                            next_cap,
                        )
                        continue
                    if response.status_code == 400 and _is_context_length_error(detail):
                        break
                    raise RuntimeError(
                        "Ollama embedding request failed "
                        f"(status={response.status_code}, model={model_name}, batch={len(prepared_texts)}, "
                        f"cap_tokens={cap}, cap_chars={cap_chars}): {detail}"
                    )

                data = response.json()
                embeddings = data.get("embeddings") or []
                if len(embeddings) != len(prepared_texts):
                    raise RuntimeError(
                        f"Embedding count mismatch from Ollama: expected {len(prepared_texts)} got {len(embeddings)}"
                    )

                all_embeddings.extend(embeddings)
                all_kept_indices.extend([batch_global_indices[idx] for idx in kept_local_indices])
                succeeded = True
                break

            if not succeeded:
                raise RuntimeError(
                    "Ollama embedding request failed after context retries "
                    f"(model={model_name}, attempted_token_caps={attempted_caps}): {last_error}"
                )

    if len(all_embeddings) != len(all_kept_indices):
        raise RuntimeError("Embedding alignment mismatch between vectors and source chunks")

    return all_embeddings, all_kept_indices


def _build_embedding_text(
    chunk_text: str,
    chunk_meta: Dict[str, Any],
    document: CanonicalDocument,
    source_item: SourceItem,
    source: Source,
) -> str:
    title = (chunk_meta.get("title") or document.title or "").strip()
    source_uri = (chunk_meta.get("source_uri") or source_item.uri or "").strip()
    source_type = (chunk_meta.get("source_type") or source.source_type or "text").strip()
    display_uri = (chunk_meta.get("display_uri") or source_item.display_uri or "").strip()

    context_lines = [
        f"Title: {title}" if title else "",
        f"Source: {display_uri or source_uri}" if (display_uri or source_uri) else "",
        f"Source Type: {source_type}" if source_type else "",
    ]
    context = "\n".join(line for line in context_lines if line)
    if not context:
        return chunk_text
    return f"{context}\n\n{chunk_text}".strip()


def embed_chunks_and_store(qdrant_client, chunks: List[Chunk], embedder: str,
                          document: CanonicalDocument, registry: IngestionRegistry,
                          source_item: SourceItem, source: Source, kb_name: str,
                          collection_name: Optional[str] = None,
                          embed_batch_size: int = DEFAULT_EMBED_BATCH_SIZE,
                          qdrant_batch_size: int = DEFAULT_QDRANT_BATCH_SIZE) -> Dict[str, int]:
    """
    Embed chunks and store in Qdrant.
    """
    stats = {"chunks_embedded": 0, "points_stored": 0}
    if not chunks:
        return stats

    model_name = _resolve_embed_model(embedder or os.getenv("OLLAMA_EMBED_MODEL", "all-minilm:latest"))
    ollama_url = os.getenv("OLLAMA_URL", "http://127.0.0.1:11434")
    resolved_collection_name = _collection_name_for(kb_name, model_name, collection_name)
    precheck_collection_name = resolved_collection_name or collection_name

    prepared_items: List[Dict[str, Any]] = []
    precheck_skipped_existing = 0
    skipped_missing_text = 0
    for chunk in chunks:
        existing = registry.get_chunk_by_text_hash(chunk.text_hash)
        if existing and precheck_collection_name and registry.vector_point_exists(chunk.chunk_id, precheck_collection_name):
                precheck_skipped_existing += 1
                continue
        conn = sqlite3.connect(registry.db_path)
        cur = conn.cursor()
        cur.execute("SELECT citation_json FROM chunks WHERE chunk_id = ?", (chunk.chunk_id,))
        row = cur.fetchone()
        conn.close()
        chunk_text = ""
        chunk_meta = {}
        if row and row[0]:
            try:
                meta = json.loads(row[0]) if row[0].startswith("{") else {}
                chunk_text = meta.get("chunk_text", "")
                chunk_meta = meta.get("metadata", {})
            except Exception:
                chunk_text = ""
        if not chunk_text:
            skipped_missing_text += 1
            continue
        embed_text = _build_embedding_text(chunk_text, chunk_meta, document, source_item, source)
        prepared_items.append({
            "chunk": chunk,
            "embed_text": embed_text,
            "chunk_text": chunk_text,
            "chunk_meta": chunk_meta,
        })

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
    embeddings, kept_indices = _embed_texts(embed_texts, model_name, ollama_url, batch_size=embed_batch_size)
    embedded_items = [prepared_items[idx] for idx in kept_indices]

    if not embedded_items:
        logger.info("No embeddable chunks left after preprocessing for document %s", document.doc_id)
        return stats

    vector_dim = len(embeddings[0])
    collection_name = resolved_collection_name or collection_name_for_dimension(vector_dim)
    _ensure_collection_compatible(qdrant_client, collection_name, model_name, vector_dim)

    filtered_items: List[Dict[str, Any]] = []
    filtered_embeddings: List[List[float]] = []
    postcheck_skipped_existing = 0
    for item, vector in zip(embedded_items, embeddings):
        chunk = item["chunk"]
        if registry.vector_point_exists(chunk.chunk_id, collection_name):
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
            status="completed"
        ))

    write_batch_size = max(1, qdrant_batch_size)
    points_written = 0
    for i in range(0, len(points), write_batch_size):
        batch = points[i:i+write_batch_size]
        qdrant_client.upsert(collection_name=collection_name, points=batch)
        for record in point_records[i:i+write_batch_size]:
            registry.add_vector_point(record)
        points_written += len(batch)

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
