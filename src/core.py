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
from dataclasses import dataclass, asdict
import logging
import uuid
import json
import httpx
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
    ".bz2": "application/x-bzip2",
    ".zim": "application/x-zim",
}


def detect_source_type_and_mime(file_path: Path) -> Tuple[str, str]:
    """Detect source_type and mime_type for a file path."""
    suffix = file_path.suffix.lower()
    mime_type = EXTENSION_MIME_MAP.get(suffix, "application/octet-stream")

    if suffix == ".zim":
        return "zim", mime_type
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
    """Handles chunking of documents based on sections."""
    
    def __init__(self, max_tokens: int = 1000, overlap_tokens: int = 100):
        self.max_tokens = max_tokens
        self.overlap_tokens = overlap_tokens
        
    def chunk_text(self, text: str, section: str = "", metadata: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """
        Break text into chunks.
        
        Args:
            text (str): The text to chunk
            section (str): Section identifier
            metadata (Dict[str, Any]): Metadata associated with the text
            
        Returns:
            List[Dict[str, Any]]: List of chunks with their metadata
        """
        # For simplicity in this implementation, we'll do basic chunking
        # In a real implementation, this would leverage tokenization
        chunks = []
        chunk_start = 0
        chunk_index = 0
        
        # Split text into paragraphs for initial chunking  
        paragraphs = text.strip().split('\n\n')
        current_chunk = ""
        current_tokens = 0
        
        for para in paragraphs:
            # Estimate tokens (simple approximation)
            para_tokens = len(para) // 4  # Rough estimation
            
            if current_tokens + para_tokens > self.max_tokens and current_chunk:
                # Save current chunk
                chunks.append({
                    'chunk_text': current_chunk.strip(),
                    'chunk_index': chunk_index,
                    'section': section,
                    'char_start': chunk_start,
                    'char_end': chunk_start + len(current_chunk),
                    'token_estimate': current_tokens,
                    'metadata': metadata or {}
                })
                chunk_index += 1
                current_chunk = ""
                current_tokens = 0
                chunk_start = chunk_start + len(current_chunk)
            
            current_chunk += para + '\n\n'
            current_tokens += para_tokens
            
        # Add final chunk
        if current_chunk:
            chunks.append({
                'chunk_text': current_chunk.strip(),
                'chunk_index': chunk_index,
                'section': section,
                'char_start': chunk_start,
                'char_end': chunk_start + len(current_chunk),
                'token_estimate': current_tokens,
                'metadata': metadata or {}
            })
            
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

# Main ingestion pipeline
def run_ingestion_pipeline(source_id: str, registry: IngestionRegistry,
                          qdrant_client, embedder: str, kb_name: str = "wiki_kb"):
    """
    Run the full ingestion pipeline for a source.
    """
    logger.info(f"Starting ingestion pipeline for source: {source_id}")
    
    # Start new ingestion run
    
    
    run_id = f"run_{uuid.uuid4().hex}"
    source = registry.get_source(source_id)
    
    if not source:
        logger.error(f"Source {source_id} not found")
        return
        
    run = IngestionRun(
        ingest_run_id=run_id,
        source_id=source_id,
        started_at=str(int(time.time())),
        status="running"
    )
    registry.add_ingestion_run(run)
    
    try:
        # Discover source files and register them before processing
        if source.source_type == "folder":
            for file_path in Path(source.root_uri).rglob("*"):
                if not file_path.is_file():
                    continue

                stat = file_path.stat()
                _, mime_type = detect_source_type_and_mime(file_path)
                item_id = f"item_{hashlib.md5(str(file_path).encode('utf-8')).hexdigest()}"
                source_item = SourceItem(
                    source_item_id=item_id,
                    source_id=source.source_id,
                    uri=str(file_path),
                    display_uri=str(file_path.relative_to(source.root_uri)),
                    mime_type=mime_type,
                    size_bytes=stat.st_size,
                    mtime=str(int(stat.st_mtime)),
                    content_hash="",
                    status="pending"
                )
                registry.add_source_item(source_item)
        elif source.source_type in {"zim", "wikidump", "file", "text"}:
            source_file = Path(source.root_uri)
            if source_file.exists() and source_file.is_file():
                stat = source_file.stat()
                _, mime_type = detect_source_type_and_mime(source_file)
                item_id = f"item_{hashlib.md5(str(source_file).encode('utf-8')).hexdigest()}"
                source_item = SourceItem(
                    source_item_id=item_id,
                    source_id=source.source_id,
                    uri=str(source_file),
                    display_uri=source_file.name,
                    mime_type=mime_type,
                    size_bytes=stat.st_size,
                    mtime=str(int(stat.st_mtime)),
                    content_hash="",
                    status="pending",
                )
                registry.add_source_item(source_item)
            else:
                logger.error(f"Source file not found: {source.root_uri}")

        # Get source items to process
        items = registry.get_unprocessed_items(source_id)
        logger.info(f"Found {len(items)} items to process")
        
        # Process each item
        for item in items:
            try:
                logger.info(f"Processing item: {item.uri}")
                
                parser = ParserFactory.create_parser_for_item(item)
                parsed_docs = parser.parse_documents(item)
                doc_manager = DocumentManager(registry)
                parser_name = getattr(parser, "parser_name", "text")

                for parsed_doc in parsed_docs:
                    text = parsed_doc.get("text", "")
                    if not text.strip():
                        continue
                    metadata = parsed_doc.get("metadata", {})
                    title = parsed_doc.get("title") or Path(item.uri).name
                    source_uri = parsed_doc.get("source_uri") or item.uri
                    source_type = parsed_doc.get("source_type") or metadata.get("source_type") or source.source_type or "text"
                    doc_key = f"{source_uri}:{title}:{hashlib.sha256(text.encode('utf-8')).hexdigest()}"

                    document = doc_manager.create_document(
                        item,
                        text,
                        parser_name,
                        metadata,
                        title=title,
                        doc_key=doc_key,
                    )

                    chunker = Chunker(max_tokens=1000)
                    chunks_list = chunker.chunk_text(text, "", metadata)

                    chunked_data = []
                    for i, chunk_data in enumerate(chunks_list):
                    # Create chunk and register it
                        chunk_id = f"chunk_{hashlib.md5((document.doc_id + ':' + str(i) + ':' + chunk_data['chunk_text']).encode('utf-8')).hexdigest()}"
                        chunk = Chunk(
                            chunk_id=chunk_id,
                            doc_id=document.doc_id,
                            chunk_index=i,
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
                                },
                                "chunk_text": chunk_data['chunk_text']
                            })
                        )
                    
                        registry.add_chunk(chunk)
                        chunked_data.append(chunk)

                    embed_chunks_and_store(qdrant_client, chunked_data, embedder,
                                           document, registry, item, source, kb_name)

                registry.update_source_item_status(item.source_item_id, "completed")
                
            except Exception as e:
                logger.error(f"Error processing item {item.uri}: {str(e)}")
                # Record parser error
                error = ParserError(
                    error_id=f"err_{uuid.uuid4().hex}",
                    ingest_run_id=run_id,
                    source_item_id=item.source_item_id,
                    parser="BaseParser",
                    error_type="ProcessingError",
                    message=str(e),
                    traceback=str(sys.exc_info()[2]),
                    created_at=str(int(time.time()))
                )
                # Add error to registry (will need to add method for this)
                
        # Mark ingestion run as complete
        registry.update_ingestion_run_status(run_id, "completed", str(int(time.time())))
        logger.info(f"Ingestion pipeline complete for source: {source_id}")
        
    except Exception as e:
        logger.error(f"Error in ingestion pipeline: {str(e)}")
        registry.update_ingestion_run_status(run_id, "failed", str(int(time.time())))

def _resolve_embed_model(embedder: str) -> str:
    if embedder.startswith("ollama:"):
        return embedder.split(":", 1)[1]
    return embedder or os.getenv("OLLAMA_EMBED_MODEL", "embeddinggemma:latest")


def _collection_name_for(kb_name: str, model_name: str) -> str:
    return os.getenv("LOCALWIKI_QDRANT_COLLECTION", "local_wiki")


def _embed_texts(texts: List[str], model_name: str, ollama_url: str) -> List[List[float]]:
    payload = {"model": model_name, "input": texts}
    url = ollama_url.rstrip("/") + "/api/embed"
    with httpx.Client(timeout=120.0) as client:
        response = client.post(url, json=payload)
        response.raise_for_status()
        data = response.json()
    embeddings = data.get("embeddings") or []
    if len(embeddings) != len(texts):
        raise RuntimeError("Embedding count mismatch from Ollama")
    return embeddings


def embed_chunks_and_store(qdrant_client, chunks: List[Chunk], embedder: str,
                          document: CanonicalDocument, registry: IngestionRegistry,
                          source_item: SourceItem, source: Source, kb_name: str):
    """
    Embed chunks and store in Qdrant.
    """
    if not chunks:
        return

    model_name = _resolve_embed_model(embedder or os.getenv("OLLAMA_EMBED_MODEL", "embeddinggemma:latest"))
    ollama_url = os.getenv("OLLAMA_URL", "http://127.0.0.1:11434")
    collection_name = _collection_name_for(kb_name, model_name)
    create_collection(qdrant_client, collection_name=collection_name, vector_dim=768)

    texts = []
    chunk_text_by_id = {}
    point_batch = []
    for chunk in chunks:
        existing = registry.get_chunk_by_text_hash(chunk.text_hash)
        if existing:
            conn = sqlite3.connect(registry.db_path)
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM vector_points WHERE chunk_id = ? AND collection_name = ?", (chunk.chunk_id, collection_name))
            exists = cur.fetchone() is not None
            conn.close()
            if exists:
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
            continue
        texts.append(chunk_text)
        chunk_text_by_id[chunk.chunk_id] = {"text": chunk_text, "meta": chunk_meta}
        point_batch.append(chunk)

    if not point_batch:
        logger.info("No new chunks to embed for this document")
        return

    logger.info(f"Encoding {len(point_batch)} chunks for document {document.doc_id}")
    embeddings = _embed_texts(texts, model_name, ollama_url)

    points = []
    from qdrant_client.models import PointStruct
    for chunk, vector in zip(point_batch, embeddings):
        chunk_info = chunk_text_by_id[chunk.chunk_id]
        chunk_text = chunk_info["text"]
        chunk_meta = chunk_info.get("meta", {})
        content_hash = hashlib.sha256(chunk_text.encode("utf-8")).hexdigest()
        point_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{document.doc_id}:{chunk.chunk_index}:{content_hash}"))
        payload = {
            "text": chunk_text[:4000],
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

        registry.add_vector_point(VectorPoint(
            point_id=point_id,
            chunk_id=chunk.chunk_id,
            collection_name=collection_name,
            alias_name="",
            embedding_model=model_name,
            embedding_dim=len(vector),
            vector_hash=content_hash,
            status="completed"
        ))

    batch_size = 64
    for i in range(0, len(points), batch_size):
        qdrant_client.upsert(collection_name=collection_name, points=points[i:i+batch_size])

    logger.info(f"Stored {len(points)} vectors in Qdrant collection {collection_name}")

if __name__ == "__main__":
    # Basic usage example
    registry = IngestionRegistry()
    print("warlock_ingester core components initialized")
