#!/usr/bin/env python3
"""
Test file for warlock_ingester components.
"""

import os
import sys
import tempfile
import shutil
from pathlib import Path

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from core import IngestionRegistry, Source, SourceItem, BaseParser, detect_source_type_and_mime
from parsers import ParserFactory
import pytest


def test_detect_source_type_and_mime():
    zim_type, zim_mime = detect_source_type_and_mime(Path("/tmp/wiki.zim"))
    assert zim_type == "zim"
    assert zim_mime == "application/x-zim"

    dump_type, dump_mime = detect_source_type_and_mime(Path("/tmp/wiki.jsonl"))
    assert dump_type == "wikidump"
    assert dump_mime == "application/x-ndjson"

    text_type, text_mime = detect_source_type_and_mime(Path("/tmp/readme.md"))
    assert text_type == "text"
    assert text_mime == "text/markdown"


def test_parser_factory_ndjson_parser_selection():
    item = SourceItem(
        source_item_id="item_parser_factory",
        source_id="source_parser_factory",
        uri="/tmp/wiki.ndjson",
        display_uri="wiki.ndjson",
        mime_type="application/x-ndjson",
        size_bytes=1,
        mtime="0",
        content_hash="",
        status="pending",
    )
    parser = ParserFactory.create_parser_for_item(item)
    assert getattr(parser, "parser_name", "") == "wikiextractor-jsonl"

def test_registry_initialization():
    """Test that the registry initializes correctly."""
    # Create a temporary database for testing
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
        db_path = tmp.name
    
    try:
        registry = IngestionRegistry(db_path=db_path)
        
        # Test adding a source
        source = Source(
            source_id="test_source_123",
            source_type="folder",
            root_uri="/test/path",
            display_name="Test Source",
            added_at="2023-01-01T00:00:00Z",
            settings_json='{"test": "value"}'
        )
        
        registry.add_source(source)
        
        # Test retrieving the source
        retrieved_source = registry.get_source("test_source_123")
        assert retrieved_source is not None
        assert retrieved_source.source_id == "test_source_123"
        assert retrieved_source.source_type == "folder"
        assert retrieved_source.display_name == "Test Source"
        assert retrieved_source.root_uri == "/test/path"
        
        print("Registry tests passed!")
        
    finally:
        # Clean up
        if os.path.exists(db_path):
            os.unlink(db_path)

def test_source_item():
    """Test source item functionality."""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
        db_path = tmp.name
    
    try:
        registry = IngestionRegistry(db_path=db_path)
        
        # Test adding a source item
        item = SourceItem(
            source_item_id="test_item_456",
            source_id="test_source_123",
            uri="/test/file.txt",
            display_uri="file.txt",
            mime_type="text/plain",
            size_bytes=1024,
            mtime="2023-01-01T00:00:00Z",
            content_hash="test_hash_123456",
            status="pending"
        )
        
        registry.add_source_item(item)
        
        # Test retrieving the item
        retrieved_item = registry.get_source_items("test_source_123", "pending")
        assert len(retrieved_item) == 1
        assert retrieved_item[0].source_item_id == "test_item_456"
        
        print("Source item tests passed!")
        
    finally:
        # Clean up
        if os.path.exists(db_path):
            os.unlink(db_path)

def test_chunker():
    """Test basic chunking functionality."""
    from core import Chunker
    
    # Test chunker with simple text
    chunker = Chunker(max_tokens=100)
    
    # Simple test text that's longer than max tokens to create multiple chunks
    test_text = "This is a sample text.\n\nThis is another paragraph.\n\n" * 50  # Create text that will definitely exceed chunk size
    
    chunks = chunker.chunk_text(test_text, section="Test Section")
    
    # Validate that we got some chunks back
    assert len(chunks) > 0
    print(f"Chunked text into {len(chunks)} chunks")
    
    # The first chunk should be within the token limit
    if chunks:
        first_chunk = chunks[0]
        assert 'chunk_text' in first_chunk
        assert 'token_estimate' in first_chunk
        print("Chunker tests passed!")

class DummyParser(BaseParser):
    """Test parser implementation."""
    
    def __init__(self):
        super().__init__("dummy_parser")
    
    def parse(self, source_item):
        """Simple parser that returns static content."""
        return ("This is test parsed content for the source item.", 
                {"test_metadata": "value"})

def test_parser():
    """Test parser functionality."""
    from core import DocumentManager
    
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
        db_path = tmp.name
    
    try:
        registry = IngestionRegistry(db_path=db_path)
        parser = DummyParser()
        
        # Create a source item for testing
        item = SourceItem(
            source_item_id="test_parser_789",
            source_id="test_source_123",
            uri="/test/file.txt",
            display_uri="file.txt",
            mime_type="text/plain",
            size_bytes=1024,
            mtime="2023-01-01T00:00:00Z",
            content_hash="test_hash_789",
            status="pending"
        )
        
        registry.add_source_item(item)
        
        # Test parsing
        text, metadata = parser.parse(item)
        assert text is not None
        assert isinstance(metadata, dict)
        print("Parser tests passed!")
        
    finally:
        # Clean up
        if os.path.exists(db_path):
            os.unlink(db_path)

if __name__ == "__main__":
    print("Running warlock_ingester tests...")
    
    try:
        test_registry_initialization()
        test_source_item()
        test_chunker()
        test_parser()
        print("All tests passed!")
    except Exception as e:
        print(f"Test failed: {e}")
        sys.exit(1)
