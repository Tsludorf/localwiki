#!/usr/bin/env python3
"""
Test file for warlock_ingester components.
"""

import os
import sys
import tempfile
import shutil
from pathlib import Path
import importlib.util
import types

# Add project root and src to Python path for direct execution
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_ROOT = PROJECT_ROOT / "src"
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(SRC_ROOT))

from core import (
    IngestionRegistry,
    Source,
    SourceItem,
    BaseParser,
    CanonicalDocument,
    VectorPoint,
    DEFAULT_EMBEDDER,
    DEFAULT_CHUNK_TOKENS,
    DEFAULT_CHUNK_OVERLAP,
    detect_source_type_and_mime,
    _build_embedding_text,
    resolve_collection_name,
    resolve_model_ingestion_config,
)
from parsers import ParserFactory
# Optional in non-test-runtime environments where pytest is unavailable.
try:  # pragma: no cover - best effort import shim
    import pytest  # type: ignore
except Exception:  # pragma: no cover
    pytest = None  # type: ignore


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

    pdf_type, pdf_mime = detect_source_type_and_mime(Path("/tmp/report.pdf"))
    assert pdf_type == "pdf"
    assert pdf_mime == "application/pdf"


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


def test_parser_factory_pdf_parser_selection():
    item = SourceItem(
        source_item_id="item_pdf_parser_factory",
        source_id="source_pdf_parser_factory",
        uri="/tmp/report.pdf",
        display_uri="report.pdf",
        mime_type="application/pdf",
        size_bytes=1,
        mtime="0",
        content_hash="",
        status="pending",
    )
    parser = ParserFactory.create_parser_for_item(item)
    assert getattr(parser, "parser_name", "") == "pdf"


def test_build_embedding_text_includes_context():
    chunk_text = "A short passage about a topic."
    source_item = SourceItem(
        source_item_id="item_ctx",
        source_id="source_ctx",
        uri="/tmp/folder/notes.txt",
        display_uri="folder/notes.txt",
        mime_type="text/plain",
        size_bytes=10,
        mtime="0",
        content_hash="",
        status="pending",
    )
    source = Source(
        source_id="source_ctx",
        source_type="text",
        root_uri="/tmp/folder",
        display_name="notes",
        added_at="0",
        settings_json="{}",
    )
    doc = CanonicalDocument(
        doc_id="doc_ctx",
        source_item_id="item_ctx",
        title="folder/notes.txt",
        parser="text",
        parser_version="1.0.0",
        version_hash="1.0.0",
        text_hash="x",
        metadata_json="{}",
        created_at="0",
        updated_at="0",
    )

    embedded = _build_embedding_text(chunk_text, {}, doc, source_item, source)
    assert "Title: folder/notes.txt" in embedded
    assert "Source: folder/notes.txt" in embedded
    assert "Source Type: text" in embedded
    assert embedded.endswith(chunk_text)


def test_resolve_collection_name_from_embedder_profile():
    assert resolve_collection_name(embedder="all-minilm:latest") == "local_wiki_384"
    assert resolve_collection_name(embedder="embeddinggemma") == "local_wiki_768"
    assert resolve_collection_name(embedder="bge-m3:latest") == "local_wiki_1024"


def test_resolve_model_ingestion_config_profile_and_overrides():
    profile_cfg = resolve_model_ingestion_config(embedder="all-minilm:latest")
    assert profile_cfg["chunk_tokens"] == 128
    assert profile_cfg["chunk_overlap"] == 32
    assert profile_cfg["embed_batch_size"] == 64
    assert profile_cfg["qdrant_batch_size"] == 256
    assert profile_cfg["max_chunk_chars"] == 512

    override_cfg = resolve_model_ingestion_config(
        embedder="all-minilm:latest",
        chunk_tokens=256,
        chunk_overlap=48,
        embed_batch_size=16,
        qdrant_batch_size=64,
        max_chunk_chars=1024,
    )
    assert override_cfg["chunk_tokens"] == 256
    assert override_cfg["chunk_overlap"] == 48
    assert override_cfg["embed_batch_size"] == 16
    assert override_cfg["qdrant_batch_size"] == 64
    assert override_cfg["max_chunk_chars"] == 1024


def test_default_profile_alignment_is_bge_m3():
    cfg = resolve_model_ingestion_config(embedder=DEFAULT_EMBEDDER)
    assert DEFAULT_EMBEDDER == "bge-m3:latest"
    assert cfg["dimensions"] == 1024
    assert cfg["chunk_tokens"] == DEFAULT_CHUNK_TOKENS
    assert cfg["chunk_overlap"] == DEFAULT_CHUNK_OVERLAP

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
    chunker = Chunker(max_tokens=100, overlap_tokens=50)
    
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


def test_registry_vector_status_batch_flow():
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp:
        db_path = tmp.name

    try:
        registry = IngestionRegistry(db_path=db_path)
        points = [
            VectorPoint(
                point_id="point_1",
                chunk_id="chunk_1",
                collection_name="local_wiki_1024",
                alias_name="",
                embedding_model="bge-m3:latest",
                embedding_dim=1024,
                vector_hash="h1",
                status="pending",
            ),
            VectorPoint(
                point_id="point_2",
                chunk_id="chunk_2",
                collection_name="local_wiki_1024",
                alias_name="",
                embedding_model="bge-m3:latest",
                embedding_dim=1024,
                vector_hash="h2",
                status="pending",
            ),
        ]
        registry.add_vector_points(points)

        pending_chunk_ids = registry.existing_vector_chunk_ids(
            ["chunk_1", "chunk_2"],
            "local_wiki_1024",
            statuses=["pending"],
        )
        assert pending_chunk_ids == {"chunk_1", "chunk_2"}

        registry.update_vector_point_statuses(["point_1", "point_2"], "confirmed")
        confirmed_chunk_ids = registry.existing_vector_chunk_ids(
            ["chunk_1", "chunk_2"],
            "local_wiki_1024",
            statuses=["confirmed"],
        )
        assert confirmed_chunk_ids == {"chunk_1", "chunk_2"}
    finally:
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


def _load_imager_app_module_for_tests():
    try:
        import flask  # type: ignore
    except Exception:
        fake_flask = types.ModuleType("flask")

        class _FakeFlask:
            def __init__(self, *args, **kwargs):
                self.config = {}

            def route(self, *args, **kwargs):  # noqa: ARG002
                def decorator(func):
                    return func

                return decorator

        def _fake_jsonify(*args, **kwargs):
            if args and len(args) == 1 and not kwargs:
                return args[0]
            if args and len(args) > 1 and not kwargs:
                return {"payload": list(args)}
            return kwargs

        def _fake_render_template(*args, **kwargs):  # noqa: ARG001
            return ""

        fake_flask.Flask = _FakeFlask
        fake_flask.jsonify = _fake_jsonify
        fake_flask.render_template = _fake_render_template
        fake_flask.request = types.SimpleNamespace()
        sys.modules["flask"] = fake_flask

    spec = importlib.util.spec_from_file_location("warlock_ui_app", PROJECT_ROOT / "ui" / "app.py")
    assert spec is not None
    assert spec.loader is not None
    if "warlock_ui_app" in sys.modules:
        module = sys.modules["warlock_ui_app"]
    else:
        module = importlib.util.module_from_spec(spec)
        sys.modules["warlock_ui_app"] = module
        spec.loader.exec_module(module)
    return module


def test_normalize_imager_target_value_parses_embedded_array_with_repaired_urls():
    app_module = _load_imager_app_module_for_tests()
    raw = 'https://[   \\\"https:/www.instagram.com/p/DYegkieOUZ4/\\\",   \\\"https:/www.instagram.com/p/DY3Xxvqs4Bk/\\\",   \\\"https:/www.instagram.com/p/DZI5bD-N36f/\\\",   \\\"https:/www.instagram.com/p/DZJORlNst3W/\\\" ]'
    normalized, error = app_module._normalize_imager_target_value(raw)

    assert error is None
    assert normalized is not None
    assert normalized == [
        "https://www.instagram.com/p/DYegkieOUZ4/",
        "https://www.instagram.com/p/DY3Xxvqs4Bk/",
        "https://www.instagram.com/p/DZI5bD-N36f/",
        "https://www.instagram.com/p/DZJORlNst3W/",
    ]


def test_normalize_imager_target_value_parses_single_quoted_array_and_trailing_commas():
    app_module = _load_imager_app_module_for_tests()
    raw = "['https:/www.instagram.com/p/one/', 'https:/www.instagram.com/p/two/',]"
    normalized, error = app_module._normalize_imager_target_value(raw)

    assert error is None
    assert normalized is not None
    assert normalized == ["https://www.instagram.com/p/one/", "https://www.instagram.com/p/two/"]


def test_resolve_imager_targets_report_target_index_for_missing_target():
    app_module = _load_imager_app_module_for_tests()

    with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as temp_target:
        temp_target.write(b"ready")
        target_path = temp_target.name

    try:
        resolved, resolve_error, status_code = app_module._resolve_imager_targets([target_path, "missing-target", target_path])
        assert resolved is None
        assert resolve_error is not None
        assert resolve_error.get("error_code") == "IMAGER_TARGET_MISSING"
        assert resolve_error.get("target_index") == 2
        assert status_code == 400
    finally:
        if os.path.exists(target_path):
            os.unlink(target_path)


def test_resolve_imager_targets_parses_embedded_array_before_resolving():
    app_module = _load_imager_app_module_for_tests()

    with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as temp_target:
        temp_target.write(b"ready")
        target_path = temp_target.name

    try:
        raw = f'[ "{target_path}", "{target_path}" ]'
        resolved, resolve_error, status_code = app_module._resolve_imager_targets([raw])

        assert resolve_error is None
        assert status_code is None
        assert resolved == [target_path, target_path]
    finally:
        if os.path.exists(target_path):
            os.unlink(target_path)


def _read_imager_template():
    return (PROJECT_ROOT / "ui" / "templates" / "imager.html").read_text(encoding="utf-8")


def test_imager_template_contains_reels_controls():
    template = _read_imager_template()
    assert 'id="reelUsername"' in template
    assert 'id="reelPage"' in template
    assert 'id="reelPageSize"' in template
    assert 'id="reelMaxResults"' in template
    assert 'id="reelOrder"' in template
    assert 'id="reelUseCache"' in template
    assert 'id="discoverReelsBtn"' in template
    assert 'id="reelDiscoverOutput"' in template


def test_imager_status_payload_exposes_reels_discovery_settings():
    app_module = _load_imager_app_module_for_tests()
    payload = app_module._imager_status_payload()
    reel_settings = payload.get("reels_discovery")
    assert isinstance(reel_settings, dict)
    assert reel_settings.get("default_page") == 1
    assert reel_settings.get("default_page_size") == app_module.IMAGER_INSTAGRAM_REELS_PAGE_SIZE
    assert reel_settings.get("default_max_results") == app_module.IMAGER_INSTAGRAM_REELS_DEFAULT_MAX_RESULTS
    assert reel_settings.get("max_page_size") == app_module.IMAGER_INSTAGRAM_REELS_PAGE_SIZE
    assert reel_settings.get("max_results") == app_module.IMAGER_INSTAGRAM_REELS_MAX_RESULTS


def test_imager_route_includes_reels_controls_in_rendered_html():
    app_module = _load_imager_app_module_for_tests()
    app = getattr(app_module, "app", None)
    test_client = getattr(app, "test_client", None)
    if not callable(test_client):
        return

    with test_client() as client:
        response = client.get("/imager")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert 'id="reelUsername"' in html
    assert 'id="discoverReelsBtn"' in html
    assert '/api/imager/reels/discover' in html


def test_ui_navigation_includes_imager_entry_on_landing_and_dashboard():
    app_module = _load_imager_app_module_for_tests()
    app = getattr(app_module, "app", None)
    test_client = getattr(app, "test_client", None)
    if not callable(test_client):
        return

    with test_client() as client:
        landing_html = client.get("/").get_data(as_text=True)
        dashboard_html = client.get("/dashboard").get_data(as_text=True)

    assert 'href="/imager"' in landing_html
    assert 'Open Imager Webscraper' in landing_html
    assert 'href="/imager"' in dashboard_html
    assert 'Open Imager Webscraper' in dashboard_html


def _extract_json_response(response):
    status_code = 200
    payload = response
    if isinstance(response, tuple):
        payload = response[0]
        if len(response) > 1 and response[1] is not None:
            status_code = int(response[1])
    elif hasattr(payload, "status_code"):
        status_code = int(getattr(payload, "status_code") or 200)

    if hasattr(payload, "get_json"):
        json_payload = payload.get_json(silent=True)
        if json_payload is not None:
            payload = json_payload

    return payload, status_code


def _run_imager_reels_discover_with_body(app_module, body):
    app = getattr(app_module, "app", None)
    test_request_context = getattr(app, "test_request_context", None)
    if callable(test_request_context):
        with test_request_context("/api/imager/reels/discover", method="POST", json=body):
            return app_module.api_imager_reels_discover()

    original_request = app_module.request
    app_module.request = types.SimpleNamespace(get_json=lambda **_: body)
    try:
        return app_module.api_imager_reels_discover()
    finally:
        app_module.request = original_request


def test_coerce_int_range_bounds_and_types():
    app_module = _load_imager_app_module_for_tests()

    value, error = app_module._coerce_int_range("12", field="page", minimum=1, maximum=24, default=12)
    assert value == 12
    assert error is None

    value, error = app_module._coerce_int_range("99", field="page", minimum=1, maximum=24, default=12)
    assert value == 12
    assert error == "page must be at most 24."

    value, error = app_module._coerce_int_range("0", field="page", minimum=1, maximum=24, default=12)
    assert value == 12
    assert error == "page must be at least 1."

    value, error = app_module._coerce_int_range("abc", field="page", minimum=1, maximum=24, default=12)
    assert value == 12
    assert error == "page must be an integer."


def test_coerce_bool_values_and_defaults():
    app_module = _load_imager_app_module_for_tests()

    value, error = app_module._coerce_bool("yes", field="use_cache", default=True)
    assert value is True
    assert error is None

    value, error = app_module._coerce_bool(0, field="use_cache", default=True)
    assert value is False
    assert error is None

    value, error = app_module._coerce_bool("n", field="use_cache", default=True)
    assert value is False
    assert error is None

    value, error = app_module._coerce_bool("maybe", field="use_cache", default=True)
    assert value is True
    assert error == "use_cache must be a boolean."


def test_discover_instagram_reels_with_ytdlp_short_circuit_for_invalid_range():
    app_module = _load_imager_app_module_for_tests()

    urls, discovery_error = app_module._discover_instagram_reels_with_ytdlp(
        "demo", start=10, end=2, use_cache=False, order="newest"
    )
    assert urls == []
    assert discovery_error is None


def test_api_imager_reels_discover_requires_json_object():
    app_module = _load_imager_app_module_for_tests()

    response, status_code = _extract_json_response(_run_imager_reels_discover_with_body(app_module, body="not-an-object"))
    assert status_code == 400
    assert response.get("error_code") == "IMAGER_INVALID_BODY"


def test_api_imager_reels_discover_uses_cache_when_available():
    app_module = _load_imager_app_module_for_tests()

    original_load_cache = app_module._load_imager_reels_cache
    original_discover = app_module._discover_instagram_reels_with_ytdlp

    discovered_calls = {"count": 0}

    def fake_load_imager_reels_cache(_cache_path):
        return [
            "https://www.instagram.com/p/abc1/",
            "https://www.instagram.com/p/abc2/",
        ]

    def fake_discover(*_args, **_kwargs):
        discovered_calls["count"] += 1
        return None, {
            "ok": False,
            "error": "should not be called",
            "error_code": "IMAGER_REELS_DISCOVERY_FAILED",
            "status_code": 502,
        }

    app_module._load_imager_reels_cache = fake_load_imager_reels_cache
    app_module._discover_instagram_reels_with_ytdlp = fake_discover

    try:
        response, status_code = _extract_json_response(
            _run_imager_reels_discover_with_body(
                app_module,
                {
                    "username": "example_user",
                    "page": 1,
                    "page_size": 12,
                    "max_results": 50,
                    "use_cache": True,
                    "order": "newest",
                },
            )
        )
        assert status_code == 200
        assert isinstance(response, list)
        assert response == [
            "https://www.instagram.com/p/abc1/",
            "https://www.instagram.com/p/abc2/",
        ]
        assert discovered_calls["count"] == 0
    finally:
        app_module._load_imager_reels_cache = original_load_cache
        app_module._discover_instagram_reels_with_ytdlp = original_discover


def test_api_imager_reels_discover_discovers_and_saves_on_cache_miss():
    app_module = _load_imager_app_module_for_tests()

    original_load_cache = app_module._load_imager_reels_cache
    original_discover = app_module._discover_instagram_reels_with_ytdlp
    original_save_cache = app_module._save_imager_reels_cache

    calls = {"load": False, "discover": 0, "saved": None}

    def fake_load_imager_reels_cache(_cache_path):
        calls["load"] = True
        return None

    def fake_discover_instagram_reels_with_ytdlp(
        username, start, end, use_cache=False, order="newest"
    ):  # noqa: ARG001
        calls["discover"] += 1
        return [
            "https://www.instagram.com/p/discovered-1/",
            "https://www.instagram.com/p/discovered-2/",
        ], None

    def fake_save_imager_reels_cache(cache_path, urls, *, username, page, page_size, max_results, order):  # noqa: ARG001
        calls["saved"] = {
            "cache_path": cache_path,
            "urls": urls,
            "username": username,
            "page": page,
            "page_size": page_size,
            "max_results": max_results,
            "order": order,
        }

    app_module._load_imager_reels_cache = fake_load_imager_reels_cache
    app_module._discover_instagram_reels_with_ytdlp = fake_discover_instagram_reels_with_ytdlp
    app_module._save_imager_reels_cache = fake_save_imager_reels_cache

    try:
        response, status_code = _extract_json_response(
            _run_imager_reels_discover_with_body(
                app_module,
                {
                    "username": "example_user",
                    "page": 2,
                    "page_size": 5,
                    "max_results": 50,
                    "use_cache": True,
                    "order": "oldest",
                },
            )
        )
        assert status_code == 200
        assert response == [
            "https://www.instagram.com/p/discovered-1/",
            "https://www.instagram.com/p/discovered-2/",
        ]
        assert calls["load"] is True
        assert calls["discover"] == 1
        assert calls["saved"]["username"] == "example_user"
        assert calls["saved"]["page"] == 2
        assert calls["saved"]["page_size"] == 5
        assert calls["saved"]["max_results"] == 50
        assert calls["saved"]["order"] == "oldest"
        assert calls["saved"]["urls"] == response
    finally:
        app_module._load_imager_reels_cache = original_load_cache
        app_module._discover_instagram_reels_with_ytdlp = original_discover
        app_module._save_imager_reels_cache = original_save_cache


def test_api_imager_reels_discover_reports_upstream_errors():
    app_module = _load_imager_app_module_for_tests()

    original_load_cache = app_module._load_imager_reels_cache
    original_discover = app_module._discover_instagram_reels_with_ytdlp

    def fake_load_imager_reels_cache(_cache_path):
        return None

    def fake_discover_instagram_reels_with_ytdlp(
        _username, start, end, use_cache=False, order="newest"  # noqa: ARG001
    ):
        return None, {
            "ok": False,
            "error": "rate limited",
            "error_code": "IMAGER_REELS_RATE_LIMITED",
            "status_code": 429,
        }

    app_module._load_imager_reels_cache = fake_load_imager_reels_cache
    app_module._discover_instagram_reels_with_ytdlp = fake_discover_instagram_reels_with_ytdlp

    try:
        response, status_code = _extract_json_response(
            _run_imager_reels_discover_with_body(
                app_module,
                {
                    "username": "example_user",
                    "page": 1,
                    "page_size": 10,
                    "max_results": 50,
                    "use_cache": True,
                    "order": "newest",
                },
            )
        )
        assert status_code == 429
        assert response.get("error_code") == "IMAGER_REELS_RATE_LIMITED"
    finally:
        app_module._load_imager_reels_cache = original_load_cache
        app_module._discover_instagram_reels_with_ytdlp = original_discover


def test_api_imager_reels_discover_empty_page_returns_cached_empty_and_saves():
    app_module = _load_imager_app_module_for_tests()

    original_load_cache = app_module._load_imager_reels_cache
    original_discover = app_module._discover_instagram_reels_with_ytdlp
    original_save_cache = app_module._save_imager_reels_cache

    calls = {"load": False, "discover": 0, "saved": 0}

    def fake_load_imager_reels_cache(_cache_path):
        calls["load"] = True
        return None

    def fake_discover_instagram_reels_with_ytdlp(*_args, **_kwargs):
        calls["discover"] += 1
        return ["https://www.instagram.com/p/should-not-see"], None

    def fake_save_imager_reels_cache(_cache_path, _urls, **_kwargs):
        calls["saved"] += 1

    app_module._load_imager_reels_cache = fake_load_imager_reels_cache
    app_module._discover_instagram_reels_with_ytdlp = fake_discover_instagram_reels_with_ytdlp
    app_module._save_imager_reels_cache = fake_save_imager_reels_cache

    try:
        response, status_code = _extract_json_response(
            _run_imager_reels_discover_with_body(
                app_module,
                {
                    "username": "example_user",
                    "page": 2,
                    "page_size": 5,
                    "max_results": 1,
                    "use_cache": True,
                    "order": "newest",
                },
            )
        )
        assert status_code == 200
        assert response == []
        assert calls["load"] is True
        assert calls["discover"] == 0
        assert calls["saved"] == 1
    finally:
        app_module._load_imager_reels_cache = original_load_cache
        app_module._discover_instagram_reels_with_ytdlp = original_discover
        app_module._save_imager_reels_cache = original_save_cache
