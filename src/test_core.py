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
import time

# Add project root and src to Python path for direct execution
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_ROOT = PROJECT_ROOT / "src"
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(SRC_ROOT))

from management_quotes_dsn_utils import is_management_quotes_placeholder_dsn
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
        fake_request = types.SimpleNamespace(
            get_json=lambda **_: {},
            headers={},
        )
        fake_flask.request = fake_request
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


def _read_dashboard_template():
    return (PROJECT_ROOT / "ui" / "templates" / "index.html").read_text(encoding="utf-8")


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


def test_dashboard_template_contains_management_quotes_ui():
    template = _read_dashboard_template()
    assert 'id="managementQuotesTabStatus"' in template
    assert 'id="mqSyncTickerId"' in template
    assert 'id="mqExtractTickerId"' in template
    assert 'id="mqCompareTickerId"' in template
    assert 'id="managementQuotesCommand"' in template
    assert 'id="mqFmpConfigPath"' in template
    assert 'id="mqFmpConfigUpload"' in template
    assert 'id="mqFmpMessage"' in template
    assert 'id="mqFmpConfigClearBtn"' in template


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


def test_ui_navigation_includes_imager_entry_on_landing_only():
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
    assert 'href="/imager"' not in dashboard_html
    assert 'Open Imager Webscraper' not in dashboard_html


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


def _run_management_quotes_route_with_body(app_module, route_name, body, headers=None, disable_token_gate: bool = True):
    if headers is None:
        headers = {}

    restored_token = None
    if disable_token_gate and not headers:
        restored_token = getattr(app_module, "MANAGEMENT_QUOTES_API_TOKEN", "")
        app_module.MANAGEMENT_QUOTES_API_TOKEN = ""

    app = getattr(app_module, "app", None)
    test_request_context = getattr(app, "test_request_context", None)
    if callable(test_request_context):
        try:
            with test_request_context(
                f"/api/management-quotes/{route_name}",
                method="POST",
                json=body,
                headers=headers,
            ):
                return getattr(app_module, f"api_management_quotes_{route_name}")()
        finally:
            if restored_token is not None:
                app_module.MANAGEMENT_QUOTES_API_TOKEN = restored_token

    original_request = app_module.request
    app_module.request = types.SimpleNamespace(
        get_json=lambda **_: body,
        headers=(dict(getattr(original_request, "headers", {})) if headers is None else dict(headers)),
    )
    try:
        return getattr(app_module, f"api_management_quotes_{route_name}")()
    finally:
        app_module.request = original_request
        if restored_token is not None:
            app_module.MANAGEMENT_QUOTES_API_TOKEN = restored_token


def _run_management_quotes_job_status_route(app_module, job_id, disable_token_gate: bool = True):
    restored_token = None
    if disable_token_gate:
        restored_token = getattr(app_module, "MANAGEMENT_QUOTES_API_TOKEN", "")
        app_module.MANAGEMENT_QUOTES_API_TOKEN = ""

    app = getattr(app_module, "app", None)
    test_request_context = getattr(app, "test_request_context", None)
    if callable(test_request_context):
        try:
            with test_request_context(f"/api/management-quotes/jobs/{job_id}", method="GET"):
                return app_module.api_management_quotes_job_status(job_id)
        finally:
            if restored_token is not None:
                app_module.MANAGEMENT_QUOTES_API_TOKEN = restored_token

    original_request = app_module.request
    app_module.request = types.SimpleNamespace(get_json=lambda **_: None)
    try:
        return app_module.api_management_quotes_job_status(job_id)
    finally:
        app_module.request = original_request
        if restored_token is not None:
            app_module.MANAGEMENT_QUOTES_API_TOKEN = restored_token


def _wait_for_management_quotes_call(calls, timeout_seconds: float = 1.0):
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if calls.get("args") is not None:
            return True
        time.sleep(0.01)
    return False


def _clear_management_quotes_jobs_for_tests(app_module):
    jobs = getattr(app_module, "MANAGEMENT_QUOTES_JOBS", None)
    if isinstance(jobs, dict):
        jobs.clear()


def test_api_management_quotes_sync_validates_ticker_id():
    app_module = _load_imager_app_module_for_tests()

    response, status_code = _extract_json_response(_run_management_quotes_route_with_body(app_module, "sync", {}))
    assert status_code == 400
    assert response.get("ok") is False
    assert response.get("error_code") == "MANAGEMENT_QUOTES_TICKER_ID_REQUIRED"

    response, status_code = _extract_json_response(
        _run_management_quotes_route_with_body(app_module, "sync", {"ticker_id": "   ", "years": 3})
    )
    assert status_code == 400
    assert response.get("error_code") == "MANAGEMENT_QUOTES_TICKER_ID_REQUIRED"

    response, status_code = _extract_json_response(
        _run_management_quotes_route_with_body(app_module, "sync", {"ticker_id": 12, "years": 999})
    )
    assert status_code == 400
    assert response.get("error_code") == "MANAGEMENT_QUOTES_YEARS_INVALID"


def test_api_management_quotes_sync_accepts_equity_ticker_with_space():
    app_module = _load_imager_app_module_for_tests()

    calls = {}

    def fake_run(args):
        calls["args"] = list(args)
        return {
            "ok": True,
            "exit_code": 0,
            "stdout": "ok",
            "stderr": "",
            "latency_ms": 1,
            "ran_at": "2020-01-01T00:00:00Z",
        }

    original_runner = app_module._run_management_quotes_command
    app_module._run_management_quotes_command = fake_run

    try:
        response, status_code = _extract_json_response(
            _run_management_quotes_route_with_body(
                app_module,
                "sync",
                {"ticker_id": "2124 JP Equity", "years": 3},
            )
        )
    finally:
        app_module._run_management_quotes_command = original_runner

    assert status_code == 202
    assert response.get("ok") is True
    assert calls["args"][2] == "2124 JP Equity"


def test_api_management_quotes_extract_validates_provider_and_fields():
    app_module = _load_imager_app_module_for_tests()

    response, status_code = _extract_json_response(
        _run_management_quotes_route_with_body(app_module, "extract", {"ticker_id": 12, "provider": "openai"})
    )
    assert status_code == 400
    assert response.get("error_code") == "MANAGEMENT_QUOTES_PROVIDER_INVALID"

    response, status_code = _extract_json_response(
        _run_management_quotes_route_with_body(app_module, "extract", {"ticker_id": 12, "sleep": -1})
    )
    assert status_code == 400
    assert response.get("error_code") == "MANAGEMENT_QUOTES_SLEEP_INVALID"

    response, status_code = _extract_json_response(
        _run_management_quotes_route_with_body(app_module, "extract", {"ticker_id": 12, "limit": 0})
    )
    assert status_code == 400
    assert response.get("error_code") == "MANAGEMENT_QUOTES_LIMIT_INVALID"


def test_api_management_quotes_extract_uses_defaults_and_caps():
    app_module = _load_imager_app_module_for_tests()

    calls = {}

    def fake_run(args):
        calls["args"] = list(args)
        return {
            "ok": True,
            "exit_code": 0,
            "stdout": "ok",
            "stderr": "",
            "latency_ms": 1,
            "ran_at": "2020-01-01T00:00:00Z",
        }

    original_runner = app_module._run_management_quotes_command
    app_module._run_management_quotes_command = fake_run
    _clear_management_quotes_jobs_for_tests(app_module)

    try:
        response, status_code = _extract_json_response(
            _run_management_quotes_route_with_body(
                app_module,
                "extract",
                {
                    "ticker_id": 12,
                    "provider": "ollama",
                },
            )
        )
    finally:
        app_module._run_management_quotes_command = original_runner

    assert status_code == 202
    assert response.get("ok") is True
    assert response.get("command") == "extract"
    assert calls["args"] == [
        "extract",
        "--ticker-id",
        "12",
        "--provider",
        "ollama",
        "--sleep",
        "0.0",
        "--limit",
        "50",
    ]

    # Ensure explicit out-of-range limits are rejected.
    _clear_management_quotes_jobs_for_tests(app_module)
    response, status_code = _extract_json_response(
        _run_management_quotes_route_with_body(
            app_module,
            "extract",
            {
                "ticker_id": 12,
                "provider": "ollama",
                "limit": app_module.MANAGEMENT_QUOTES_EXTRACT_MAX_LIMIT + 1,
            },
        )
    )
    assert status_code == 400
    assert response.get("error_code") == "MANAGEMENT_QUOTES_LIMIT_INVALID"


def test_api_management_quotes_extract_blocks_schema_mutation_when_disabled():
    app_module = _load_imager_app_module_for_tests()
    original_flag = app_module.MANAGEMENT_QUOTES_ALLOW_SCHEMA_CHANGES
    app_module.MANAGEMENT_QUOTES_ALLOW_SCHEMA_CHANGES = False
    try:
        response, status_code = _extract_json_response(
            _run_management_quotes_route_with_body(
                app_module,
                "extract",
                {
                    "ticker_id": 12,
                    "provider": "ollama",
                    "create_table": True,
                },
            )
        )
    finally:
        app_module.MANAGEMENT_QUOTES_ALLOW_SCHEMA_CHANGES = original_flag
    assert status_code == 403
    assert response.get("error_code") == "MANAGEMENT_QUOTES_SCHEMA_CHANGES_DISABLED"


def test_api_management_quotes_endpoints_require_token_when_configured():
    app_module = _load_imager_app_module_for_tests()
    original_token = app_module.MANAGEMENT_QUOTES_API_TOKEN
    original_runner = app_module._run_management_quotes_command

    def fake_run(_args):
        return {
            "ok": True,
            "exit_code": 0,
            "stdout": "ok",
            "stderr": "",
            "latency_ms": 1,
            "ran_at": "2020-01-01T00:00:00Z",
        }

    app_module.MANAGEMENT_QUOTES_API_TOKEN = "secret-token"
    app_module._run_management_quotes_command = fake_run
    try:
        app_module.request.headers = {"X-Management-Quotes-Token": "wrong"}
        response, status_code = _extract_json_response(
            _run_management_quotes_route_with_body(
                app_module,
                "sync",
                {"ticker_id": 12},
                headers={"X-Management-Quotes-Token": "wrong"},
            )
        )
        assert status_code == 401
        assert response.get("error_code") == "MANAGEMENT_QUOTES_UNAUTHORIZED"

        response, status_code = _extract_json_response(
            _run_management_quotes_route_with_body(
                app_module,
                "sync",
                {"ticker_id": 12},
                headers={"X-Management-Quotes-Token": "secret-token"},
            )
        )
        assert status_code == 202
        assert response.get("ok") is True

        response, status_code = _extract_json_response(
            _run_management_quotes_route_with_body(
                app_module,
                "sync",
                {"ticker_id": 12},
                headers={"Authorization": "Bearer secret-token"},
            )
        )
        assert status_code == 202
        assert response.get("ok") is True
    finally:
        app_module.MANAGEMENT_QUOTES_API_TOKEN = original_token
        app_module._run_management_quotes_command = original_runner


def test_api_management_quotes_config_endpoints_require_token_when_configured():
    app_module = _load_imager_app_module_for_tests()
    original_token = app_module.MANAGEMENT_QUOTES_API_TOKEN
    original_state_path = app_module.MANAGEMENT_QUOTES_CREDENTIALS_STATE_PATH
    original_upload_dir = app_module.MANAGEMENT_QUOTES_UPLOAD_DIR
    original_upload_path = app_module.MANAGEMENT_QUOTES_UPLOAD_PATH

    app_module.MANAGEMENT_QUOTES_API_TOKEN = "secret-token"

    with tempfile.TemporaryDirectory(prefix="mq-fmp-auth-") as tmpdir:
        tmp_root = Path(tmpdir)
        source_path = tmp_root / "app-config.json"
        source_path.write_text('{"FMP_API_KEY":"from-file"}', encoding="utf-8")

        app_module.MANAGEMENT_QUOTES_CREDENTIALS_STATE_PATH = tmp_root / "state.json"
        app_module.MANAGEMENT_QUOTES_UPLOAD_DIR = tmp_root / "secrets"
        app_module.MANAGEMENT_QUOTES_UPLOAD_PATH = app_module.MANAGEMENT_QUOTES_UPLOAD_DIR / "app-config.json"

        try:
            wrong = {"X-Management-Quotes-Token": "wrong"}
            correct = {"X-Management-Quotes-Token": "secret-token"}

            response, status_code = _extract_json_response(
                _run_management_quotes_route_with_body(
                    app_module,
                    "config",
                    {"config_path": str(source_path)},
                    headers=wrong,
                )
            )
            assert status_code == 401
            assert response.get("error_code") == "MANAGEMENT_QUOTES_UNAUTHORIZED"

            response, status_code = _extract_json_response(
                _run_management_quotes_route_with_body(
                    app_module,
                    "config",
                    {"config_path": str(source_path)},
                    headers=correct,
                )
            )
            assert status_code == 200
            assert response.get("ok") is True

            response, status_code = _extract_json_response(
                _run_management_quotes_route_with_body(
                    app_module,
                    "upload",
                    {"filename": "app-config.json", "content": '{"api_key":"token-route"}'},
                    headers=wrong,
                )
            )
            assert status_code == 401
            assert response.get("error_code") == "MANAGEMENT_QUOTES_UNAUTHORIZED"

            response, status_code = _extract_json_response(
                _run_management_quotes_route_with_body(
                    app_module,
                    "clear",
                    {},
                    headers=wrong,
                )
            )
            assert status_code == 401
            assert response.get("error_code") == "MANAGEMENT_QUOTES_UNAUTHORIZED"

            response, status_code = _extract_json_response(
                _run_management_quotes_route_with_body(
                    app_module,
                    "upload",
                    {"filename": "app-config.json", "content": '{"api_key":"token-route"}'},
                    headers=correct,
                )
            )
            assert status_code == 200
            assert response.get("ok") is True

            response, status_code = _extract_json_response(
                _run_management_quotes_route_with_body(
                    app_module,
                    "clear",
                    {},
                    headers=correct,
                )
            )
            assert status_code == 200
            assert response.get("ok") is True
            assert response.get("configured") is False
        finally:
            app_module.MANAGEMENT_QUOTES_API_TOKEN = original_token
            app_module.MANAGEMENT_QUOTES_CREDENTIALS_STATE_PATH = original_state_path
            app_module.MANAGEMENT_QUOTES_UPLOAD_DIR = original_upload_dir
            app_module.MANAGEMENT_QUOTES_UPLOAD_PATH = original_upload_path


def test_api_management_quotes_credential_routes_manage_fmp_config():
    app_module = _load_imager_app_module_for_tests()

    with tempfile.TemporaryDirectory(prefix="mq-fmp-creds-") as tmpdir:
        tmp_root = Path(tmpdir)
        original_state_path = app_module.MANAGEMENT_QUOTES_CREDENTIALS_STATE_PATH
        original_upload_dir = app_module.MANAGEMENT_QUOTES_UPLOAD_DIR
        original_upload_path = app_module.MANAGEMENT_QUOTES_UPLOAD_PATH

        app_module.MANAGEMENT_QUOTES_CREDENTIALS_STATE_PATH = tmp_root / "management_quotes.json"
        app_module.MANAGEMENT_QUOTES_UPLOAD_DIR = tmp_root / "secrets"
        app_module.MANAGEMENT_QUOTES_UPLOAD_PATH = app_module.MANAGEMENT_QUOTES_UPLOAD_DIR / "app-config.json"

        source_path = tmp_root / "app-config.json"
        source_path.write_text('{"FMP_API_KEY":"test-key-from-path"}', encoding="utf-8")

        source_path_txt = tmp_root / "fmp_config.txt"
        source_path_txt.write_text(
            "# Database\n"
            "NEON_CONNECTION_STRING=postgresql://user:password@host/database\n"
            "# Financial Modeling Prep\n"
            "FMP_API_KEY=txt-file-key\n",
            encoding="utf-8",
        )

        try:
            response, status_code = _extract_json_response(
                _run_management_quotes_route_with_body(
                    app_module,
                    "config",
                    {"config_path": str(source_path)},
                )
            )
            assert status_code == 200
            assert response.get("ok") is True
            state = app_module._load_management_quotes_credentials_state()
            assert state.get("config_source") == "path"

            response, status_code = _extract_json_response(
                _run_management_quotes_route_with_body(
                    app_module,
                    "config",
                    {
                        "config_path": str(source_path_txt),
                    },
                )
            )
            assert status_code == 200
            assert response.get("ok") is True
            state = app_module._load_management_quotes_credentials_state()
            assert state.get("config_source") == "path"
            assert state.get("config_path") == str(source_path_txt)

            response, status_code = _extract_json_response(
                _run_management_quotes_route_with_body(
                    app_module,
                    "upload",
                    {"filename": "app-config.json", "content": '{"api_key":"uploaded-key"}'},
                )
            )
            assert status_code == 200
            assert response.get("ok") is True
            assert app_module.MANAGEMENT_QUOTES_UPLOAD_PATH.exists()
            state = app_module._load_management_quotes_credentials_state()
            assert state.get("config_source") == "uploaded_file"

            response, status_code = _extract_json_response(
                _run_management_quotes_route_with_body(app_module, "clear", {})
            )
            assert status_code == 200
            assert response.get("ok") is True
            state = app_module._load_management_quotes_credentials_state()
            assert state.get("config_source") == "not_set"

            response, status_code = _extract_json_response(
                _run_management_quotes_route_with_body(
                    app_module,
                    "upload",
                    {"filename": "bad.txt", "content": '{"api_key":"x"}'},
                )
            )
            assert status_code == 400
            assert response.get("message") == "Uploaded filename must end with .json."

            response, status_code = _extract_json_response(
                _run_management_quotes_route_with_body(
                    app_module,
                    "upload",
                    {"filename": "app-config.json", "content": "not-json"},
                )
            )
            assert status_code == 400
            assert response.get("message") == "Invalid Management Quotes configuration."
        finally:
            app_module.MANAGEMENT_QUOTES_CREDENTIALS_STATE_PATH = original_state_path
            app_module.MANAGEMENT_QUOTES_UPLOAD_DIR = original_upload_dir
            app_module.MANAGEMENT_QUOTES_UPLOAD_PATH = original_upload_path


def test_api_management_quotes_api_key_resolution_prefers_environment_then_state_path():
    app_module = _load_imager_app_module_for_tests()

    with tempfile.TemporaryDirectory(prefix="mq-fmp-key-") as tmpdir:
        tmp_root = Path(tmpdir)
        original_state_path = app_module.MANAGEMENT_QUOTES_CREDENTIALS_STATE_PATH
        state_path = tmp_root / "state.json"
        config_path = tmp_root / "app-config.json"

        app_module.MANAGEMENT_QUOTES_CREDENTIALS_STATE_PATH = state_path
        config_path.write_text('{"FMP_API_TOKEN":"state-key"}', encoding="utf-8")
        state = app_module._load_management_quotes_credentials_state()
        state["config_source"] = "path"
        state["config_path"] = str(config_path)
        app_module._save_management_quotes_credentials_state(state)

        original_fmp_api_key = os.environ.get("FMP_API_KEY")
        original_fmp_api_token = os.environ.get("FMP_API_TOKEN")

        try:
            os.environ["FMP_API_KEY"] = "env-key"
            assert app_module._resolve_management_quotes_api_key() == "env-key"

            if "FMP_API_KEY" in os.environ:
                del os.environ["FMP_API_KEY"]

            if "FMP_API_TOKEN" in os.environ:
                del os.environ["FMP_API_TOKEN"]

            assert app_module._resolve_management_quotes_api_key() == "state-key"
        finally:
            if original_fmp_api_key is None:
                os.environ.pop("FMP_API_KEY", None)
            else:
                os.environ["FMP_API_KEY"] = original_fmp_api_key

            if original_fmp_api_token is None:
                os.environ.pop("FMP_API_TOKEN", None)
            else:
                os.environ["FMP_API_TOKEN"] = original_fmp_api_token

            app_module.MANAGEMENT_QUOTES_CREDENTIALS_STATE_PATH = original_state_path


def test_api_management_quotes_api_key_resolution_reads_fmp_config_txt_path():
    app_module = _load_imager_app_module_for_tests()

    with tempfile.TemporaryDirectory(prefix="mq-fmp-text-") as tmpdir:
        tmp_root = Path(tmpdir)
        config_file = tmp_root / "fmp_config.txt"
        config_file.write_text(
            "# Database\n"
            "NEON_CONNECTION_STRING=postgresql://user:password@host/database\n"
            "\n"
            "# Financial Modeling Prep\n"
            "FMP_API_KEY=my-text-fmp-key\n"
            "\n"
            "# Optional\n"
            "OPENROUTER_API_KEY=or-key\n",
            encoding="utf-8",
        )

        original_fallback = app_module.MANAGEMENT_QUOTES_FALLBACK_CONFIG_PATH
        original_state_path = app_module.MANAGEMENT_QUOTES_CREDENTIALS_STATE_PATH
        app_module.MANAGEMENT_QUOTES_FALLBACK_CONFIG_PATH = config_file
        app_module.MANAGEMENT_QUOTES_CREDENTIALS_STATE_PATH = tmp_root / "state.json"

        try:
            assert app_module._resolve_management_quotes_api_key() == "my-text-fmp-key"
        finally:
            app_module.MANAGEMENT_QUOTES_FALLBACK_CONFIG_PATH = original_fallback
            app_module.MANAGEMENT_QUOTES_CREDENTIALS_STATE_PATH = original_state_path


def test_api_management_quotes_database_dsn_resolution_prefers_environment_then_state_path():
    app_module = _load_imager_app_module_for_tests()

    with tempfile.TemporaryDirectory(prefix="mq-dsn-") as tmpdir:
        tmp_root = Path(tmpdir)
        original_state_path = app_module.MANAGEMENT_QUOTES_CREDENTIALS_STATE_PATH
        state_path = tmp_root / "state.json"
        config_path = tmp_root / "app-config.json"

        app_module.MANAGEMENT_QUOTES_CREDENTIALS_STATE_PATH = state_path
        config_path.write_text('{"NEON_CONNECTION_STRING":"postgresql://state-user:state-pass@state-host/state-db"}', encoding="utf-8")
        state = app_module._load_management_quotes_credentials_state()
        state["config_source"] = "path"
        state["config_path"] = str(config_path)
        app_module._save_management_quotes_credentials_state(state)

        original_neon = os.environ.get("NEON_CONNECTION_STRING")
        original_database_url = os.environ.get("DATABASE_URL")

        try:
            os.environ["NEON_CONNECTION_STRING"] = "postgresql://env-user:env-pass@env-host/env-db"
            assert app_module._resolve_management_quotes_database_dsn() == "postgresql://env-user:env-pass@env-host/env-db"

            os.environ.pop("NEON_CONNECTION_STRING")
            os.environ.pop("DATABASE_URL", None)
            assert app_module._resolve_management_quotes_database_dsn() == "postgresql://state-user:state-pass@state-host/state-db"
        finally:
            if original_neon is None:
                os.environ.pop("NEON_CONNECTION_STRING", None)
            else:
                os.environ["NEON_CONNECTION_STRING"] = original_neon

            if original_database_url is None:
                os.environ.pop("DATABASE_URL", None)
            else:
                os.environ["DATABASE_URL"] = original_database_url

            app_module.MANAGEMENT_QUOTES_CREDENTIALS_STATE_PATH = original_state_path


def test_api_management_quotes_database_dsn_resolution_reads_fallback_config_txt_path():
    app_module = _load_imager_app_module_for_tests()

    with tempfile.TemporaryDirectory(prefix="mq-dsn-text-") as tmpdir:
        tmp_root = Path(tmpdir)
        config_file = tmp_root / "fmp_config.txt"
        config_file.write_text(
            "# Database\n"
            "NEON_CONNECTION_STRING=postgresql://fallback-user:fallback-pass@fallback-host/fallback-db\n"
            "FMP_API_KEY=txt-key\n",
            encoding="utf-8",
        )

        original_fallback = app_module.MANAGEMENT_QUOTES_FALLBACK_CONFIG_PATH
        original_state_path = app_module.MANAGEMENT_QUOTES_CREDENTIALS_STATE_PATH

        try:
            app_module.MANAGEMENT_QUOTES_FALLBACK_CONFIG_PATH = config_file
            app_module.MANAGEMENT_QUOTES_CREDENTIALS_STATE_PATH = tmp_root / "state.json"

            assert app_module._resolve_management_quotes_database_dsn() == (
                "postgresql://fallback-user:fallback-pass@fallback-host/fallback-db"
            )
        finally:
            app_module.MANAGEMENT_QUOTES_FALLBACK_CONFIG_PATH = original_fallback
            app_module.MANAGEMENT_QUOTES_CREDENTIALS_STATE_PATH = original_state_path


def test_run_management_quotes_command_includes_management_dsn_and_api_key_env():
    app_module = _load_imager_app_module_for_tests()

    calls = {}

    class _FakeResult:
        returncode = 0

    original_run = app_module.subprocess.run

    def fake_run(command, **kwargs):
        calls["command"] = list(command)
        calls["env"] = dict(kwargs.get("env", {}))
        return _FakeResult()

    original_neon = os.environ.get("NEON_CONNECTION_STRING")
    original_fmp_key = os.environ.get("FMP_API_KEY")

    try:
        os.environ["NEON_CONNECTION_STRING"] = "postgresql://env-user:env-pass@env-host/env-db"
        os.environ["FMP_API_KEY"] = "env-fmp-key"
        app_module.subprocess.run = fake_run

        result = app_module._run_management_quotes_command(["sync", "--ticker-id", "1234"])

        assert result.get("ok") is True
        assert calls["env"].get("NEON_CONNECTION_STRING") == "postgresql://env-user:env-pass@env-host/env-db"
        assert calls["env"].get("FMP_API_KEY") == "env-fmp-key"
        assert calls["command"][0] == str(app_module.LOCALWIKI_VENV_PYTHON)
    finally:
        app_module.subprocess.run = original_run
        if original_neon is None:
            os.environ.pop("NEON_CONNECTION_STRING", None)
        else:
            os.environ["NEON_CONNECTION_STRING"] = original_neon

        if original_fmp_key is None:
            os.environ.pop("FMP_API_KEY", None)
        else:
            os.environ["FMP_API_KEY"] = original_fmp_key


def test_run_management_quotes_command_surfaces_traceback_on_exception():
    app_module = _load_imager_app_module_for_tests()

    calls = {}

    def fake_run(command, **kwargs):
        calls["command"] = list(command)
        raise OSError("boom")

    original_run = app_module.subprocess.run
    try:
        app_module.subprocess.run = fake_run
        result = app_module._run_management_quotes_command(["sync", "--ticker-id", "1234"])

        assert result.get("ok") is False
        assert calls.get("command") == [str(app_module.LOCALWIKI_VENV_PYTHON), str(app_module.MANAGEMENT_QUOTES_CLI), "sync", "--ticker-id", "1234"]
        stderr_text = result.get("stderr", "")
        assert "OSError" in stderr_text
        assert "traceback" in stderr_text
        assert "error_type" in stderr_text
    finally:
        app_module.subprocess.run = original_run


def test_run_management_quotes_command_surfaces_timeout_context():
    app_module = _load_imager_app_module_for_tests()

    def fake_run(command, **kwargs):
        raise subprocess.TimeoutExpired(cmd=command, timeout=7)

    original_run = app_module.subprocess.run
    try:
        app_module.subprocess.run = fake_run
        result = app_module._run_management_quotes_command(["extract", "--ticker-id", "1234"], timeout=7)

        assert result.get("ok") is False
        stderr_text = result.get("stderr", "")
        assert "Management Quotes command timed out." in stderr_text
        assert "\"timeout_seconds\": 7" in stderr_text
    finally:
        app_module.subprocess.run = original_run


def test_run_management_quotes_command_rejects_placeholder_database_host():
    app_module = _load_imager_app_module_for_tests()

    with tempfile.TemporaryDirectory(prefix="mq-placeholder-") as tmpdir:
        tmp_root = Path(tmpdir)
        fallback = tmp_root / "fmp_config.txt"
        fallback.write_text("NEON_CONNECTION_STRING=postgresql://user:password@host/database\n", encoding="utf-8")

        original_fallback = app_module.MANAGEMENT_QUOTES_FALLBACK_CONFIG_PATH
        original_state = app_module.MANAGEMENT_QUOTES_CREDENTIALS_STATE_PATH
        original_neon = os.environ.get("NEON_CONNECTION_STRING")
        original_database_url = os.environ.get("DATABASE_URL")

        app_module.MANAGEMENT_QUOTES_FALLBACK_CONFIG_PATH = fallback
        app_module.MANAGEMENT_QUOTES_CREDENTIALS_STATE_PATH = tmp_root / "state.json"
        os.environ.pop("NEON_CONNECTION_STRING", None)
        os.environ.pop("DATABASE_URL", None)

        calls = {"run_called": False}
        original_run = app_module.subprocess.run

        def fake_run(*_args, **_kwargs):
            calls["run_called"] = True

        app_module.subprocess.run = fake_run

        try:
            result = app_module._run_management_quotes_command(["sync", "--ticker-id", "1234"])

            assert result.get("ok") is False
            assert result.get("exit_code") is None
            assert calls["run_called"] is False
            stderr_text = result.get("stderr", "")
            assert "placeholder host 'host'" in stderr_text
            assert "Refusing unresolved DB host 'host'" in stderr_text
        finally:
            app_module.subprocess.run = original_run
            app_module.MANAGEMENT_QUOTES_FALLBACK_CONFIG_PATH = original_fallback
            app_module.MANAGEMENT_QUOTES_CREDENTIALS_STATE_PATH = original_state
            if original_neon is None:
                os.environ.pop("NEON_CONNECTION_STRING", None)
            else:
                os.environ["NEON_CONNECTION_STRING"] = original_neon
            if original_database_url is None:
                os.environ.pop("DATABASE_URL", None)
            else:
                os.environ["DATABASE_URL"] = original_database_url


def test_is_management_quotes_placeholder_dsn_detection():
    assert is_management_quotes_placeholder_dsn("postgresql://user:password@host/database")
    assert is_management_quotes_placeholder_dsn("postgresql+psycopg://u:p@HOST/db")
    assert not is_management_quotes_placeholder_dsn("postgresql://user:password@db.internal/database")
    assert not is_management_quotes_placeholder_dsn("sqlite:///tmp/data.db")
    assert not is_management_quotes_placeholder_dsn(None)


def test_api_management_quotes_compare_rejects_missing_prompt_version():
    app_module = _load_imager_app_module_for_tests()

    response, status_code = _extract_json_response(
        _run_management_quotes_route_with_body(app_module, "compare", {"ticker_id": 12, "local_prompt_version": "", "cloud_prompt_version": ""})
    )
    assert status_code == 400
    assert response.get("error_code") == "MANAGEMENT_QUOTES_PROMPT_VERSION_REQUIRED"


def test_api_management_quotes_extract_uses_subprocess_helper(monkeypatch):
    app_module = _load_imager_app_module_for_tests()
    _clear_management_quotes_jobs_for_tests(app_module)

    calls = {}

    def fake_run(args):
        calls["args"] = list(args)
        return {
            "ok": True,
            "exit_code": 0,
            "stdout": "done",
            "stderr": "",
            "latency_ms": 1,
            "ran_at": "2020-01-01T00:00:00Z",
        }

    original_runner = app_module._run_management_quotes_command
    app_module._run_management_quotes_command = fake_run

    try:
        response, status_code = _extract_json_response(
            _run_management_quotes_route_with_body(
                app_module,
                "extract",
                {
                    "ticker_id": 12,
                    "provider": "ollama",
                    "model": "qwen3.5:30b",
                    "sleep": 0.1,
                    "limit": 2,
                },
            )
    )
    finally:
        app_module._run_management_quotes_command = original_runner

    assert status_code == 202
    assert response.get("ok") is True
    assert response.get("command") == "extract"
    assert isinstance(response.get("job_id"), str)
    assert response.get("status") == "queued"
    assert _wait_for_management_quotes_call(calls)
    assert calls.get("args") == [
        "extract",
        "--ticker-id",
        "12",
        "--provider",
        "ollama",
        "--model",
        "qwen3.5:30b",
        "--sleep",
        "0.1",
        "--limit",
        "2",
    ]


def test_api_management_quotes_sync_uses_subprocess_helper(monkeypatch):
    app_module = _load_imager_app_module_for_tests()
    _clear_management_quotes_jobs_for_tests(app_module)

    calls = {}

    def fake_run(args):
        calls["args"] = list(args)
        return {
            "ok": True,
            "exit_code": 0,
            "stdout": "done",
            "stderr": "",
            "latency_ms": 2,
            "ran_at": "2020-01-01T00:00:00Z",
        }

    original_runner = app_module._run_management_quotes_command
    app_module._run_management_quotes_command = fake_run

    try:
        response, status_code = _extract_json_response(
            _run_management_quotes_route_with_body(
                app_module,
                "sync",
                {
                    "ticker_id": 12,
                    "years": 4,
                    "sleep_seconds": 1.25,
                    "dry_run": True,
                },
            )
        )
    finally:
        app_module._run_management_quotes_command = original_runner

    assert status_code == 202
    assert response.get("ok") is True
    assert response.get("command") == "sync"
    assert isinstance(response.get("job_id"), str)
    assert response.get("status") == "queued"
    assert _wait_for_management_quotes_call(calls)
    assert calls.get("args") == [
        "sync",
        "--ticker-id",
        "12",
        "--years",
        "4",
        "--sleep",
        "1.25",
        "--dry-run",
    ]


def test_api_management_quotes_compare_uses_subprocess_helper_and_defaults(monkeypatch):
    app_module = _load_imager_app_module_for_tests()
    _clear_management_quotes_jobs_for_tests(app_module)

    calls = {}

    def fake_run(args):
        calls["args"] = list(args)
        return {
            "ok": True,
            "exit_code": 0,
            "stdout": "done",
            "stderr": "",
            "latency_ms": 2,
            "ran_at": "2020-01-01T00:00:00Z",
        }

    original_runner = app_module._run_management_quotes_command
    app_module._run_management_quotes_command = fake_run

    try:
        response, status_code = _extract_json_response(
            _run_management_quotes_route_with_body(
                app_module,
                "compare",
                {
                    "ticker_id": 12,
                    "no_csv": True,
                },
            )
        )
    finally:
        app_module._run_management_quotes_command = original_runner

    assert status_code == 202
    assert response.get("ok") is True
    assert response.get("command") == "compare"
    assert isinstance(response.get("job_id"), str)
    assert response.get("status") == "queued"
    assert _wait_for_management_quotes_call(calls)
    assert calls.get("args") == [
        "compare",
        "--ticker-id",
        "12",
        "--local-prompt-version",
        "management_quotes_v1_ollama",
        "--cloud-prompt-version",
        "management_quotes_v1",
        "--no-csv",
    ]


def test_api_management_quotes_job_status_reports_result(monkeypatch):
    app_module = _load_imager_app_module_for_tests()
    _clear_management_quotes_jobs_for_tests(app_module)

    def fake_run(args):
        return {
            "ok": True,
            "exit_code": 0,
            "stdout": "done",
            "stderr": "",
            "latency_ms": 1,
            "ran_at": "2020-01-01T00:00:00Z",
        }

    original_runner = app_module._run_management_quotes_command
    app_module._run_management_quotes_command = fake_run

    try:
        response, status_code = _extract_json_response(
            _run_management_quotes_route_with_body(
                app_module,
                "extract",
                {
                    "ticker_id": 12,
                    "provider": "ollama",
                    "model": "qwen3.5:30b",
                    "sleep": 0.1,
                    "limit": 2,
                },
            )
        )
        assert status_code == 202
        job_id = response.get("job_id")
        assert isinstance(job_id, str)

        deadline = time.time() + 1.0
        job_status = None
        while time.time() < deadline:
            job_status, job_http_status = _extract_json_response(_run_management_quotes_job_status_route(app_module, job_id))
            if job_status.get("status") == "completed":
                break
            time.sleep(0.01)

        assert job_http_status == 200
        assert job_status.get("job_id") == job_id
        assert job_status.get("command") == "extract"
        assert job_status.get("status") == "completed"
        assert job_status.get("ok") is True
        result_payload = job_status.get("result")
        assert isinstance(result_payload, dict)
        assert result_payload.get("ok") is True
        assert result_payload.get("stdout") == "done"
    finally:
        app_module._run_management_quotes_command = original_runner


def test_api_management_quotes_job_status_not_found():
    app_module = _load_imager_app_module_for_tests()
    _clear_management_quotes_jobs_for_tests(app_module)

    response, status_code = _extract_json_response(_run_management_quotes_job_status_route(app_module, "missing-job-id"))
    assert status_code == 404
    assert response.get("ok") is False
    assert response.get("error_code") == "MANAGEMENT_QUOTES_JOB_NOT_FOUND"


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
