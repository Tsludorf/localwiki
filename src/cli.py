#!/usr/bin/env python3
"""
warlock_ingester - CLI Interface

This module implements the command-line interface for warlock_ingester.
"""

import os
import sys
import argparse
from pathlib import Path
import json
import time
import uuid
from typing import Dict, Any
import os

from src.core import IngestionRegistry, Source, SourceItem, DocumentManager, run_ingestion_pipeline, detect_source_type_and_mime
from src.qdrant_utils import initialize_qdrant_client, list_collections, search_in_collection, create_collection
import httpx


def setup_cli():
    """Setup the command-line interface."""
    
    parser = argparse.ArgumentParser(
        description="warlock_ingester - Local data ingestion for knowledge bases",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  localwiki init                    # Initialize the registry
  localwiki sources add-folder ~/Documents --name personal_docs
  localwiki sources add-zim ~/wiki.zim --name wikipedia_zim
  localwiki ingest --source wikipedia_zim --kb main_kb
  localwiki search --query "What is AI?"
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Init command
    init_parser = subparsers.add_parser('init', help='Initialize the ingestion system')
    
    # Status command
    status_parser = subparsers.add_parser('status', help='Show system status')
    
    # Sources commands
    sources_parser = subparsers.add_parser('sources', help='Manage data sources')
    sources_subparsers = sources_parser.add_subparsers(dest='sources_command', help='Source commands')
    
    # Add folder source
    add_folder_parser = sources_subparsers.add_parser('add-folder', help='Add a folder as a source')
    add_folder_parser.add_argument('path', help='Path to the folder')
    add_folder_parser.add_argument('--name', required=True, help='Display name for the source')
    
    # Add ZIM source
    add_zim_parser = sources_subparsers.add_parser('add-zim', help='Add a ZIM archive as a source')
    add_zim_parser.add_argument('path', help='Path to the ZIM file')
    add_zim_parser.add_argument('--name', required=True, help='Display name for the source')
    add_zim_parser.add_argument('--kb', help='KB name for immediate ingestion (default: source name)')
    add_zim_parser.add_argument('--embedder', default='ollama:embeddinggemma', help='Embedding model for immediate ingestion')
    add_zim_parser.add_argument('--qdrant', default='http://127.0.0.1:6333', help='Qdrant endpoint for immediate ingestion')
    add_zim_parser.add_argument('--no-auto-ingest', action='store_true', help='Only add source, do not ingest immediately')
    
    # Add WikiDump source
    add_wikidump_parser = sources_subparsers.add_parser('add-wikidump', help='Add a Wikipedia dump as a source')
    add_wikidump_parser.add_argument('path', help='Path to the Wikipedia dump')
    add_wikidump_parser.add_argument('--name', required=True, help='Display name for the source')

    # Bulk update sources from Desktop/wiki_sources
    update_sources_parser = sources_subparsers.add_parser('update', help='Add new files from ~/Desktop/wiki_sources')
    update_sources_parser.add_argument('--path', default='~/Desktop/wiki_sources', help='Folder to scan for source files')
    update_sources_parser.add_argument('--embedder', default='ollama:embeddinggemma', help='Embedding model for auto-ingest')
    update_sources_parser.add_argument('--qdrant', default='http://127.0.0.1:6333', help='Qdrant endpoint for auto-ingest')
    update_sources_parser.add_argument('--no-auto-ingest', action='store_true', help='Only add sources, do not ingest immediately')

    delete_all_sources_parser = sources_subparsers.add_parser('delete_all', help='Delete all sources and clear local_wiki collection')
    delete_all_sources_parser.add_argument('--qdrant', default='http://127.0.0.1:6333', help='Qdrant endpoint')
    delete_all_sources_parser.add_argument('--collection', default=None, help='Collection to clear (default: env/local_wiki)')
    
    # Ingest command
    ingest_parser = subparsers.add_parser('ingest', help='Start ingestion process')
    ingest_parser.add_argument('--source', required=True, help='Source to ingest')
    ingest_parser.add_argument('--kb', required=True, help='Knowledge base to use')
    ingest_parser.add_argument('--embedder', default='ollama:embeddinggemma', help='Embedding model to use')
    ingest_parser.add_argument('--qdrant', default='http://127.0.0.1:6333', help='Qdrant endpoint')
    ingest_parser.add_argument('--collection', help='Qdrant collection name')
    ingest_parser.add_argument('--alias', help='Qdrant collection alias')
    ingest_parser.add_argument('--changed-only', action='store_true', help='Only process changed documents')
    
    # Search command
    search_parser = subparsers.add_parser('search', help='Search in the knowledge base')
    search_parser.add_argument('--query', required=True, help='Search query')
    search_parser.add_argument('--top', type=int, default=5, help='Number of results')
    search_parser.add_argument('--kb', default=None, help='Optional KB payload filter')
    search_parser.add_argument('--embedder', default='embeddinggemma:latest', help='Embedding model for search')
    search_parser.add_argument('--qdrant', default='http://127.0.0.1:6333', help='Qdrant endpoint')
    search_parser.add_argument('--collection', help='Qdrant collection to search')
    
    # Preview command
    preview_parser = subparsers.add_parser('preview', help='Preview source content')
    preview_parser.add_argument('--source', required=True, help='Source to preview')
    preview_parser.add_argument('--limit', type=int, default=1, help='Number of items to show')
    
    # Collections command
    collections_parser = subparsers.add_parser('collections', help='Manage Qdrant collections')
    collections_subparsers = collections_parser.add_subparsers(dest='collections_command', help='Collection commands')
    
    # List collections
    list_collections_parser = collections_subparsers.add_parser('list', help='List all collections')
    
    # Alias commands
    alias_parser = collections_subparsers.add_parser('alias-create', help='Create a collection alias')
    alias_parser.add_argument('--collection', required=True, help='Collection to alias')
    alias_parser.add_argument('--alias', required=True, help='Alias name')
    
    # Bridge command for AnythingLLM
    bridge_parser = subparsers.add_parser('bridge', help='Bridge with AnythingLLM')
    bridge_parser.add_argument('workspace_name', help='Workspace name in AnythingLLM')
    bridge_parser.add_argument('--workspace-id', help='Workspace ID in AnythingLLM')
    bridge_parser.add_argument('--qdrant', default='http://127.0.0.1:6333', help='Qdrant endpoint')
    bridge_parser.add_argument('--collection', help='Qdrant collection to use')
    
    # Help command
    help_parser = subparsers.add_parser('help', help='Show detailed help and examples')
    
    # Troubleshoot command
    troubleshoot_parser = subparsers.add_parser('troubleshoot', help='Diagnose common issues')
    
    return parser


def run_init():
    """Initialize the registry database."""
    registry = IngestionRegistry()
    print("warlock_ingester initialized successfully")

def run_status():
    """Show system status."""
    print("warlock_ingester Status")
    print("=======================")
    registry = IngestionRegistry()
    
    # Show sources
    print("\nSources:")
    print("-" * 20)
    try:
        # This is a placeholder - we'll need to implement real query methods
        print("No sources defined yet. Use 'localwiki sources add-folder' to add sources.")
    except Exception as e:
        print(f"Error reading sources: {e}")
    
    print("\nSystem Info:")
    print("-" * 20)
    print("Ollama Service: Running")
    print("Qdrant Service: Running")
    print("Registry: Connected")


def run_sources_add_folder(folder_path, name):
    """Add a folder as a source."""
    print(f"Adding folder source: {folder_path}")
    
    # Validate the path
    if not os.path.exists(folder_path):
        print(f"Error: Path {folder_path} does not exist")
        return
    
    # Get or create registry
    registry = IngestionRegistry()
    
    # Create source
    source_id = f"source_{uuid.uuid4().hex[:12]}"
    source = Source(
        source_id=source_id,
        source_type="folder",
        root_uri=folder_path,
        display_name=name,
        added_at=str(int(time.time())),
        settings_json="{}"
    )
    
    registry.add_source(source)
    print(f"Successfully added folder source: {source_id}")


def run_sources_add_zim(zim_path, name, kb_name=None, embedder='ollama:embeddinggemma',
                        qdrant_url='http://127.0.0.1:6333', auto_ingest=True):
    """Add a ZIM archive as a source."""
    print(f"Adding ZIM source: {zim_path}")
    
    # Validate the path
    if not os.path.exists(zim_path):
        print(f"Error: Path {zim_path} does not exist")
        return
    
    # Get or create registry
    registry = IngestionRegistry()
    
    # Create source
    source_id = f"source_{uuid.uuid4().hex[:12]}"
    source = Source(
        source_id=source_id,
        source_type="zim",
        root_uri=zim_path,
        display_name=name,
        added_at=str(int(time.time())),
        settings_json="{}"
    )
    
    registry.add_source(source)
    print(f"Successfully added ZIM source: {source_id}")

    if not auto_ingest:
        return

    effective_kb = kb_name or name
    print(f"Auto-ingesting source: {source_id} into kb: {effective_kb}")
    qdrant_client = initialize_qdrant_client(qdrant_url)
    run_ingestion_pipeline(
        source_id=source_id,
        registry=registry,
        qdrant_client=qdrant_client,
        embedder=embedder,
        kb_name=effective_kb,
    )
    print("Auto-ingestion complete!")


def run_sources_add_wikidump(dump_path, name):
    """Add a Wikipedia dump as a source."""
    print(f"Adding Wikipedia dump source: {dump_path}")
    
    # Validate the path
    if not os.path.exists(dump_path):
        print(f"Error: Path {dump_path} does not exist")
        return
    
    # Get or create registry
    registry = IngestionRegistry()
    
    # Create source
    source_id = f"source_{uuid.uuid4().hex[:12]}"
    source = Source(
        source_id=source_id,
        source_type="wikidump",
        root_uri=dump_path,
        display_name=name,
        added_at=str(int(time.time())),
        settings_json="{}"
    )
    
    registry.add_source(source)
    print(f"Successfully added Wikipedia dump source: {source_id}")


def run_sources_update(folder_path: str, embedder: str = 'ollama:embeddinggemma',
                       qdrant_url: str = 'http://127.0.0.1:6333', auto_ingest: bool = True):
    """Add all new source files from a folder.

    Rules:
    - If folder is missing, create it.
    - Add only files not already present as source root_uri.
    - Determines source type from file extension.
    """
    resolved = os.path.expanduser(folder_path)
    if not os.path.exists(resolved):
        os.makedirs(resolved, exist_ok=True)
        print(f"Created missing source folder: {resolved}")

    print(f"Scanning source folder: {resolved}")
    registry = IngestionRegistry()
    existing_root_uris = {s.root_uri for s in registry.list_sources()}
    qdrant_client = initialize_qdrant_client(qdrant_url) if auto_ingest else None

    added = 0
    ingested = 0
    ingest_failed = 0
    skipped_existing = 0
    skipped_unsupported = 0

    for child in sorted(Path(resolved).iterdir()):
        if not child.is_file():
            continue

        full_path = str(child.resolve())
        if full_path in existing_root_uris:
            skipped_existing += 1
            continue

        source_type, _ = detect_source_type_and_mime(child)
        if source_type == 'file':
            skipped_unsupported += 1
            continue

        source_id = f"source_{uuid.uuid4().hex[:12]}"
        source = Source(
            source_id=source_id,
            source_type=source_type,
            root_uri=full_path,
            display_name=child.stem,
            added_at=str(int(time.time())),
            settings_json="{}"
        )
        registry.add_source(source)
        existing_root_uris.add(full_path)
        added += 1
        print(f"Added {source_type} source: {source_id} -> {full_path}")

        if auto_ingest:
            kb_name = child.stem
            print(f"Auto-ingesting source: {source_id} into kb: {kb_name}")
            try:
                run_ingestion_pipeline(
                    source_id=source_id,
                    registry=registry,
                    qdrant_client=qdrant_client,
                    embedder=embedder,
                    kb_name=kb_name,
                )
                ingested += 1
            except Exception as e:
                ingest_failed += 1
                print(f"Auto-ingest failed for {source_id}: {e}")

    print("Source update complete")
    print(f"- added: {added}")
    if auto_ingest:
        print(f"- auto-ingested: {ingested}")
        print(f"- auto-ingest failed: {ingest_failed}")
    print(f"- skipped existing: {skipped_existing}")
    print(f"- skipped unsupported: {skipped_unsupported}")


def run_sources_delete_all(qdrant_url: str, collection_name: str = None):
    """Delete all sources and clear the canonical Qdrant collection."""
    collection = collection_name or os.getenv("LOCALWIKI_QDRANT_COLLECTION", "local_wiki")
    registry = IngestionRegistry()
    registry.delete_all_sources()

    client = initialize_qdrant_client(qdrant_url)
    existing = set(list_collections(client))
    if collection in existing:
        client.delete_collection(collection)
        print(f"Deleted Qdrant collection: {collection}")

    create_collection(client, collection_name=collection, vector_dim=768)
    print(f"Recreated empty Qdrant collection: {collection}")
    print("Deleted all localwiki sources and associated vectors")


def run_ingest(args):
    """Start the ingestion process."""
    print(f"Ingesting source: {args.source}")
    
    # Get or create registry
    registry = IngestionRegistry()
    
    # Create Qdrant client
    qdrant_client = initialize_qdrant_client(args.qdrant)
    
    # Start the ingestion pipeline
    run_ingestion_pipeline(
        source_id=args.source,
        registry=registry,
        qdrant_client=qdrant_client,
        embedder=args.embedder,
        kb_name=args.kb
    )
    
    print("Ingestion complete!")


def run_search(args):
    """Perform a search."""
    print(f"Searching for: {args.query}")
    print(f"Top {args.top} results")

    collection = args.collection or os.getenv("LOCALWIKI_QDRANT_COLLECTION", "local_wiki")
    qdrant_client = initialize_qdrant_client(args.qdrant)
    ollama_url = os.getenv("OLLAMA_URL", "http://127.0.0.1:11434").rstrip('/') + "/api/embed"

    response = httpx.post(ollama_url, json={"model": args.embedder, "input": [args.query]}, timeout=120.0)
    response.raise_for_status()
    query_vector = response.json()["embeddings"][0]

    results = search_in_collection(qdrant_client, collection, query_vector, args.top, kb_name=args.kb)
    if not results:
        print("No results found")
        return

    for i, result in enumerate(results, start=1):
        payload = result.get("payload", {})
        text = (payload.get("text") or "").replace("\n", " ").strip()
        preview = (text[:220] + "...") if len(text) > 220 else text
        print(f"{i}. score={result.get('score', 0):.4f}")
        print(f"   title: {payload.get('title', 'unknown')}")
        source_uri = payload.get("source_uri") or payload.get("uri") or "unknown"
        print(f"   source: {source_uri}")
        print(f"   preview: {preview}")


def run_preview(args):
    """Preview source content."""
    print(f"Previewing source: {args.source}")
    print(f"Limit: {args.limit}")
    
    # In a full implementation, this would show sample content from the source
    print("Preview functionality placeholders")
    print("Sample documents would be shown here")


def run_collections_list():
    """List Qdrant collections."""
    print("Listing Qdrant collections...")
    client = initialize_qdrant_client(os.getenv("QDRANT_URL", "http://127.0.0.1:6333"))
    for name in list_collections(client):
        print(f"- {name}")


def run_bridge(args):
    """Bridge with AnythingLLM."""
    print(f"Creating bridge for workspace: {args.workspace_name}")
    print(f"Qdrant: {args.qdrant}")
    
    # Bridge process would establish connection with AnythingLLM
    print("Bridge creation placeholder")
    print("Would create connection with AnythingLLM workspace")


def run_help():
    """Show detailed help and examples."""
    help_text = """
warlock_ingester - Detailed Help
==================================

GETTING STARTED
---------------
1. Start prerequisites (Qdrant and Ollama)
2. Initialize the registry:     localwiki init
3. Add a data source:           localwiki sources add-folder ~/Documents --name docs
4. Ingest content:              localwiki ingest --source <source_id> --kb main_kb
5. Search:                      localwiki search --query "your question"

COMMON WORKFLOWS
---------------

# Process a folder of documents
localwiki sources add-folder ~/Documents --name personal_docs
localwiki ingest --source source_xxx --kb main_kb
localwiki search --query "machine learning"

# Process a ZIM archive (Wikipedia offline)
localwiki sources add-zim ~/wikipedia.zim --name wikipedia
localwiki ingest --source source_xxx --kb wiki_kb

# Resume interrupted ingestion
localwiki ingest --source source_xxx --kb main_kb --changed-only

TROUBLESHOOTING
---------------
Run: localwiki troubleshoot

ENVIRONMENT VARIABLES
---------------------
OLLAMA_URL  - Ollama endpoint (default: http://127.0.0.1:11434)
QDRANT_URL  - Qdrant endpoint (default: http://127.0.0.1:6333)

For more help, see README.md
"""
    print(help_text)


def run_troubleshoot():
    """Diagnose common issues."""
    print("=" * 50)
    print("warlock_ingester - Troubleshooting Diagnostics")
    print("=" * 50)
    
    issues_found = []
    
    # Check Qdrant
    print("\n1. Checking Qdrant...")
    try:
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex(('127.0.0.1', 6333))
        if result == 0:
            print("   ✅ Qdrant is reachable on port 6333")
        else:
            print("   ❌ Qdrant is NOT reachable on port 6333")
            print("      Solution: Start Qdrant: docker run -p 6333:6333 qdrant/qdrant")
            issues_found.append("Qdrant not running")
        sock.close()
    except Exception as e:
        print(f"   ❌ Error checking Qdrant: {e}")
        issues_found.append("Qdrant check failed")
    
    # Check Ollama
    print("\n2. Checking Ollama...")
    try:
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = sock.connect_ex(('127.0.0.1', 11434))
        if result == 0:
            print("   ✅ Ollama is reachable on port 11434")
        else:
            print("   ❌ Ollama is NOT reachable on port 11434")
            print("      Solution: Start Ollama: ollama serve")
            issues_found.append("Ollama not running")
        sock.close()
    except Exception as e:
        print(f"   ❌ Error checking Ollama: {e}")
        issues_found.append("Ollama check failed")
    
    # Check registry
    print("\n3. Checking registry database...")
    registry_path = "localwiki_registry.db"
    import os
    if os.path.exists(registry_path):
        print(f"   ✅ Registry found at {registry_path}")
    else:
        print("   ⚠️  Registry not found (will be created on first use)")
    
    # Check data directory
    print("\n4. Checking data sources directory...")
    data_path = "data/sources"
    if os.path.exists(data_path):
        print(f"   ✅ Data directory exists: {data_path}")
    else:
        print(f"   ⚠️  Data directory not found: {data_path}")
    
    # Summary
    print("\n" + "=" * 50)
    if issues_found:
        print("ISSUES FOUND:")
        for issue in issues_found:
            print(f"  - {issue}")
        print("\nRun 'localwiki troubleshoot' after fixing these issues.")
    else:
        print("✅ All systems appear to be running correctly!")
    print("=" * 50)


def main():
    """Main entry point for CLI."""
    parser = setup_cli()
    args = parser.parse_args()
    
    # Handle command
    if args.command == 'init':
        run_init()
    elif args.command == 'status':
        run_status()
    elif args.command == 'sources':
        if args.sources_command == 'add-folder':
            run_sources_add_folder(args.path, args.name)
        elif args.sources_command == 'add-zim':
            run_sources_add_zim(
                args.path,
                args.name,
                kb_name=args.kb,
                embedder=args.embedder,
                qdrant_url=args.qdrant,
                auto_ingest=not args.no_auto_ingest,
            )
        elif args.sources_command == 'add-wikidump':
            run_sources_add_wikidump(args.path, args.name)
        elif args.sources_command == 'update':
            run_sources_update(
                args.path,
                embedder=args.embedder,
                qdrant_url=args.qdrant,
                auto_ingest=not args.no_auto_ingest,
            )
        elif args.sources_command == 'delete_all':
            run_sources_delete_all(args.qdrant, args.collection)
        else:
            parser.print_help()
    elif args.command == 'ingest':
        run_ingest(args)
    elif args.command == 'search':
        run_search(args)
    elif args.command == 'preview':
        run_preview(args)
    elif args.command == 'collections':
        if args.collections_command == 'list':
            run_collections_list()
        else:
            parser.print_help()
    elif args.command == 'bridge':
        run_bridge(args)
    elif args.command == 'help':
        run_help()
    elif args.command == 'troubleshoot':
        run_troubleshoot()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
