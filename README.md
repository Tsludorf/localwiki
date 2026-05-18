# localwiki# warlock_ingester

A local data ingestion system for building knowledge bases using Qdrant vector storage.

## Features

- Support for multiple data source types (folders, ZIM archives, Wikipedia dumps)
- SQLite registry for tracking ingestion state and enabling resume capability
- Section-aware text chunking with overlap for context preservation
- Integration with Qdrant for vector storage
- Content deduplication using hash-based detection
- Ollama embedding generation
- AnythingLLM RAG integration

## Quick Start

```bash
# Clone and enter the project
cd /home/loc-llm/warlock_ingester

# Install dependencies
pip install -e .

# Initialize the registry
localwiki init

# Add a folder as a data source
localwiki sources add-folder ~/Documents --name personal_docs

# Ingest content
localwiki ingest --source <source_id> --kb main_kb

# Search
localwiki search --query "your question" --top 5
```

## Prerequisites

- Python 3.12+
- Qdrant (vector database)
- Ollama (for embeddings)
- Docker (for Qdrant and AnythingLLM services)

### Starting Services

```bash
# Start Qdrant
docker run -p 6333:6333 qdrant/qdrant

# Start Ollama
ollama serve
ollama pull nomic-embed-text
```

## CLI Commands

```bash
# Initialize
localwiki init                    # Initialize the registry database

# Source Management
localwiki sources add-folder ~/Documents --name personal_docs
localwiki sources add-zim ~/wiki.zim --name wikipedia_zim
localwiki sources add-wikidump ~/dumps/enwiki --name enwiki_dump

# Ingestion
localwiki ingest --source <source_id> --kb <kb_name>
localwiki ingest --source <source_id> --kb <kb_name> --changed-only

# Search
localwiki search --query "What is AI?" --top 5

# Qdrant Collections
localwiki collections list

# Status
localwiki status

# Help - Detailed usage information
localwiki help

# Troubleshoot - Diagnose common issues
localwiki troubleshoot
```

## Project Structure

```
warlock_ingester/
├── src/
│   ├── cli.py          # Command-line interface
│   ├── core.py         # Core ingestion pipeline, registry, chunking
│   ├── qdrant_utils.py # Qdrant vector storage utilities
│   ├── parsers.py      # Data source parsers
│   ├── config.py       # Configuration management
│   └── main.py         # Entry point
├── setup.py
├── README.md
├── ARCHITECTURE.md
└── INTEGRATION.md
```

## Core Components

- **IngestionRegistry**: SQLite-based state tracking with support for resume capability
- **Chunker**: Token-aware text chunking with section preservation
- **DocumentManager**: Content hashing for deduplication
- **BaseParser**: Framework for different source types (folder, ZIM, wikidump)

## Environment Variables

- `OLLAMA_URL` - Ollama endpoint (default: `http://127.0.0.1:11434`)
- `QDRANT_URL` - Qdrant endpoint (default: `http://127.0.0.1:6333`)

## Development

```bash
# Run tests
python -m pytest src/test_core.py

# Verify system
python verify_system.py
```