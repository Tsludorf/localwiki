# warlock_ingester - Integration Documentation

## Current Working System

We have successfully implemented and validated a complete local RAG system with the following working components:

### 1. Pipeline Overview
```text
Input Source → Parser → Chunker → Embedder → Vector Store (Qdrant) → RAG (AnythingLLM)
```

### 2. Component Integration

#### Input Sources
- Folder ingestion (multiple files)
- File ingestion (single file)
- ZIM archive support (experimental)  
- Wikipedia dumps (experimental)

#### Processing Components
- **Parser**: Text file parser (with modular framework)
- **Chunker**: Section-aware chunking with configurable size
- **Embedder**: Ollama embeddings generator
- **Vector Store**: Qdrant vector database
- **RAG**: AnythingLLM query interface

#### Integration Points
- **Qdrant**: Vector storage with collection/alias naming
- **AnythingLLM**: Workspace slug matching for collections
- **Ollama**: Embedding generation service

### 3. Validation Results

The system has been tested and proven to work end-to-end with:

#### 3.1 Functional Validation
✅ Source ingestion from folder structure  
✅ Text parsing and chunking  
✅ Ollama embedding generation (nomic-embed-text)  
✅ Qdrant vector storage and retrieval  
✅ AnythingLLM RAG querying using local vectors  
✅ Change detection and resume functionality  
✅ Deduplication using content hashing  

#### 3.2 Technical Validation
✅ Collection/alias naming matches AnythingLLM requirements  
✅ Embedding dimensions (768) match between systems  
✅ Section-aware chunking preserves document semantics  
✅ SQLite registration system works for ingestion state tracking  

#### 3.3 Performance Verification
✅ Resumable ingestion for incremental updates  
✅ Efficient deduplication  
✅ Scalable chunking  

### 4. System Commands

#### Source Management
```bash
# Add sources
localwiki sources add-folder /path/to/folder --display-name "My Folder"
localwiki sources add-file /path/to/file.txt --display-name "My File"

# List sources
localwiki sources list

# Remove sources
localwiki sources remove <source_id>
```

#### Ingestion  
```bash
# Start full ingestion
localwiki ingest

# Resume previous ingestion
localwiki ingest resume

# Process only changed files
localwiki ingest changed-only
```

#### Qdrant Operations
```bash
# List collections
localwiki qdrant list

# Show collection info
localwiki qdrant info <collection_name>

# Manage aliases
localwiki qdrant alias-add <collection> <alias>
localwiki qdrant alias-switch <alias> <new_collection>
```

#### Search
```bash
# Search the ingested content
localwiki search "what is machine learning?"

# Preview chunks
localwiki preview <source_item_id>
```

### 5. Integration Details

#### AnythingLLM Bridge (Mode 2)
The system works with AnythingLLM in Mode 2 (bridge approach):
- Qdrant collection name matches AnythingLLM workspace slug
- Vector dimensions match required embedding model
- Local processing pipeline generates vectors that AnythingLLM can query

#### Qdrant Integration
- Uses standard Qdrant client for connections
- Supports alias management for workspace mapping
- Efficient vector storage and retrieval

#### Ollama Integration  
- Leverages local Ollama instance for embeddings
- Default model: `nomic-embed-text` (768 dimensions)
- Configurable Ollama URL

### 6. Configuration

Configuration is handled through:
- Environment variables (`.env` file)
- Command-line options  
- JSON configuration files
- Default parameter fallbacks

### 7. Data Flow

1. **Source Registration**: Add files/folders to system registry
2. **Ingestion Run**: Process all source items
3. **Content Parsing**: Individual file parsing by type
4. **Text Chunking**: Section-aware chunking for semantic preservation  
5. **Embedding Generation**: Ollama embeddings for chunks
6. **Vector Storage**: Qdrant vector database storage
7. **RAG Querying**: AnythingLLM queries against local vectors

### 8. File Structure

```
warlock-ingester/
├── localwiki            # CLI entry point
├── src/                 # Source code
│   ├── config.py        # Configuration management  
│   ├── core.py          # Core components
│   ├── parsers.py       # Parser framework
│   └── qdrant_utils.py  # Qdrant utilities
├── scripts/             # Supporting scripts
├── data/                # Sample data  
├── .env                 # Environment config
└── README.md            # System documentation
```

### 9. Limitations and Current Scope

#### Current Features
- Local-first RAG system
- Multi-source ingestion (folder, file)
- Resumable ingestion with change detection  
- Deduplication capabilities
- Section-aware chunking
- Qdrant vector storage
- AnythingLLM bridge integration

#### Future Expansion
- Additional source types (PDF, ZIM, Wikipedia XML)
- Advanced chunking strategies
- Citation-first search patterns
- Hybrid search (vector + keyword)
- Cloud data source support
- API endpoints for external integrations

### 10. Performance Notes

- Efficient SQLite state tracking
- Configurable chunk size for different content types
- Support for incremental updates with change detection
- Optimized vector storage and lookup
- Memory-efficient processing pipeline

This documentation represents our current working system. The architecture is designed for extensibility and allows continuous expansion while maintaining the core functionality that has already been proven to work end-to-end.