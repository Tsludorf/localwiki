# warlock_ingester - System Architecture & Expansion Plan

## Current System Status

We have a fully functional local RAG system that has proven end-to-end capability:
- Parsing and chunking
- Ollama embeddings generation
- Qdrant vector storage
- AnythingLLM RAG querying

Integration points have been validated and work together effectively.

## Architecture Overview

### 1. Core Ingestion Flow
```
[Source] → [Parser] → [Chunker] → [Embedder] → [Vector Store (Qdrant)]
     ↑
[Registry] → [Ingestion Engine] → [Document Manager] 
```

### 2. CLI Interface
The `localwiki` command-line tool provides:
- Source management: add-folder, add-zim, add-wikidump
- Ingestion controls: start, resume, changed-only processing
- Search and preview functionality
- Qdrant integration: list, alias management
- AnythingLLM bridge support

### 3. Data Models
- `Source`: Representation of data sources (folder, ZIM, etc.)
- `SourceItem`: Individual files within sources
- `CanonicalDocument`: Processed document structure
- `Chunk`: Text chunks with semantic boundaries
- `VectorPoint`: Qdrant storage representation

## Configuration and Extensibility

### 1. Flexible Config System
We plan to support:
- Multi-mode configuration (Mode 1 - AnythingLLM bridge, Mode 2 - local processing)
- Source-specific settings
- Embedding model selection
- Qdrant connection parameters

### 2. Parser Modular System
The system supports:
- Extensible parser architecture
- Support for different source types (text, ZIM, Wikipedia XML, etc.)
- Pluggable parser interface
- Custom parsing logic per source type

### 3. Bridge Integration
We support integration with:
- AnythingLLM (both Mode 1 and Mode 2 approaches)
- Qdrant collection aliasing for workspace mapping
- Consistent embedding vector dimensions

## Expansion Roadmap

### Phase 1: Configuration Enhancements
- Add configuration files for different ingestion modes  
- Support for source-specific settings
- Environment variable integration
- Default parameter configuration

### Phase 2: Parser Ecosystem
- Add concrete parser implementations for:
  - Text files (simple)
  - ZIM archives 
  - Wikipedia XML dumps
  - PDF documents
  - Markdown files
- Parser configuration and testing framework

### Phase 3: Advanced Features
- Citation-first search patterns
- Hybrid search (vectors + keyword)
- Advanced chunking strategies
- Processing pipeline optimization

### Phase 4: Integration Expansion
- Support for other vector stores
- Integration with additional LLM platforms
- Cloud storage source support
- API endpoints for external applications

## Technical Considerations

### Database Design
The SQLite registry includes:
- Sources table
- Source items table  
- Canonical documents table
- Chunks table
- Vector points table
- Ingestion runs table
- Parser errors table

### Memory and Performance
- Resumable ingestion with change detection
- Deduplication logic using content hashing
- Chunking that preserves document sections
- Efficient Qdrant operations

### Compatibility
The system maintains compatibility with:
- AnythingLLM workspace slug naming
- Standard Qdrant collection/alias patterns
- Ollama embedding dimensions
- SQLite-based state tracking

## Integration Validation

We have confirmed that:
1. The pipeline from source → embedding → Qdrant → AnythingLLM works
2. Collection/alias naming aligns with AnythingLLM workspace slugs  
3. Embedding dimensions match between systems
4. Change detection works for resumable ingestion
5. Section-aware chunking preserves semantic meaning

## Future Development Considerations

### Performance
- Distributed processing options
- Caching strategies for embeddings  
- Indexing optimizations
- Parallel ingestion support

### Scalability
- Multi-source ingestion
- Cloud data source support
- Load balancing approaches
- Monitoring and logging capabilities

### Usability
- Web interface (optional)
- Dashboard for ingestion status
- Export/import capabilities
- User guidance and help system

## Current Working System

The system has been validated with:
- Test ingestion of text sources
- Qdrant vector storage and retrieval
- AnythingLLM integration
- Change detection (resume capability)
- Deduplication functionality

The approach taken provides maximum flexibility while maintaining:
- Clean separation of concerns
- Extensible architecture
- Local-first design philosophy
- Integration with existing LLM platforms