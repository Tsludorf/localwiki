#!/usr/bin/env python3
"""Ingestion orchestration service."""

import hashlib
import time
import traceback
import uuid
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Optional


class IngestionService:
    """Orchestrates ingestion workflow with injected collaborators."""

    def __init__(
        self,
        *,
        logger,
        parser_factory,
        source_item_cls,
        ingestion_run_cls,
        parser_error_cls,
        document_manager_cls,
        chunker_cls,
        chunk_cls,
        json_dumps_fn,
        detect_source_type_and_mime,
        resolve_model_ingestion_config,
        low_value_chunk_predicate,
        embed_chunks_and_store,
    ):
        self.logger = logger
        self.parser_factory = parser_factory
        self.source_item_cls = source_item_cls
        self.ingestion_run_cls = ingestion_run_cls
        self.parser_error_cls = parser_error_cls
        self.document_manager_cls = document_manager_cls
        self.chunker_cls = chunker_cls
        self.chunk_cls = chunk_cls
        self.json_dumps_fn = json_dumps_fn
        self.detect_source_type_and_mime = detect_source_type_and_mime
        self.resolve_model_ingestion_config = resolve_model_ingestion_config
        self.low_value_chunk_predicate = low_value_chunk_predicate
        self.embed_chunks_and_store = embed_chunks_and_store

    def run(
        self,
        source_id: str,
        registry,
        qdrant_client,
        embedder: str,
        kb_name: str = "wiki_kb",
        limit: int = 10000,
        collection_name: Optional[str] = None,
        changed_only: bool = False,
        embed_batch_size: Optional[int] = None,
        qdrant_batch_size: Optional[int] = None,
        chunk_tokens: Optional[int] = None,
        chunk_overlap: Optional[int] = None,
        max_chunk_chars: Optional[int] = None,
    ) -> Dict[str, int]:
        self.logger.info("Starting ingestion pipeline for source: %s", source_id)
        run_started_at = time.time()
        ingestion_config = self.resolve_model_ingestion_config(
            embedder=embedder,
            embed_batch_size=embed_batch_size,
            qdrant_batch_size=qdrant_batch_size,
            chunk_tokens=chunk_tokens,
            chunk_overlap=chunk_overlap,
            max_chunk_chars=max_chunk_chars,
        )
        model_name = ingestion_config["model_name"]
        effective_embed_batch_size = ingestion_config["embed_batch_size"]
        effective_qdrant_batch_size = ingestion_config["qdrant_batch_size"]
        effective_chunk_tokens = ingestion_config["chunk_tokens"]
        effective_chunk_overlap = ingestion_config["chunk_overlap"]
        effective_max_chunk_chars = ingestion_config["max_chunk_chars"]

        effective_limit = limit if (limit and limit > 0) else None
        print(f"[Ingestion Start] source={source_id} model={model_name} kb={kb_name}")
        print(
            "[Ingestion Config] "
            f"chunk_tokens={effective_chunk_tokens} "
            f"chunk_overlap={effective_chunk_overlap} "
            f"embed_batch_size={effective_embed_batch_size} "
            f"qdrant_batch_size={effective_qdrant_batch_size} "
            f"max_chunk_chars={effective_max_chunk_chars} "
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

        run_id = f"run_{uuid.uuid4().hex}"
        source = registry.get_source(source_id)

        if not source:
            self.logger.error("Source %s not found", source_id)
            return stats

        run = self.ingestion_run_cls(
            ingest_run_id=run_id,
            source_id=source_id,
            started_at=str(int(time.time())),
            status="running",
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

            if source.source_type == "folder":
                for file_path in Path(source.root_uri).rglob("*"):
                    if not file_path.is_file():
                        continue

                    discovery_seen += 1
                    stat = file_path.stat()
                    _, mime_type = self.detect_source_type_and_mime(file_path)
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
                    source_item = self.source_item_cls(
                        source_item_id=item_id,
                        source_id=source.source_id,
                        uri=str(file_path),
                        display_uri=str(file_path.relative_to(source.root_uri)),
                        mime_type=mime_type,
                        size_bytes=stat.st_size,
                        mtime=mtime,
                        content_hash=f"{stat.st_size}:{mtime}",
                        status="pending",
                    )
                    registry.add_source_item(source_item)
                    discovery_registered += 1
            elif source.source_type in {"zim", "wikidump", "file", "text", "pdf"}:
                source_file = Path(source.root_uri)
                if source_file.exists() and source_file.is_file():
                    discovery_seen += 1
                    stat = source_file.stat()
                    _, mime_type = self.detect_source_type_and_mime(source_file)
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
                        self.logger.info("Skipping unchanged source item: %s", source_file)
                    else:
                        source_item = self.source_item_cls(
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
                    self.logger.error("Source file not found: %s", source.root_uri)

            self.logger.info(
                "Discovery summary for source=%s: seen=%s registered=%s skipped_unchanged=%s changed_only=%s",
                source_id,
                discovery_seen,
                discovery_registered,
                discovery_skipped_unchanged,
                changed_only,
            )

            items = registry.get_unprocessed_items(source_id)
            if changed_only:
                before_filter = len(items)
                items = [item for item in items if item.source_item_id in discovered_item_ids]
                self.logger.info(
                    "Changed-only queue filter applied: before=%s after=%s filtered_out=%s",
                    before_filter,
                    len(items),
                    before_filter - len(items),
                )
            status_counts = Counter(item.status for item in items)
            self.logger.info(
                "Queued items for processing: total=%s status_breakdown=%s",
                len(items),
                dict(status_counts),
            )

            for item in items:
                try:
                    parser = None
                    self.logger.info("Processing item: %s", item.uri)
                    chunks_before_item = stats["chunks_created"]
                    chunks_prepared_before_item = stats["chunks_prepared"]
                    embedded_before_item = stats["chunks_embedded"]
                    vectors_before_item = stats["points_stored"]

                    parser = self.parser_factory.create_parser_for_item(item)
                    parsed_docs = parser.parse_documents(item)
                    self.logger.info(
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
                    doc_manager = self.document_manager_cls(registry)
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
                        if self.low_value_chunk_predicate(text, title=title):
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

                        chunker = self.chunker_cls(max_tokens=effective_chunk_tokens, overlap_tokens=effective_chunk_overlap, min_tokens=300)
                        chunks_list = chunker.chunk_text(text, "", metadata)
                        item_chunks_raw += len(chunks_list)

                        chunked_data = []
                        for chunk_data in chunks_list:
                            if len(chunk_data["chunk_text"]) > effective_max_chunk_chars:
                                chunk_data["chunk_text"] = chunk_data["chunk_text"][:effective_max_chunk_chars]
                                item_chunks_truncated += 1
                            if self.low_value_chunk_predicate(chunk_data["chunk_text"], title=title):
                                item_chunks_low_value += 1
                                continue
                            chunk_id = f"chunk_{hashlib.md5((document.doc_id + ':' + str(chunk_data['chunk_index']) + ':' + chunk_data['chunk_text']).encode('utf-8')).hexdigest()}"
                            chunk = self.chunk_cls(
                                chunk_id=chunk_id,
                                doc_id=document.doc_id,
                                chunk_index=chunk_data['chunk_index'],
                                section=chunk_data['section'],
                                char_start=chunk_data['char_start'],
                                char_end=chunk_data['char_end'],
                                token_estimate=chunk_data['token_estimate'],
                                text_hash=doc_manager.generate_hash(chunk_data['chunk_text']),
                                citation_json=self.json_dumps_fn(
                                    {
                                        "metadata": {
                                            **(chunk_data.get('metadata') or {}),
                                            "title": title,
                                            "source_uri": source_uri,
                                            "source_type": source_type,
                                            "display_uri": display_uri,
                                        },
                                        "chunk_text": chunk_data['chunk_text'],
                                    }
                                ),
                            )

                            registry.add_chunk(chunk)
                            chunked_data.append(chunk)
                        stats["chunks_prepared"] += len(chunked_data)

                        embed_stats = self.embed_chunks_and_store(
                            qdrant_client,
                            chunked_data,
                            embedder,
                            document,
                            registry,
                            item,
                            source,
                            kb_name,
                            collection_name=collection_name,
                            embed_batch_size=effective_embed_batch_size,
                            qdrant_batch_size=effective_qdrant_batch_size,
                            embed_max_tokens=effective_chunk_tokens,
                            max_chunk_chars=effective_max_chunk_chars,
                        )
                        stats["chunks_created"] += embed_stats["chunks_embedded"]
                        stats["chunks_embedded"] += embed_stats["chunks_embedded"]
                        stats["points_stored"] += embed_stats["points_stored"]

                    if effective_limit is not None and stats["documents_discovered"] >= effective_limit:
                        self.logger.info("Reached ingest limit (%s documents discovered)", effective_limit)
                        break

                    registry.update_source_item_status(item.source_item_id, "completed")
                    processed_items += 1
                    elapsed = max(0.001, time.time() - run_started_at)
                    chunks_per_sec = stats["chunks_created"] / elapsed
                    item_chunks = stats["chunks_created"] - chunks_before_item
                    item_chunks_prepared = stats["chunks_prepared"] - chunks_prepared_before_item
                    item_embedded = stats["chunks_embedded"] - embedded_before_item
                    item_vectors = stats["points_stored"] - vectors_before_item
                    self.logger.info(
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
                    self.logger.error("Error processing item %s: %s", item.uri, str(e))
                    had_item_errors = True
                    failed_items += 1
                    registry.update_source_item_status(item.source_item_id, "failed")
                    error = self.parser_error_cls(
                        error_id=f"err_{uuid.uuid4().hex}",
                        ingest_run_id=run_id,
                        source_item_id=item.source_item_id,
                        parser=getattr(locals().get("parser"), "parser_name", "unknown"),
                        error_type=type(e).__name__,
                        message=str(e),
                        traceback=traceback.format_exc(),
                        created_at=str(int(time.time())),
                    )
                    registry.add_parser_error(error)

            if had_item_errors:
                registry.update_ingestion_run_status(run_id, "failed", str(int(time.time())))
                raise RuntimeError(
                    f"Ingestion failed for source {source_id}: processed={processed_items}, failed={failed_items}"
                )

            registry.update_ingestion_run_status(run_id, "completed", str(int(time.time())))
            self.logger.info("Ingestion pipeline complete for source: %s", source_id)
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
            self.logger.error("Error in ingestion pipeline: %s", str(e))
            registry.update_ingestion_run_status(run_id, "failed", str(int(time.time())))
            elapsed = max(0.001, time.time() - run_started_at)
            stats["elapsed_seconds"] = int(elapsed)
            stats["avg_chunks_per_sec"] = round(stats["chunks_created"] / elapsed, 2)
            stats["status"] = "failed"
            return stats
