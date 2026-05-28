#!/usr/bin/env python3
"""Embedding service boundary for Ollama interactions."""

import os
import re
from typing import Any, Dict, List, Optional, Tuple

from src.errors import EmbeddingError


EMBED_CONTEXT_FALLBACK_TOKENS = [600, 384, 300, 192, 128]


def _coalesce_positive_int(override: Optional[int], default_value: int) -> int:
    if isinstance(override, int) and override > 0:
        return override
    return int(default_value)


def _estimate_token_count(text: str) -> int:
    return len(re.findall(r"\S+", text or ""))


def _token_caps_for_retry(initial_cap: int) -> List[int]:
    ordered: List[int] = [max(1, initial_cap)]
    for cap in EMBED_CONTEXT_FALLBACK_TOKENS:
        if cap < ordered[0] and cap not in ordered:
            ordered.append(cap)
    return [cap for cap in ordered if cap > 0]


def _is_context_length_error(response_text: str) -> bool:
    low = (response_text or "").lower()
    return "context length" in low or "input length exceeds" in low


def truncate_text_for_embedding(text: str, max_tokens: int, max_chars: int) -> Tuple[str, bool]:
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


def prepare_embed_items(
    texts: List[str],
    max_tokens: int,
    max_chars: int,
) -> Tuple[List[str], List[int], int, int]:
    prepared_texts: List[str] = []
    kept_indices: List[int] = []
    truncated_count = 0
    dropped_count = 0

    for idx, text in enumerate(texts):
        prepared, truncated = truncate_text_for_embedding(text, max_tokens=max_tokens, max_chars=max_chars)
        if not prepared:
            dropped_count += 1
            continue
        prepared_texts.append(prepared)
        kept_indices.append(idx)
        if truncated:
            truncated_count += 1

    return prepared_texts, kept_indices, truncated_count, dropped_count


def resolve_embed_model(embedder: Optional[str], default_embedder: str) -> str:
    if embedder and embedder.startswith("ollama:"):
        return embedder.split(":", 1)[1]
    model = embedder or os.getenv("OLLAMA_EMBED_MODEL", default_embedder)
    if ":" not in model:
        return f"{model}:latest"
    return model


def build_embedding_text(
    chunk_text: str,
    chunk_meta: Dict[str, Any],
    document: Any,
    source_item: Any,
    source: Any,
) -> str:
    title = (chunk_meta.get("title") or getattr(document, "title", "") or "").strip()
    source_uri = (chunk_meta.get("source_uri") or getattr(source_item, "uri", "") or "").strip()
    source_type = (chunk_meta.get("source_type") or getattr(source, "source_type", "text") or "text").strip()
    display_uri = (chunk_meta.get("display_uri") or getattr(source_item, "display_uri", "") or "").strip()

    context_lines = [
        f"Title: {title}" if title else "",
        f"Source: {display_uri or source_uri}" if (display_uri or source_uri) else "",
        f"Source Type: {source_type}" if source_type else "",
    ]
    context = "\n".join(line for line in context_lines if line)
    if not context:
        return chunk_text
    return f"{context}\n\n{chunk_text}".strip()


class EmbeddingService:
    """Encapsulates embedding API interaction and retry policy."""

    def __init__(
        self,
        default_embedder: str,
        default_chunk_tokens: int,
        default_max_chunk_chars: int,
    ):
        self.default_embedder = default_embedder
        self.default_chunk_tokens = default_chunk_tokens
        self.default_max_chunk_chars = default_max_chunk_chars

    def embed_texts(
        self,
        texts: List[str],
        model_name: str,
        ollama_url: str,
        batch_size: int,
        max_tokens: Optional[int],
        max_chars: Optional[int],
        logger,
    ) -> Tuple[List[List[float]], List[int]]:
        if not texts:
            return [], []

        try:
            import httpx
        except Exception as exc:
            raise EmbeddingError("Embedding requires httpx. Install with: pip install httpx") from exc

        url = ollama_url.rstrip("/") + "/api/embed"
        batch_size = max(1, batch_size)
        configured_max_tokens = int(os.getenv("LOCALWIKI_EMBED_MAX_TOKENS", "0"))
        default_max_tokens = _coalesce_positive_int(max_tokens, self.default_chunk_tokens)
        initial_max_tokens = configured_max_tokens if configured_max_tokens > 0 else default_max_tokens
        configured_max_chars = int(os.getenv("LOCALWIKI_EMBED_MAX_CHARS", "0"))
        initial_max_chars = configured_max_chars if configured_max_chars > 0 else _coalesce_positive_int(max_chars, self.default_max_chunk_chars)
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
                    prepared_texts, kept_local_indices, truncated_count, dropped_count = prepare_embed_items(
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
                    try:
                        response = client.post(url, json=payload)
                    except Exception as exc:
                        raise EmbeddingError(
                            f"Ollama embedding request transport failure (model={model_name}, cap_tokens={cap})"
                        ) from exc
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
                        raise EmbeddingError(
                            "Ollama embedding request failed "
                            f"(status={response.status_code}, model={model_name}, batch={len(prepared_texts)}, "
                            f"cap_tokens={cap}, cap_chars={cap_chars}): {detail}"
                        )

                    data = response.json()
                    embeddings = data.get("embeddings") or []
                    if len(embeddings) != len(prepared_texts):
                        raise EmbeddingError(
                            f"Embedding count mismatch from Ollama: expected {len(prepared_texts)} got {len(embeddings)}"
                        )

                    all_embeddings.extend(embeddings)
                    all_kept_indices.extend([batch_global_indices[idx] for idx in kept_local_indices])
                    succeeded = True
                    break

                if not succeeded:
                    raise EmbeddingError(
                        "Ollama embedding request failed after context retries "
                        f"(model={model_name}, attempted_token_caps={attempted_caps}): {last_error}"
                    )

        if len(all_embeddings) != len(all_kept_indices):
            raise EmbeddingError("Embedding alignment mismatch between vectors and source chunks")

        return all_embeddings, all_kept_indices
