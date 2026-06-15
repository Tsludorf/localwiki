#!/usr/bin/env python3
"""
management_quotes_combined.py

Combined single-file version of the FMP earnings transcript sync, topic taxonomy,
and management quote extraction pipeline.

Includes:
- Stage A: FMP earnings-call transcript discovery/fetch/upsert
- Topic taxonomy: 8 fixed management quote buckets
- Stage B: quote extraction through either OpenRouter or local Ollama
- Verbatim substring verification
- Speaker attribution using existing _fmp_speakers helpers when available
- DB upsert into fmp_management_quote
- Optional comparison report between prompt versions

Expected environment:
- NEON_CONNECTION_STRING or DATABASE_URL
- FMP_API_KEY or FMP_API_TOKEN for transcript sync
- OPENROUTER_API_KEY for OpenRouter provider
- OLLAMA_BASE_URL, optional, defaults to http://127.0.0.1:11434
- MANAGEMENT_QUOTES_LOCAL_MODEL, optional, defaults to qwen3.5:30b

Examples:
  python management_quotes_combined.py sync --ticker-id 1234 --years 5
  python management_quotes_combined.py extract --provider ollama --ticker-id 1234 --limit 3 --model qwen3.5:30b
  python management_quotes_combined.py extract --provider openrouter --ticker-id 1234 --model openai/gpt-5-mini
  python management_quotes_combined.py compare --ticker-id 1234 --local-prompt-version management_quotes_v1_ollama --cloud-prompt-version management_quotes_v1
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import traceback
import time
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from management_quotes_dsn_utils import is_management_quotes_placeholder_dsn

try:
    import psycopg
except ModuleNotFoundError:
    print("Missing dependency: psycopg. Install with: pip install psycopg", file=sys.stderr)
    raise
from dotenv import load_dotenv

# Optional project imports. Keep the combined file portable, but use existing
# project helpers when present.
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))

try:
    from core.fmp_ticker_map import bbg_to_fmp
except Exception:  # noqa: BLE001
    def bbg_to_fmp(ticker: str) -> str:
        return ticker.replace(" US", "").replace("-US", "").split()[0]

try:
    from _fmp_speakers import (
        find_turn_at_offset,
        load_exec_roles,
        parse_turns_with_spans,
        role_for_speaker,
        role_from_title,
        speaker_norm,
        speaker_title_from_turn_header,
    )
except Exception:  # noqa: BLE001
    @dataclass
    class _Turn:
        start: int
        end: int
        speaker: str | None = None

    def parse_turns_with_spans(content: str) -> list[_Turn]:
        return [_Turn(0, len(content), None)]

    def find_turn_at_offset(turns: list[_Turn], offset: int) -> _Turn | None:
        for t in turns:
            if t.start <= offset <= t.end:
                return t
        return None

    def load_exec_roles(conn: psycopg.Connection) -> dict[int, dict[str, set[str]]]:
        return {}

    def role_for_speaker(ticker_id: int, speaker: str, role_map: dict[int, dict[str, set[str]]]):
        return "OTHER", False, False

    def role_from_title(title: str):
        t = title.lower()
        is_ceo = "chief executive" in t or "ceo" in t
        is_cfo = "chief financial" in t or "cfo" in t
        return ("CEO" if is_ceo else "CFO" if is_cfo else "OTHER"), is_ceo, is_cfo

    def speaker_norm(speaker: str) -> str:
        return re.sub(r"\s+", " ", speaker.strip().lower())

    def speaker_title_from_turn_header(content: str, start: int) -> str | None:
        return None


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

STABLE_BASE = "https://financialmodelingprep.com/stable"
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
DEFAULT_OPENROUTER_MODEL = "anthropic/claude-sonnet-4.5"
DEFAULT_OLLAMA_MODEL = os.getenv("MANAGEMENT_QUOTES_LOCAL_MODEL", "qwen3.5:30b")
PROMPT_VERSION_CLOUD = "management_quotes_v1"
PROMPT_VERSION_OLLAMA = "management_quotes_v1_ollama"

_MODEL_PRICING_USD_PER_M: dict[str, tuple[float, float]] = {
    "anthropic/claude-sonnet-4.5": (3.0, 15.0),
    "anthropic/claude-3.5-sonnet": (3.0, 15.0),
    "openai/gpt-5":                (1.5, 6.0),
    "openai/gpt-5-mini":           (0.3, 1.2),
    "openai/gpt-4o":               (2.5, 10.0),
    "openai/gpt-4o-mini":          (0.15, 0.60),
    "google/gemini-2.5-flash":     (0.075, 0.30),
}


def project_root() -> Path:
    return Path(__file__).resolve().parent


def load_database_dsn_from_env() -> str:
    load_dotenv(project_root() / ".env")
    dsn = os.getenv("NEON_CONNECTION_STRING") or os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("Set NEON_CONNECTION_STRING or DATABASE_URL in .env")
    if is_management_quotes_placeholder_dsn(dsn):
        raise RuntimeError("Refusing unresolved DB host 'host' in connection string")
    return dsn


def load_fmp_api_key() -> str:
    load_dotenv(project_root() / ".env")
    key = (os.getenv("FMP_API_KEY") or os.getenv("FMP_API_TOKEN") or "").strip()
    if not key:
        raise RuntimeError("Set FMP_API_KEY or FMP_API_TOKEN in .env")
    return key


def load_openrouter_key() -> str:
    load_dotenv(project_root() / ".env")
    key = (os.getenv("OPENROUTER_API_KEY") or "").strip()
    if not key:
        raise RuntimeError("Set OPENROUTER_API_KEY in .env")
    return key


def qa_dir() -> Path:
    d = project_root() / "data" / "transcripts" / "qa"
    d.mkdir(parents=True, exist_ok=True)
    return d


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Topic taxonomy
# ---------------------------------------------------------------------------

TOPICS: list[tuple[str, str]] = [
    ("capital_allocation",    "Capital Allocation"),
    ("competitive_advantage", "Competitive Advantage"),
    ("growth",                "Growth"),
    ("competition",           "Competition"),
    ("operations",            "Operations"),
    ("financials",            "Financials"),
    ("risks_macro",           "Risks & Macro"),
    ("outlook_guidance",      "Outlook & Guidance"),
]

TOPIC_SLUGS: frozenset[str] = frozenset(slug for slug, _ in TOPICS)
TOPIC_LABEL: dict[str, str] = dict(TOPICS)

TOPIC_DEFINITIONS: dict[str, str] = {
    "capital_allocation": (
        "M&A philosophy, buybacks, dividends, debt paydown, R&D / capex allocation. "
        "Excludes pure financial results."
    ),
    "competitive_advantage": (
        "Durable structural advantages the company claims — product, scale, integration, moats. "
        "Quotes that say why they win."
    ),
    "growth": (
        "Volume, revenue, customer, vertical, or geographic expansion, recent or forward. "
        "Excludes pure guidance numbers."
    ),
    "competition": (
        "Named or unnamed competitors, market structure, share, pricing behavior, competitor moves."
    ),
    "operations": (
        "Internal execution — in-sourcing, AI initiatives, system upgrades, integration, "
        "headcount discipline, restructuring."
    ),
    "financials": (
        "Backward-looking quoted results — revenue, EBITDA, margins, FCF, leverage, balance sheet specifics."
    ),
    "risks_macro": (
        "Consumer, FX, regulatory, geopolitical, recessionary, demand risks discussed by management."
    ),
    "outlook_guidance": "Forward-looking ranges, targets, mid-term outlook.",
}


def topic_label(slug: str) -> str:
    return TOPIC_LABEL.get(slug, slug)


# ---------------------------------------------------------------------------
# Stage A: FMP transcript sync
# ---------------------------------------------------------------------------


def _period(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip().upper()
    if not s:
        return None
    if s.startswith("Q"):
        return s
    if s.isdigit():
        return f"Q{s}"
    return s


def _date(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s[:10] if s else None


def _text(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    if not s or s.lower() in {"nan", "none", "null"}:
        return None
    return s


@dataclass(frozen=True)
class BackfillCounts:
    ticker_id: int
    provider_symbol: str | None
    years_attempted: int
    existing_before: int
    discovered: int
    fetched: int
    pulled: int
    failed: int
    skipped_existing_with_content: int


def fmp_get_json(path: str, params: dict[str, Any], api_key: str, *, sleep_s: float = 0.5, max_retries: int = 4) -> Any:
    q = {k: v for k, v in params.items() if v is not None}
    q["apikey"] = api_key
    url = f"{STABLE_BASE}/{path.lstrip('/')}?{urlencode(q)}"
    last_err: Exception | None = None
    for attempt in range(max_retries):
        try:
            if sleep_s > 0:
                time.sleep(sleep_s)
            req = Request(url, headers={"User-Agent": "lhweb-fmp-loader/1.0"})
            with urlopen(req, timeout=120) as resp:
                raw = resp.read().decode("utf-8")
            data = json.loads(raw)
            if isinstance(data, dict) and data.get("Error Message"):
                raise RuntimeError(f"FMP error: {data['Error Message']}")
            return data
        except HTTPError as exc:
            last_err = exc
            if exc.code in (401, 403, 404):
                raise
            time.sleep(min(2 ** attempt, 8))
        except (URLError, json.JSONDecodeError, RuntimeError) as exc:
            last_err = exc
            time.sleep(min(2 ** attempt, 8))
    assert last_err is not None
    raise last_err


def resolve_provider_symbol(conn: psycopg.Connection, ticker_id: int) -> tuple[str | None, str | None]:
    with conn.cursor() as cur:
        cur.execute("SELECT ticker FROM company_master WHERE ticker_id = %s", (int(ticker_id),))
        row = cur.fetchone()
    if not row or not row[0]:
        return None, None
    bbg = str(row[0]).strip()
    with conn.cursor() as cur:
        cur.execute("SELECT provider_symbol FROM fmp_company WHERE ticker_id = %s", (int(ticker_id),))
        fmp_row = cur.fetchone()
    if fmp_row and fmp_row[0]:
        return bbg, str(fmp_row[0])
    return bbg, bbg_to_fmp(bbg)


def resolve_ticker_id(conn: psycopg.Connection, ticker_id: str) -> int:
    normalized = str(ticker_id or "").strip()
    if not normalized:
        raise ValueError("ticker_id is required.")

    try:
        return int(normalized)
    except (TypeError, ValueError):
        pass

    matches: list[int] = []
    with conn.cursor() as cur:
        cur.execute(
            "SELECT ticker_id FROM company_master WHERE LOWER(TRIM(ticker)) = LOWER(%s)",
            (normalized,),
        )
        for row in cur.fetchall():
            if row and row[0] is not None:
                matches.append(int(row[0]))

        if len(matches) > 1:
            raise ValueError(f"multiple company IDs found for ticker_id '{normalized}'.")
        if len(matches) == 1:
            return matches[0]

        cur.execute(
            "SELECT ticker_id FROM fmp_company WHERE LOWER(TRIM(provider_symbol)) = LOWER(%s)",
            (normalized,),
        )
        for row in cur.fetchall():
            if row and row[0] is not None:
                matches.append(int(row[0]))

    if len(matches) > 1:
        raise ValueError(f"multiple company IDs found for ticker_id '{normalized}'.")
    if len(matches) == 1:
        return matches[0]

    raise ValueError(f"ticker_id '{normalized}' not found.")


def existing_periods(conn: psycopg.Connection, ticker_id: int) -> dict[tuple[int, str], bool]:
    sql = """
    SELECT fiscal_year, fiscal_period,
           (content IS NOT NULL AND content <> '') AS has_content
    FROM fmp_transcript
    WHERE ticker_id = %s
    """
    out: dict[tuple[int, str], bool] = {}
    with conn.cursor() as cur:
        cur.execute(sql, (int(ticker_id),))
        for fy, fp, has_content in cur.fetchall():
            if fy is not None and fp:
                out[(int(fy), str(fp))] = bool(has_content)
    return out


def fetch_transcript_dates(provider_symbol: str, api_key: str, *, sleep_s: float = 0.5) -> list[dict[str, Any]]:
    data = fmp_get_json("earning-call-transcript-dates", {"symbol": provider_symbol}, api_key, sleep_s=sleep_s)
    return [item for item in data if isinstance(item, dict)] if isinstance(data, list) else []


def fetch_one_transcript(provider_symbol: str, year: int, quarter: int, api_key: str, *, sleep_s: float = 0.5) -> dict[str, Any] | None:
    data = fmp_get_json(
        "earning-call-transcript",
        {"symbol": provider_symbol, "year": year, "quarter": quarter},
        api_key,
        sleep_s=sleep_s,
    )
    if isinstance(data, list):
        return data[0] if data and isinstance(data[0], dict) else None
    return data if isinstance(data, dict) else None


def upsert_transcripts(conn: psycopg.Connection, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    sql = """
    INSERT INTO fmp_transcript (
        ticker_id, provider_symbol, fiscal_year, fiscal_period, call_date,
        content, content_sha256
    ) VALUES (
        %(ticker_id)s, %(provider_symbol)s, %(fiscal_year)s,
        %(fiscal_period)s, %(call_date)s, %(content)s, %(content_sha256)s
    )
    ON CONFLICT (ticker_id, fiscal_year, fiscal_period) DO UPDATE SET
        provider_symbol = EXCLUDED.provider_symbol,
        call_date = COALESCE(EXCLUDED.call_date, fmp_transcript.call_date),
        content = COALESCE(EXCLUDED.content, fmp_transcript.content),
        content_sha256 = COALESCE(EXCLUDED.content_sha256, fmp_transcript.content_sha256),
        loaded_at = now();
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
        return cur.rowcount or len(rows)


def write_qa_errors(rows: list[dict[str, Any]]) -> Path | None:
    if not rows:
        return None
    path = qa_dir() / "fmp_transcript_backfill_errors.csv"
    is_new = not path.exists()
    fieldnames = ["timestamp", "ticker_id", "provider_symbol", "year", "quarter", "reason"]
    with path.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if is_new:
            w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})
    return path


def backfill_company(conn: psycopg.Connection, ticker_id: int, *, years: int = 5, api_key: str | None = None, dry_run: bool = False, sleep_s: float = 0.5, today: date | None = None) -> BackfillCounts:
    today = today or date.today()
    _bbg, provider_symbol = resolve_provider_symbol(conn, ticker_id)
    existing = existing_periods(conn, ticker_id)
    existing_with_content = sum(1 for v in existing.values() if v)

    if not provider_symbol:
        return BackfillCounts(ticker_id, None, years, len(existing), 0, 0, 0, 0, existing_with_content)

    api_key = api_key or load_fmp_api_key()
    end_year = today.year
    start_year = end_year - years + 1
    errors: list[dict[str, Any]] = []
    dates_payload = fetch_transcript_dates(provider_symbol, api_key, sleep_s=sleep_s)

    targets: list[tuple[int, int, str | None]] = []
    for item in dates_payload:
        fy_raw = item.get("fiscalYear") or item.get("year")
        q_raw = item.get("quarter") or item.get("period")
        try:
            fy_int = int(fy_raw) if fy_raw is not None else None
        except (TypeError, ValueError):
            fy_int = None
        q_int: int | None = None
        if q_raw is not None:
            s = str(q_raw).strip().upper().lstrip("Q")
            if s.isdigit():
                q_int = int(s)
        if fy_int is None or q_int is None or not (1 <= q_int <= 4):
            continue
        if start_year <= fy_int <= end_year:
            targets.append((fy_int, q_int, _date(item.get("date"))))

    targets.sort(key=lambda t: (t[0], t[1]), reverse=True)
    fetched = 0
    pulled = 0
    rows_to_upsert: list[dict[str, Any]] = []

    for fy_int, q_int, call_date_str in targets:
        fiscal_period = f"Q{q_int}"
        if existing.get((fy_int, fiscal_period), False):
            continue
        try:
            item = fetch_one_transcript(provider_symbol, fy_int, q_int, api_key, sleep_s=sleep_s)
        except Exception as exc:  # noqa: BLE001
            errors.append({"timestamp": utc_now().isoformat(), "ticker_id": ticker_id, "provider_symbol": provider_symbol, "year": fy_int, "quarter": q_int, "reason": f"{type(exc).__name__}: {exc}"[:500]})
            continue
        if item is None:
            errors.append({"timestamp": utc_now().isoformat(), "ticker_id": ticker_id, "provider_symbol": provider_symbol, "year": fy_int, "quarter": q_int, "reason": "empty_payload"})
            continue

        content = _text(item.get("content"))
        payload_fy = item.get("year") or item.get("fiscalYear") or fy_int
        try:
            payload_fy_int = int(payload_fy)
        except (TypeError, ValueError):
            payload_fy_int = fy_int
        row = {
            "ticker_id": ticker_id,
            "provider_symbol": provider_symbol,
            "fiscal_year": payload_fy_int,
            "fiscal_period": _period(item.get("period") or item.get("quarter")) or fiscal_period,
            "call_date": _date(item.get("date")) or call_date_str,
            "content": content,
            "content_sha256": sha256(content.encode("utf-8")).hexdigest() if content else None,
        }
        fetched += 1
        if not dry_run:
            rows_to_upsert.append(row)

    if rows_to_upsert and not dry_run:
        pulled = upsert_transcripts(conn, rows_to_upsert)
        conn.commit()

    if errors and not dry_run:
        write_qa_errors(errors)

    return BackfillCounts(ticker_id, provider_symbol, years, len(existing), len(targets), fetched, 0 if dry_run else pulled, len(errors), existing_with_content)


# ---------------------------------------------------------------------------
# Stage B: Management quote extraction
# ---------------------------------------------------------------------------

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS fmp_management_quote (
    id                    BIGSERIAL PRIMARY KEY,
    transcript_id         BIGINT NOT NULL REFERENCES fmp_transcript (id) ON DELETE RESTRICT,
    ticker_id             BIGINT NOT NULL REFERENCES company_master (ticker_id) ON DELETE RESTRICT,
    topic                 TEXT NOT NULL,
    quote                 TEXT NOT NULL,
    quote_char_start      INTEGER NOT NULL,
    quote_char_end        INTEGER NOT NULL,
    verified_exact        BOOLEAN NOT NULL DEFAULT TRUE,
    speaker               TEXT NOT NULL,
    speaker_title         TEXT,
    speaker_norm          TEXT,
    speaker_role          TEXT NOT NULL DEFAULT 'OTHER',
    is_ceo                BOOLEAN NOT NULL DEFAULT FALSE,
    is_cfo                BOOLEAN NOT NULL DEFAULT FALSE,
    fiscal_year           INTEGER,
    fiscal_period         TEXT,
    call_date             DATE,
    source_content_sha256 TEXT,
    extracted_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    llm_model             TEXT,
    llm_prompt_version    TEXT,
    llm_reason            TEXT,
    CONSTRAINT fmp_management_quote_dedupe_key UNIQUE (transcript_id, topic, quote_char_start)
);
CREATE INDEX IF NOT EXISTS fmp_management_quote_ticker_date_idx ON fmp_management_quote (ticker_id, call_date DESC);
CREATE INDEX IF NOT EXISTS fmp_management_quote_ticker_topic_idx ON fmp_management_quote (ticker_id, topic, call_date DESC);
CREATE INDEX IF NOT EXISTS fmp_management_quote_transcript_idx ON fmp_management_quote (transcript_id);
CREATE INDEX IF NOT EXISTS fmp_management_quote_prompt_version_idx ON fmp_management_quote (llm_prompt_version);
"""

# Optional migration for side-by-side local/cloud benchmark. Only run manually
# if you want the same quote span to be stored under multiple prompt versions.
MIGRATE_DEDUPE_INCLUDE_PROMPT_VERSION_SQL = """
ALTER TABLE fmp_management_quote DROP CONSTRAINT IF EXISTS fmp_management_quote_dedupe_key;
ALTER TABLE fmp_management_quote
ADD CONSTRAINT fmp_management_quote_dedupe_key
UNIQUE (transcript_id, topic, quote_char_start, llm_prompt_version);
"""


def ensure_table(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)
    conn.commit()


def migrate_dedupe_include_prompt_version(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(MIGRATE_DEDUPE_INCLUDE_PROMPT_VERSION_SQL)
    conn.commit()


_NORMALIZE_TRANSLATE = str.maketrans({
    "\u00a0": " ", "\u2009": " ", "\u200a": " ", "\u200b": "",
    "\u200c": "", "\u200d": "", "\ufeff": "", "\u2018": "'",
    "\u2019": "'", "\u201c": '"', "\u201d": '"', "\u2013": "-",
    "\u2014": "-", "\u2026": "...",
})
_WHITESPACE_RUN = re.compile(r"\s+")


def normalize_for_match(s: str) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKC", s)
    s = s.translate(_NORMALIZE_TRANSLATE)
    s = _WHITESPACE_RUN.sub(" ", s)
    return s.strip()


@dataclass(frozen=True)
class VerifiedQuote:
    quote: str
    quote_char_start: int
    quote_char_end: int


def verify_quote_in_content(content: str, quote: str) -> VerifiedQuote | None:
    if not content or not quote:
        return None
    idx = content.find(quote)
    if idx >= 0:
        return VerifiedQuote(quote, idx, idx + len(quote))

    norm_content = normalize_for_match(content)
    norm_quote = normalize_for_match(quote)
    if not norm_quote or norm_content.find(norm_quote) < 0:
        return None

    head = quote.strip().split("\n", 1)[0][:80]
    if head:
        for variant in (head, head.replace("'", "\u2019"), head.replace('"', "\u201c")):
            idx = content.find(variant)
            if idx >= 0:
                tail_window = content[idx:idx + len(quote) + 128]
                words = quote.strip().split()
                tail = words[-1] if words else ""
                if tail:
                    tail_idx = tail_window.rfind(tail)
                    if tail_idx >= 0:
                        return VerifiedQuote(content[idx:idx + tail_idx + len(tail)], idx, idx + tail_idx + len(tail))
                return VerifiedQuote(content[idx:idx + len(quote)], idx, idx + len(quote))
    return None


def build_system_prompt() -> str:
    lines = [
        "You are an analyst pulling verbatim quotes from a single earnings-call",
        "or investor-event transcript, organized into eight fixed topic buckets.",
        "Your only job is to find the strongest 1-6 verbatim sentences per topic --",
        "fewer if there are no clean fits. Do not invent, summarize, paraphrase, or",
        "merge sentences.",
        "",
        "Topic definitions (you must use exactly these slugs):",
        "",
    ]
    for slug, _label in TOPICS:
        lines.append(f"- {slug}: {TOPIC_DEFINITIONS[slug]}")
    lines.extend([
        "",
        "HARD RULES:",
        "1. quote MUST be a contiguous verbatim substring of the transcript text.",
        "   Do not introduce ellipses or change punctuation, capitalization, or whitespace.",
        "2. quote MUST be one or two adjacent sentences. Never join sentences from different speaker turns.",
        "3. speaker MUST be the literal name of the person speaking that turn, as it appears in the transcript.",
        "4. Use only the supplied topic slugs. If a sentence does not cleanly fit one of the eight, omit it.",
        "5. Skip the Q&A section's analyst questions. Quote management answers when they contain substantive content.",
        "6. Skip operator boilerplate, safe-harbor statements, greetings, and meeting procedure language.",
        "7. Return ONLY valid JSON. No prose, no markdown fences.",
        "",
        "OUTPUT SCHEMA:",
        '{',
        '  "transcript_id": <int>,',
        '  "quotes": [',
        '    {',
        '      "topic": "<slug>",',
        '      "quote": "<verbatim substring>",',
        '      "speaker": "<full name>",',
        '      "speaker_title": "<title from transcript header, if visible>",',
        '      "reason": "<one short sentence why this fits the topic>"',
        '    }',
        '  ]',
        '}',
    ])
    return "\n".join(lines)


def build_user_prompt(transcript: dict[str, Any], prompt_version: str) -> str:
    return (
        f"PROMPT_VERSION={prompt_version}\n"
        f"transcript_id={transcript['id']}\n"
        f"company={transcript.get('short_name') or ''} ({transcript.get('ticker') or transcript.get('bloomberg_ticker') or ''})\n"
        f"fiscal={transcript.get('fiscal_year')} {transcript.get('fiscal_period')}\n"
        f"call_date={transcript.get('call_date') or ''}\n\n"
        f"TRANSCRIPT:\n{transcript.get('content') or ''}"
    )


def extract_json(text: str) -> Any:
    t = (text or "").strip()
    if t.startswith("```"):
        t = t.split("\n", 1)[1] if "\n" in t else t.lstrip("`")
        if t.endswith("```"):
            t = t.rsplit("```", 1)[0]
    return json.loads(t)


def estimate_cost_usd(model: str, in_tokens: int, out_tokens: int) -> float:
    in_rate, out_rate = _MODEL_PRICING_USD_PER_M.get(model, (0.0, 0.0))
    return round((in_tokens / 1_000_000.0) * in_rate + (out_tokens / 1_000_000.0) * out_rate, 4)


def openrouter_chat(api_key: str, model: str, messages: list[dict[str, str]], *, timeout: int = 600, max_retries: int = 3) -> dict[str, Any]:
    body = json.dumps({
        "model": model,
        "messages": messages,
        "temperature": 0,
        "response_format": {"type": "json_object"},
    }).encode("utf-8")
    last_err: Exception | None = None
    for attempt in range(max_retries):
        try:
            req = Request(
                OPENROUTER_URL,
                data=body,
                method="POST",
                headers={
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                    "HTTP-Referer": "https://github.com/lh-app",
                    "X-Title": "lh-app Management Quotes",
                },
            )
            with urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
            return json.loads(raw.decode("utf-8"))
        except HTTPError as exc:
            last_err = exc
            if exc.code in (401, 403):
                raise
            time.sleep(min(2 ** attempt, 12))
        except (URLError, json.JSONDecodeError) as exc:
            last_err = exc
            time.sleep(min(2 ** attempt, 12))
    assert last_err is not None
    raise last_err


def ollama_chat(model: str, messages: list[dict[str, str]], *, timeout: int = 1800) -> dict[str, Any]:
    base_url = os.getenv("OLLAMA_BASE_URL", "http://127.0.0.1:11434").rstrip("/")
    body = json.dumps({
        "model": model,
        "messages": messages,
        "stream": False,
        "format": "json",
        "options": {"temperature": 0},
    }).encode("utf-8")
    req = Request(
        f"{base_url}/api/chat",
        data=body,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    with urlopen(req, timeout=timeout) as resp:
        raw = json.loads(resp.read().decode("utf-8"))
    content = ((raw.get("message") or {}).get("content") or "").strip()
    return {
        "choices": [{"message": {"content": content}}],
        "usage": {
            "prompt_tokens": int(raw.get("prompt_eval_count") or 0),
            "completion_tokens": int(raw.get("eval_count") or 0),
        },
        "raw": raw,
    }


def provider_chat(provider: str, model: str, messages: list[dict[str, str]]) -> dict[str, Any]:
    if provider == "openrouter":
        return openrouter_chat(load_openrouter_key(), model, messages)
    if provider == "ollama":
        return ollama_chat(model, messages)
    raise ValueError(f"Unsupported provider: {provider}")


def fetch_transcripts_for_ticker(conn: psycopg.Connection, ticker_id: int, *, prompt_version: str, limit: int | None = None) -> list[dict[str, Any]]:
    sql = """
    SELECT
        t.id, t.ticker_id, t.provider_symbol, t.fiscal_year, t.fiscal_period,
        t.call_date, t.content, t.content_sha256,
        cm.ticker AS bloomberg_ticker, cm.short_name AS short_name
    FROM fmp_transcript t
    JOIN company_master cm ON cm.ticker_id = t.ticker_id
    WHERE t.ticker_id = %(ticker_id)s
      AND t.content IS NOT NULL
      AND t.content <> ''
      AND NOT EXISTS (
          SELECT 1 FROM fmp_management_quote q
          WHERE q.transcript_id = t.id
            AND q.llm_prompt_version = %(prompt_version)s
      )
    ORDER BY t.call_date ASC NULLS LAST, t.id ASC
    """
    params: dict[str, Any] = {"ticker_id": int(ticker_id), "prompt_version": prompt_version}
    if limit is not None:
        sql += " LIMIT %(limit)s"
        params["limit"] = int(limit)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [d.name for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def insert_quotes(conn: psycopg.Connection, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    sql = """
    INSERT INTO fmp_management_quote (
        transcript_id, ticker_id, topic, quote,
        quote_char_start, quote_char_end, verified_exact,
        speaker, speaker_title, speaker_norm, speaker_role,
        is_ceo, is_cfo,
        fiscal_year, fiscal_period, call_date,
        source_content_sha256, extracted_at,
        llm_model, llm_prompt_version, llm_reason
    ) VALUES (
        %(transcript_id)s, %(ticker_id)s, %(topic)s, %(quote)s,
        %(quote_char_start)s, %(quote_char_end)s, %(verified_exact)s,
        %(speaker)s, %(speaker_title)s, %(speaker_norm)s, %(speaker_role)s,
        %(is_ceo)s, %(is_cfo)s,
        %(fiscal_year)s, %(fiscal_period)s, %(call_date)s,
        %(source_content_sha256)s, now(),
        %(llm_model)s, %(llm_prompt_version)s, %(llm_reason)s
    )
    ON CONFLICT DO NOTHING
    """
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
        return cur.rowcount or 0


def write_unverified_qa(rows: list[dict[str, Any]]) -> Path | None:
    if not rows:
        return None
    path = qa_dir() / "fmp_management_quote_unverified.csv"
    is_new = not path.exists()
    fieldnames = [
        "timestamp", "provider", "model", "ticker_id", "transcript_id", "fiscal_year", "fiscal_period",
        "topic", "speaker", "reason_returned", "quote",
    ]
    with path.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        if is_new:
            w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})
    return path


@dataclass
class TranscriptResult:
    transcript_id: int
    processed: bool
    inserted: int
    unverified: int
    input_tokens: int
    output_tokens: int
    estimated_cost_usd: float
    error: str | None = None


def parse_model_response(resp: dict[str, Any]) -> Any:
    message = resp["choices"][0]["message"]["content"]
    return extract_json(message)


def extract_for_transcript(conn: psycopg.Connection, transcript: dict[str, Any], role_map: dict[int, dict[str, set[str]]], *, provider: str, model: str, prompt_version: str, sleep_s: float = 0.0) -> TranscriptResult:
    tid = int(transcript["id"])
    ticker_id = int(transcript["ticker_id"])
    content: str = transcript.get("content") or ""
    if not content.strip():
        return TranscriptResult(tid, False, 0, 0, 0, 0, 0.0, error="empty_content")

    turns = parse_turns_with_spans(content)
    messages = [
        {"role": "system", "content": build_system_prompt()},
        {"role": "user", "content": build_user_prompt(transcript, prompt_version)},
    ]
    if sleep_s > 0:
        time.sleep(sleep_s)

    resp = provider_chat(provider, model, messages)
    usage = resp.get("usage") or {}
    in_toks = int(usage.get("prompt_tokens") or 0)
    out_toks = int(usage.get("completion_tokens") or 0)
    cost = 0.0 if provider == "ollama" else estimate_cost_usd(model, in_toks, out_toks)

    try:
        parsed = parse_model_response(resp)
    except Exception:
        # One repair attempt: give the raw response back and demand valid JSON.
        raw_message = ((resp.get("choices") or [{}])[0].get("message") or {}).get("content") or ""
        repair_messages = [
            {"role": "system", "content": "Return only valid JSON matching the requested schema. No prose. No markdown fences."},
            {"role": "user", "content": raw_message},
        ]
        try:
            resp2 = provider_chat(provider, model, repair_messages)
            usage2 = resp2.get("usage") or {}
            in_toks += int(usage2.get("prompt_tokens") or 0)
            out_toks += int(usage2.get("completion_tokens") or 0)
            parsed = parse_model_response(resp2)
        except Exception as exc:  # noqa: BLE001
            return TranscriptResult(tid, False, 0, 0, in_toks, out_toks, cost, error=f"bad_json: {type(exc).__name__}: {exc}"[:500])

    quotes = parsed.get("quotes") if isinstance(parsed, dict) else parsed
    if not isinstance(quotes, list):
        return TranscriptResult(tid, False, 0, 0, in_toks, out_toks, cost, error="quotes_not_list")

    rows_to_insert: list[dict[str, Any]] = []
    unverified_rows: list[dict[str, Any]] = []
    seen_keys: set[tuple[str, int]] = set()

    for item in quotes:
        if not isinstance(item, dict):
            continue
        topic = str(item.get("topic") or "").strip()
        if topic not in TOPIC_SLUGS:
            continue
        quote_text = str(item.get("quote") or "").strip()
        if not quote_text:
            continue

        verified = verify_quote_in_content(content, quote_text)
        if verified is None:
            unverified_rows.append({
                "timestamp": utc_now().isoformat(), "provider": provider, "model": model,
                "ticker_id": ticker_id, "transcript_id": tid,
                "fiscal_year": transcript.get("fiscal_year"), "fiscal_period": transcript.get("fiscal_period"),
                "topic": topic, "speaker": str(item.get("speaker") or "")[:200],
                "reason_returned": str(item.get("reason") or "")[:500], "quote": quote_text,
            })
            continue

        key = (topic, verified.quote_char_start)
        if key in seen_keys:
            continue
        seen_keys.add(key)

        turn = find_turn_at_offset(turns, verified.quote_char_start)
        if turn is not None:
            speaker = getattr(turn, "speaker", None) or str(item.get("speaker") or "Unknown")
            title_from_header = speaker_title_from_turn_header(content, getattr(turn, "start", 0))
        else:
            speaker = str(item.get("speaker") or "Unknown")
            title_from_header = None

        speaker_title = title_from_header or (str(item.get("speaker_title") or "").strip() or None)
        role, is_ceo, is_cfo = role_for_speaker(ticker_id, speaker, role_map)
        if role == "OTHER" and speaker_title:
            role, is_ceo, is_cfo = role_from_title(speaker_title)

        rows_to_insert.append({
            "transcript_id": tid, "ticker_id": ticker_id, "topic": topic, "quote": verified.quote,
            "quote_char_start": verified.quote_char_start, "quote_char_end": verified.quote_char_end,
            "verified_exact": True, "speaker": speaker[:500],
            "speaker_title": (speaker_title or "")[:500] or None,
            "speaker_norm": speaker_norm(speaker) or None, "speaker_role": role,
            "is_ceo": is_ceo, "is_cfo": is_cfo,
            "fiscal_year": transcript.get("fiscal_year"), "fiscal_period": transcript.get("fiscal_period"),
            "call_date": transcript.get("call_date"), "source_content_sha256": transcript.get("content_sha256"),
            "llm_model": model, "llm_prompt_version": prompt_version,
            "llm_reason": (str(item.get("reason") or "").strip() or None),
        })

    inserted = insert_quotes(conn, rows_to_insert)
    if unverified_rows:
        write_unverified_qa(unverified_rows)
    return TranscriptResult(tid, True, inserted, len(unverified_rows), in_toks, out_toks, cost)


@dataclass
class ExtractionCounts:
    transcripts_total: int
    transcripts_processed: int
    transcripts_failed: int
    quotes_inserted: int
    quotes_dropped_unverified: int
    input_tokens: int
    output_tokens: int
    estimated_cost_usd: float
    elapsed_seconds: float


def run_extraction(conn: psycopg.Connection, ticker_id: int, *, provider: str, model: str, prompt_version: str, limit: int | None = None, sleep_s: float = 0.0) -> ExtractionCounts:
    start = time.time()
    transcripts = fetch_transcripts_for_ticker(conn, ticker_id, prompt_version=prompt_version, limit=limit)
    role_map = load_exec_roles(conn)
    counts = ExtractionCounts(len(transcripts), 0, 0, 0, 0, 0, 0, 0.0, 0.0)
    pending_commit = 0

    for t in transcripts:
        try:
            r = extract_for_transcript(conn, t, role_map, provider=provider, model=model, prompt_version=prompt_version, sleep_s=sleep_s)
        except Exception as exc:  # noqa: BLE001
            counts.transcripts_failed += 1
            print(json.dumps({"event": "transcript_failed", "transcript_id": t.get("id"), "error": f"{type(exc).__name__}: {exc}"[:500]}), file=sys.stderr)
            continue
        if r.processed:
            counts.transcripts_processed += 1
        else:
            counts.transcripts_failed += 1
            if r.error:
                print(json.dumps({"event": "transcript_failed", "transcript_id": r.transcript_id, "error": r.error}), file=sys.stderr)
        counts.quotes_inserted += r.inserted
        counts.quotes_dropped_unverified += r.unverified
        counts.input_tokens += r.input_tokens
        counts.output_tokens += r.output_tokens
        counts.estimated_cost_usd = round(counts.estimated_cost_usd + r.estimated_cost_usd, 4)

        pending_commit += 1
        if pending_commit >= 25:
            conn.commit()
            pending_commit = 0

    if transcripts:
        conn.commit()

    counts.elapsed_seconds = round(time.time() - start, 2)
    return counts


# ---------------------------------------------------------------------------
# Comparison reporting
# ---------------------------------------------------------------------------


def compare_prompt_versions(conn: psycopg.Connection, ticker_id: int, *, local_prompt_version: str, cloud_prompt_version: str, write_csv: bool = True) -> dict[str, Any]:
    sql = """
    SELECT transcript_id, topic, quote, quote_char_start, quote_char_end, speaker_norm, llm_prompt_version
    FROM fmp_management_quote
    WHERE ticker_id = %(ticker_id)s
      AND llm_prompt_version IN (%(local)s, %(cloud)s)
    ORDER BY transcript_id, topic, quote_char_start
    """
    with conn.cursor() as cur:
        cur.execute(sql, {"ticker_id": ticker_id, "local": local_prompt_version, "cloud": cloud_prompt_version})
        cols = [d.name for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]

    local = [r for r in rows if r["llm_prompt_version"] == local_prompt_version]
    cloud = [r for r in rows if r["llm_prompt_version"] == cloud_prompt_version]

    def by_topic(rs: list[dict[str, Any]]) -> dict[str, int]:
        out: dict[str, int] = {slug: 0 for slug, _ in TOPICS}
        for r in rs:
            out[r["topic"]] = out.get(r["topic"], 0) + 1
        return out

    cloud_exact = {(r["transcript_id"], r["topic"], normalize_for_match(r["quote"])) for r in cloud}
    exact_overlap = sum(1 for r in local if (r["transcript_id"], r["topic"], normalize_for_match(r["quote"])) in cloud_exact)

    near_overlap = 0
    speaker_matches = 0
    speaker_compared = 0
    csv_rows: list[dict[str, Any]] = []

    cloud_by_tx_topic: dict[tuple[int, str], list[dict[str, Any]]] = {}
    for r in cloud:
        cloud_by_tx_topic.setdefault((int(r["transcript_id"]), str(r["topic"])), []).append(r)

    for lr in local:
        candidates = cloud_by_tx_topic.get((int(lr["transcript_id"]), str(lr["topic"])), [])
        best = None
        best_intersection = 0
        for cr in candidates:
            s1, e1 = int(lr["quote_char_start"]), int(lr["quote_char_end"])
            s2, e2 = int(cr["quote_char_start"]), int(cr["quote_char_end"])
            inter = max(0, min(e1, e2) - max(s1, s2))
            if inter > best_intersection:
                best_intersection = inter
                best = cr
        matched = bool(best and best_intersection > 0)
        if matched:
            near_overlap += 1
            speaker_compared += 1
            if (lr.get("speaker_norm") or "") == (best.get("speaker_norm") or ""):
                speaker_matches += 1
        csv_rows.append({
            "transcript_id": lr["transcript_id"], "topic": lr["topic"], "local_start": lr["quote_char_start"],
            "local_end": lr["quote_char_end"], "near_overlap": matched,
            "best_cloud_start": best.get("quote_char_start") if best else "",
            "best_cloud_end": best.get("quote_char_end") if best else "",
            "speaker_match": ((lr.get("speaker_norm") or "") == (best.get("speaker_norm") or "")) if best else "",
            "local_quote": lr["quote"], "best_cloud_quote": best.get("quote") if best else "",
        })

    path = None
    if write_csv:
        path = qa_dir() / f"management_quote_local_benchmark_{ticker_id}.csv"
        with path.open("w", newline="", encoding="utf-8") as f:
            fieldnames = list(csv_rows[0].keys()) if csv_rows else ["transcript_id", "topic"]
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            for r in csv_rows:
                w.writerow(r)

    return {
        "ticker_id": ticker_id,
        "local_prompt_version": local_prompt_version,
        "cloud_prompt_version": cloud_prompt_version,
        "local_quote_count": len(local),
        "cloud_quote_count": len(cloud),
        "local_by_topic": by_topic(local),
        "cloud_by_topic": by_topic(cloud),
        "exact_quote_overlap_count": exact_overlap,
        "near_span_overlap_count": near_overlap,
        "speaker_match_rate_on_near_overlaps": round(speaker_matches / speaker_compared, 4) if speaker_compared else None,
        "csv_path": str(path) if path else None,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    try:
        parser = argparse.ArgumentParser(description=__doc__)
        sub = parser.add_subparsers(dest="command", required=True)

        p_sync = sub.add_parser("sync", help="Fetch FMP transcripts into fmp_transcript")
        p_sync.add_argument("--ticker-id", required=True)
        p_sync.add_argument("--years", type=int, default=5)
        p_sync.add_argument("--dry-run", action="store_true")
        p_sync.add_argument("--sleep", type=float, default=0.5)

        p_extract = sub.add_parser("extract", help="Extract management quotes")
        p_extract.add_argument("--ticker-id", required=True)
        p_extract.add_argument("--provider", choices=["openrouter", "ollama"], default="ollama")
        p_extract.add_argument("--model", default=None)
        p_extract.add_argument("--prompt-version", default=None)
        p_extract.add_argument("--limit", type=int, default=None)
        p_extract.add_argument("--sleep", type=float, default=0.0)
        p_extract.add_argument("--create-table", action="store_true")
        p_extract.add_argument("--migrate-dedupe-include-prompt-version", action="store_true")
        p_extract.add_argument("--dry-run", action="store_true")

        p_compare = sub.add_parser("compare", help="Compare local/cloud prompt versions")
        p_compare.add_argument("--ticker-id", required=True)
        p_compare.add_argument("--local-prompt-version", default=PROMPT_VERSION_OLLAMA)
        p_compare.add_argument("--cloud-prompt-version", default=PROMPT_VERSION_CLOUD)
        p_compare.add_argument("--no-csv", action="store_true")

        args = parser.parse_args(argv)
        dsn = load_database_dsn_from_env()

        with psycopg.connect(dsn) as conn:
            try:
                resolved_ticker_id = resolve_ticker_id(conn, str(args.ticker_id))
            except ValueError as exc:
                print(
                    json.dumps(
                        {
                            "command": args.command,
                            "error": str(exc),
                            "error_type": type(exc).__name__,
                            "ticker_id": args.ticker_id,
                            "traceback": traceback.format_exc(),
                        },
                        indent=2,
                    ),
                    file=sys.stderr,
                )
                return 2

            if args.command == "sync":
                counts = backfill_company(
                    conn,
                    resolved_ticker_id,
                    years=args.years,
                    dry_run=args.dry_run,
                    sleep_s=args.sleep,
                )
                print(json.dumps(counts.__dict__, indent=2, default=str))
                return 0

            if args.command == "extract":
                if args.create_table:
                    ensure_table(conn)
                if args.migrate_dedupe_include_prompt_version:
                    migrate_dedupe_include_prompt_version(conn)
                provider = args.provider
                model = args.model or (DEFAULT_OLLAMA_MODEL if provider == "ollama" else DEFAULT_OPENROUTER_MODEL)
                prompt_version = args.prompt_version or (PROMPT_VERSION_OLLAMA if provider == "ollama" else PROMPT_VERSION_CLOUD)
                if args.dry_run:
                    rows = fetch_transcripts_for_ticker(conn, resolved_ticker_id, prompt_version=prompt_version, limit=args.limit)
                    print(
                        json.dumps(
                            {
                                "ticker_id": resolved_ticker_id,
                                "provider": provider,
                                "model": model,
                                "prompt_version": prompt_version,
                                "transcripts_to_process": len(rows),
                                "dry_run": True,
                            },
                            indent=2,
                        )
                    )
                    return 0
                counts = run_extraction(
                    conn,
                    resolved_ticker_id,
                    provider=provider,
                    model=model,
                    prompt_version=prompt_version,
                    limit=args.limit,
                    sleep_s=args.sleep,
                )
                print(
                    json.dumps(
                        {
                            "ticker_id": resolved_ticker_id,
                            "provider": provider,
                            "model": model,
                            "prompt_version": prompt_version,
                            **counts.__dict__,
                        },
                        indent=2,
                        default=str,
                    )
                )
                return 0

            if args.command == "compare":
                report = compare_prompt_versions(
                    conn,
                    resolved_ticker_id,
                    local_prompt_version=args.local_prompt_version,
                    cloud_prompt_version=args.cloud_prompt_version,
                    write_csv=not args.no_csv,
                )
                print(json.dumps(report, indent=2, default=str))
                return 0

        return 0
    except Exception as exc:  # noqa: BLE001
        print(
            json.dumps(
                {
                    "command": list(argv) if argv is not None else sys.argv[1:],
                    "error": str(exc),
                    "error_type": type(exc).__name__,
                    "traceback": traceback.format_exc(),
                },
                indent=2,
            ),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
