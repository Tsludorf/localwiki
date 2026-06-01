from __future__ import annotations

import json
import base64
import importlib
import importlib.metadata
import importlib.util
import ipaddress
import os
import shutil
import socket
import sqlite3
import stat
import subprocess
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Lock
from typing import Any
from urllib.parse import urlparse

import requests
from flask import Flask, jsonify, render_template, request

app = Flask(__name__)

HOST = "127.0.0.1"
PORT = 3811
REQUEST_TIMEOUT_SECONDS = 5
PORT_SCAN_TIMEOUT_SECONDS = 0.35
ANYTHINGLLM_BASE_URL = "http://127.0.0.1:3001"
ANYTHINGLLM_DEFAULT_WORKSPACE = "default"

SERVICES: list[dict[str, Any]] = [
    {
        "name": "AnythingLLM",
        "priority": 1,
        "base_url": "http://127.0.0.1:3001",
        "checks": [
            {"label": "Ping", "url": "http://127.0.0.1:3001/api/ping"},
            {"label": "Home", "url": "http://127.0.0.1:3001/"},
        ],
    },
    {
        "name": "Ollama",
        "priority": 2,
        "base_url": "http://127.0.0.1:11434",
        "checks": [
            {"label": "Tags", "url": "http://127.0.0.1:11434/api/tags"},
            {"label": "Loaded models", "url": "http://127.0.0.1:11434/api/ps"},
        ],
    },
    {
        "name": "n8n",
        "priority": 3,
        "base_url": "http://127.0.0.1:5678",
        "checks": [
            {"label": "Home", "url": "http://127.0.0.1:5678/"},
            {"label": "REST health", "url": "http://127.0.0.1:5678/rest/healthz"},
        ],
    },
    {
        "name": "Qdrant",
        "priority": 4,
        "base_url": "http://127.0.0.1:6333",
        "checks": [
            {"label": "Readyz", "url": "http://127.0.0.1:6333/readyz"},
            {"label": "Collections", "url": "http://127.0.0.1:6333/collections"},
        ],
    },
]

SNAPSHOT_PATH = Path(__file__).resolve().parent / "data" / "ports_snapshot.json"
SERVICE_STATE: dict[str, dict[str, Any]] = {}
LOCALWIKI_ROOT = Path("/home/loc-llm/warlock_ingester")
LOCALWIKI_VENV_PYTHON = LOCALWIKI_ROOT / ".venv" / "bin" / "python"
LOCALWIKI_DB = LOCALWIKI_ROOT / "localwiki_registry.db"
LOCALWIKI_ALLOWED_COMMANDS = {
    "status": ["-m", "src.cli", "status"],
    "help": ["-m", "src.cli", "help"],
    "collections_list": ["-m", "src.cli", "collections", "list"],
    "troubleshoot": ["-m", "src.cli", "troubleshoot"],
}
LOCALWIKI_DEFAULT_SOURCES_PATH = str(Path("~/Desktop/wiki_sources").expanduser())
SERVICE_RESTART_COMMANDS: dict[str, str] = {
    "AnythingLLM": "docker restart ai-anythingllm || docker restart anythingllm || docker restart anything-llm || docker restart anything_llm",
    "Ollama": "docker restart ai-ollama || docker restart ollama || (pkill -f '[o]llama serve' || true ; nohup ollama serve >/tmp/kilo/ollama.log 2>&1 </dev/null &)",
    "n8n": "docker restart ai-n8n || docker restart n8n || docker restart n8n-main || (pkill -f '[n]8n start' || true ; nohup n8n start >/tmp/kilo/n8n.log 2>&1 </dev/null &)",
    "Qdrant": "docker restart ai-qdrant || docker restart qdrant || docker restart qdrant-main",
}
UI_RESTART_SCRIPT = LOCALWIKI_ROOT / "scripts" / "start-warlock-dashboard.sh"

FACTSET_STATE_PATH = LOCALWIKI_ROOT / "config" / "integrations" / "factset.json"
FACTSET_UPLOAD_DIR = LOCALWIKI_ROOT / "data" / "secrets" / "factset"
FACTSET_UPLOAD_PATH = FACTSET_UPLOAD_DIR / "app-config.json"
FACTSET_UPLOAD_MAX_BYTES = 2 * 1024 * 1024
FACTSET_SDK_RUN_MAX_BYTES = 256 * 1024
FACTSET_ALLOWED_BASE_DIRS = [
    Path("/secure/factset"),
    LOCALWIKI_ROOT / "data" / "secrets" / "factset",
]
IMAGER_ROOT = Path("/home/loc-llm/facefusion")
IMAGER_PYTHON = IMAGER_ROOT / ".venv" / "bin" / "python"
IMAGER_SCRIPT = IMAGER_ROOT / "facefusion.py"
IMAGER_SOURCE_DEFAULT = IMAGER_ROOT / "source_face.png"
IMAGER_TARGET_DEFAULT = IMAGER_ROOT / "input_video.mp4"
IMAGER_OUTPUT_DEFAULT = IMAGER_ROOT / "output_swapped_tuned.mp4"
IMAGER_OUTPUT_STABLE = IMAGER_ROOT / "output_stable.mp4"
IMAGER_OUTPUT_ENHANCED = IMAGER_ROOT / "output_enhanced.mp4"
IMAGER_OUTPUT_PADDING60 = IMAGER_ROOT / "output_padding60.mp4"
IMAGER_OUTPUT_MIGRAPHX = IMAGER_ROOT / "output_migraphx.mp4"
IMAGER_ALLOWED_PROVIDERS = {"cpu", "migraphx", "rocm"}
IMAGER_ALLOWED_MASK_TYPES = {"box", "occlusion"}
IMAGER_ALLOWED_PROCESSORS = {"face_swapper", "face_enhancer"}
IMAGER_ALLOWED_FACE_SWAPPER_MODELS = {"inswapper_128_fp16"}
IMAGER_ALLOWED_FACE_ENHANCER_MODELS = {"gfpgan_1.4"}
IMAGER_LOG_DIR = Path("/tmp/kilo")
IMAGER_INPUT_DIR = IMAGER_ROOT / "inputs"
IMAGER_TARGET_MAX_BYTES = 50 * 1024 * 1024
IMAGER_ALLOWED_VIDEO_EXTENSIONS = {".mp4", ".mov", ".m4v", ".mkv", ".webm", ".avi"}
IMAGER_EXTRACTOR_HOSTS = {"instagram.com", "www.instagram.com", "instagr.am"}
IMAGER_COOKIES_DIR = IMAGER_ROOT / "data" / "secrets" / "imager"
IMAGER_COOKIES_PATH = IMAGER_COOKIES_DIR / "instagram-cookies.txt"
IMAGER_COOKIES_MAX_BYTES = 2 * 1024 * 1024
IMAGER_STATE_LOCK = Lock()
IMAGER_STATE: dict[str, Any] = {
    "current": None,
    "last": None,
}


def _imager_variant_defaults(variant: str) -> dict[str, Any]:
    base = {
        "source_path": str(IMAGER_SOURCE_DEFAULT),
        "target_path": str(IMAGER_TARGET_DEFAULT),
        "execution_provider": "cpu",
        "face_selector_mode": "one",
        "face_selector_order": "best-worst",
        "face_mask_types": ["box", "occlusion"],
        "face_mask_padding": [40, 40, 40, 40],
        "processors": ["face_swapper"],
        "face_swapper_model": "inswapper_128_fp16",
        "face_enhancer_model": "gfpgan_1.4",
        "output_video_quality": 90,
    }
    v = (variant or "baseline").strip().lower()
    if v == "baseline":
        base["output_path"] = str(IMAGER_OUTPUT_DEFAULT)
    elif v == "stable":
        base["output_path"] = str(IMAGER_OUTPUT_STABLE)
    elif v == "enhanced":
        base["processors"] = ["face_swapper", "face_enhancer"]
        base["output_path"] = str(IMAGER_OUTPUT_ENHANCED)
    elif v == "padding60":
        base["face_mask_padding"] = [60, 60, 60, 60]
        base["output_path"] = str(IMAGER_OUTPUT_PADDING60)
    elif v == "migraphx":
        base["execution_provider"] = "migraphx"
        base["output_path"] = str(IMAGER_OUTPUT_MIGRAPHX)
    else:
        base["output_path"] = str(IMAGER_OUTPUT_DEFAULT)
    return base


def _coerce_padding(value: Any) -> tuple[list[int] | None, str | None]:
    if value is None:
        return None, None
    if not isinstance(value, list) or len(value) != 4:
        return None, "face_mask_padding must be a list of 4 integers."
    try:
        parsed = [int(v) for v in value]
    except Exception:
        return None, "face_mask_padding must contain integers."
    if any(v < 0 or v > 120 for v in parsed):
        return None, "face_mask_padding values must be between 0 and 120."
    return parsed, None


def _merge_imager_config(payload: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None]:
    variant = str(payload.get("variant") or "baseline").strip().lower()
    cfg = _imager_variant_defaults(variant)

    for key in ["source_path", "target_path", "output_path", "execution_provider", "face_selector_mode", "face_selector_order", "face_swapper_model", "face_enhancer_model"]:
        if payload.get(key) is not None:
            cfg[key] = str(payload.get(key)).strip()

    if payload.get("face_mask_types") is not None:
        if not isinstance(payload.get("face_mask_types"), list):
            return None, "face_mask_types must be a list."
        cfg["face_mask_types"] = [str(v).strip() for v in payload.get("face_mask_types", []) if str(v).strip()]

    if payload.get("processors") is not None:
        if not isinstance(payload.get("processors"), list):
            return None, "processors must be a list."
        cfg["processors"] = [str(v).strip() for v in payload.get("processors", []) if str(v).strip()]

    if payload.get("output_video_quality") is not None:
        try:
            cfg["output_video_quality"] = int(payload.get("output_video_quality"))
        except Exception:
            return None, "output_video_quality must be an integer."

    padding, padding_error = _coerce_padding(payload.get("face_mask_padding"))
    if padding_error:
        return None, padding_error
    if padding is not None:
        cfg["face_mask_padding"] = padding

    provider = str(cfg.get("execution_provider") or "cpu").lower()
    if provider not in IMAGER_ALLOWED_PROVIDERS:
        return None, "execution_provider must be one of: cpu, migraphx, rocm."
    cfg["execution_provider"] = provider

    mask_types = [str(v).lower() for v in cfg.get("face_mask_types") or []]
    if not mask_types:
        return None, "face_mask_types cannot be empty."
    if any(v not in IMAGER_ALLOWED_MASK_TYPES for v in mask_types):
        return None, "face_mask_types can only contain: box, occlusion."
    cfg["face_mask_types"] = mask_types

    processors = [str(v).lower() for v in cfg.get("processors") or []]
    if not processors:
        return None, "processors cannot be empty."
    if any(v not in IMAGER_ALLOWED_PROCESSORS for v in processors):
        return None, "processors can only contain: face_swapper, face_enhancer."
    cfg["processors"] = processors

    if str(cfg.get("face_swapper_model") or "") not in IMAGER_ALLOWED_FACE_SWAPPER_MODELS:
        return None, "face_swapper_model must be inswapper_128_fp16."
    if "face_enhancer" in processors and str(cfg.get("face_enhancer_model") or "") not in IMAGER_ALLOWED_FACE_ENHANCER_MODELS:
        return None, "face_enhancer_model must be gfpgan_1.4 when face_enhancer is enabled."

    quality = int(cfg.get("output_video_quality") or 0)
    if quality < 1 or quality > 100:
        return None, "output_video_quality must be between 1 and 100."

    for path_key in ["source_path", "target_path", "output_path"]:
        value = str(cfg.get(path_key) or "").strip()
        if not value:
            return None, f"{path_key} is required."
        cfg[path_key] = str(Path(value).expanduser())

    cfg["variant"] = variant
    return cfg, None


def _imager_command_from_config(cfg: dict[str, Any]) -> list[str]:
    cmd = [
        str(IMAGER_PYTHON),
        str(IMAGER_SCRIPT),
        "headless-run",
        "--source-path", cfg["source_path"],
        "--target-path", cfg["target_path"],
        "--output-path", cfg["output_path"],
        "--execution-providers", cfg["execution_provider"],
        "--face-selector-mode", cfg["face_selector_mode"],
        "--face-selector-order", cfg["face_selector_order"],
        "--face-mask-types", *cfg["face_mask_types"],
        "--face-mask-padding", *[str(v) for v in cfg["face_mask_padding"]],
        "--processors", *cfg["processors"],
        "--face-swapper-model", cfg["face_swapper_model"],
        "--output-video-quality", str(cfg["output_video_quality"]),
    ]
    if "face_enhancer" in cfg["processors"]:
        cmd.extend(["--face-enhancer-model", cfg["face_enhancer_model"]])
    return cmd


def _refresh_imager_state_locked() -> None:
    current = IMAGER_STATE.get("current")
    if not current:
        return
    proc: subprocess.Popen[str] | None = current.get("process")
    if proc is None:
        return
    exit_code = proc.poll()
    if exit_code is None:
        current["status"] = "running"
        return
    current["status"] = "success" if exit_code == 0 else "failed"
    current["exit_code"] = exit_code
    current["finished_at"] = now_iso()
    current.pop("process", None)
    IMAGER_STATE["last"] = current
    IMAGER_STATE["current"] = None


def _imager_status_payload() -> dict[str, Any]:
    with IMAGER_STATE_LOCK:
        _refresh_imager_state_locked()
        current = IMAGER_STATE.get("current")
        last = IMAGER_STATE.get("last")

        def _public_job(job: dict[str, Any] | None) -> dict[str, Any] | None:
            if not job:
                return None
            payload = {k: v for k, v in job.items() if k != "process"}
            return payload

        cookies_exists = IMAGER_COOKIES_PATH.exists() and IMAGER_COOKIES_PATH.is_file()
        cookies_size = None
        if cookies_exists:
            try:
                cookies_size = IMAGER_COOKIES_PATH.stat().st_size
            except Exception:
                cookies_size = None

        return {
            "checked_at": now_iso(),
            "facefusion_root": str(IMAGER_ROOT),
            "inputs_root": str(IMAGER_INPUT_DIR),
            "target_max_bytes": IMAGER_TARGET_MAX_BYTES,
            "cookies_path": str(IMAGER_COOKIES_PATH),
            "cookies_exists": cookies_exists,
            "cookies_size_bytes": cookies_size,
            "python_path": str(IMAGER_PYTHON),
            "script_path": str(IMAGER_SCRIPT),
            "python_exists": IMAGER_PYTHON.exists(),
            "script_exists": IMAGER_SCRIPT.exists(),
            "source_exists": Path(IMAGER_SOURCE_DEFAULT).exists(),
            "target_exists": Path(IMAGER_TARGET_DEFAULT).exists(),
            "current": _public_job(current),
            "last": _public_job(last),
        }


def _run_imager_debug_command(command: list[str], timeout: int = 30) -> dict[str, Any]:
    started = time.perf_counter()
    try:
        result = subprocess.run(
            command,
            cwd=str(IMAGER_ROOT),
            text=True,
            capture_output=True,
            timeout=timeout,
        )
        latency_ms = round((time.perf_counter() - started) * 1000, 2)
        return {
            "ok": result.returncode == 0,
            "exit_code": result.returncode,
            "stdout": clip_text(result.stdout, 8000),
            "stderr": clip_text(result.stderr, 8000),
            "latency_ms": latency_ms,
            "ran_at": now_iso(),
        }
    except Exception as exc:
        latency_ms = round((time.perf_counter() - started) * 1000, 2)
        return {
            "ok": False,
            "exit_code": None,
            "stdout": "",
            "stderr": repr(exc),
            "latency_ms": latency_ms,
            "ran_at": now_iso(),
        }


def _sanitize_upload_name(name: str) -> str:
    raw = Path(str(name or "video")).name
    cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in raw)
    cleaned = cleaned.strip("._") or "video"
    return cleaned


def _ensure_video_filename(name: str, fallback_ext: str = ".mp4") -> str:
    cleaned = _sanitize_upload_name(name)
    ext = Path(cleaned).suffix.lower()
    if ext not in IMAGER_ALLOWED_VIDEO_EXTENSIONS:
        cleaned = f"{Path(cleaned).stem or 'video'}{fallback_ext}"
    return cleaned


def _detect_extension_from_content_type(content_type: str | None) -> str:
    ctype = str(content_type or "").lower()
    if "mp4" in ctype:
        return ".mp4"
    if "quicktime" in ctype or "mov" in ctype:
        return ".mov"
    if "webm" in ctype:
        return ".webm"
    if "x-matroska" in ctype or "mkv" in ctype:
        return ".mkv"
    if "x-msvideo" in ctype or "avi" in ctype:
        return ".avi"
    return ".mp4"


def _is_public_http_host(hostname: str) -> tuple[bool, str | None]:
    host = str(hostname or "").strip().lower()
    if not host:
        return False, "Missing host in URL."
    if host in {"localhost", "localhost.localdomain"}:
        return False, "Localhost URLs are not allowed."

    try:
        addr_info = socket.getaddrinfo(host, None)
    except Exception:
        return False, "Unable to resolve URL host."

    if not addr_info:
        return False, "Unable to resolve URL host."

    seen: set[str] = set()
    for item in addr_info:
        sockaddr = item[4]
        if not sockaddr:
            continue
        ip_text = str(sockaddr[0])
        if ip_text in seen:
            continue
        seen.add(ip_text)
        try:
            ip_obj = ipaddress.ip_address(ip_text)
        except Exception:
            return False, "Resolved URL host is invalid."
        if not ip_obj.is_global:
            return False, f"Non-public URL host is not allowed: {ip_text}"

    if not seen:
        return False, "Unable to resolve URL host."

    return True, None


def _is_extractor_url(source_url: str) -> bool:
    parsed = urlparse(source_url)
    host = (parsed.netloc or "").lower()
    return any(host == item or host.endswith(f".{item}") for item in IMAGER_EXTRACTOR_HOSTS)


def _download_with_extractor(source_url: str) -> tuple[Path | None, dict[str, Any] | None]:
    ytdlp_bin = shutil.which("yt-dlp") or shutil.which("youtube-dl")
    if not ytdlp_bin:
        return None, {
            "ok": False,
            "error": "yt-dlp is not installed. Install yt-dlp to download from Instagram links.",
            "error_code": "IMAGER_EXTRACTOR_MISSING",
        }

    IMAGER_INPUT_DIR.mkdir(parents=True, exist_ok=True)
    start_ts = time.time()
    output_template = str(IMAGER_INPUT_DIR / "downloaded-%(epoch)s-%(id)s.%(ext)s")
    cmd = [
        ytdlp_bin,
        "--no-playlist",
        "--max-filesize", "50M",
        "--merge-output-format", "mp4",
        "--no-progress",
        "--restrict-filenames",
        "--print", "after_move:filepath",
        "-o", output_template,
        source_url,
    ]
    using_cookies = IMAGER_COOKIES_PATH.exists() and IMAGER_COOKIES_PATH.is_file()
    if using_cookies:
        cmd[1:1] = ["--cookies", str(IMAGER_COOKIES_PATH)]

    try:
        result = subprocess.run(
            cmd,
            cwd=str(IMAGER_ROOT),
            text=True,
            capture_output=True,
            timeout=600,
        )
    except Exception as exc:
        return None, {
            "ok": False,
            "error": repr(exc),
            "error_code": "IMAGER_EXTRACTOR_FAILED",
        }

    if result.returncode != 0:
        combined = clip_text(result.stderr or result.stdout or "Extractor failed.", 4000)
        lowered = combined.lower()
        if ("login required" in lowered or "rate-limit reached" in lowered or "requested content is not available" in lowered) and not using_cookies:
            return None, {
                "ok": False,
                "error": "Instagram requires authenticated cookies for this URL. Upload a cookies.txt export and retry.",
                "error_code": "IMAGER_EXTRACTOR_AUTH_REQUIRED",
                "extractor_output": combined,
                "exit_code": result.returncode,
            }
        if ("login required" in lowered or "rate-limit reached" in lowered or "requested content is not available" in lowered) and using_cookies:
            return None, {
                "ok": False,
                "error": "Instagram authentication failed with current cookies. Refresh cookies.txt and retry.",
                "error_code": "IMAGER_EXTRACTOR_AUTH_FAILED",
                "extractor_output": combined,
                "exit_code": result.returncode,
            }
        return None, {
            "ok": False,
            "error": combined,
            "error_code": "IMAGER_EXTRACTOR_FAILED",
            "exit_code": result.returncode,
        }

    output_path: Path | None = None
    for line in reversed((result.stdout or "").splitlines()):
        candidate = line.strip()
        if not candidate:
            continue
        path = Path(candidate)
        if path.exists() and path.is_file():
            output_path = path
            break

    if output_path is None:
        recent = [
            p for p in IMAGER_INPUT_DIR.glob("downloaded-*")
            if p.is_file() and p.stat().st_mtime >= (start_ts - 2)
        ]
        if recent:
            recent.sort(key=lambda p: p.stat().st_mtime, reverse=True)
            output_path = recent[0]

    if output_path is None or not output_path.exists() or not output_path.is_file():
        return None, {
            "ok": False,
            "error": "Extractor completed but output file was not found.",
            "error_code": "IMAGER_EXTRACTOR_OUTPUT_MISSING",
        }

    try:
        size_bytes = output_path.stat().st_size
    except Exception as exc:
        return None, {
            "ok": False,
            "error": repr(exc),
            "error_code": "IMAGER_EXTRACTOR_OUTPUT_INVALID",
        }

    if size_bytes > IMAGER_TARGET_MAX_BYTES:
        try:
            output_path.unlink(missing_ok=True)
        except Exception:
            pass
        return None, {
            "ok": False,
            "error": "Downloaded file exceeds 50MB limit.",
            "error_code": "IMAGER_TARGET_TOO_LARGE",
        }

    ext = output_path.suffix.lower()
    if ext not in IMAGER_ALLOWED_VIDEO_EXTENSIONS:
        return None, {
            "ok": False,
            "error": "Extractor did not produce a supported video format.",
            "error_code": "IMAGER_TARGET_URL_NOT_VIDEO",
        }

    return output_path, None
FACTSET_PACKAGES = [
    "fds.sdk.utils",
    "fds.sdk.FactSetEntity",
    "fds.sdk.FactSetFundamentals",
    "fds.sdk.FactSetEstimates",
    "fds.sdk.FactSetPrices",
    "fds.sdk.GlobalFilings",
    "fds.sdk.Formula",
    "fds.sdk.FactSetNER",
]
FACTSET_SDK_DOCS: list[dict[str, Any]] = [
    {
        "package": "fds.sdk.FactSetEntity",
        "label": "Entity",
        "homepage": "https://github.com/FactSet/enterprise-sdk/tree/main/code/python/FactSetEntity/v1",
        "operations": [
            {
                "api_class": "EntityReferenceApi",
                "operation_id": "get_entity_references",
                "method": "GET",
                "path": "/factset-entity/v1/entity-references",
                "description": "Entity reference profile for one or more ids.",
                "sample_query": {"ids": "AAPL-US,TSLA-US"},
            },
            {
                "api_class": "EntitySecuritiesApi",
                "operation_id": "get_entity_securities",
                "method": "GET",
                "path": "/factset-entity/v1/entity-securities",
                "description": "Equity listings and debt instruments for an entity.",
                "sample_query": {"ids": "AAPL-US"},
            },
            {
                "api_class": "EntityStructureApi",
                "operation_id": "get_ultimate_entity_structure",
                "method": "GET",
                "path": "/factset-entity/v1/ultimate-entity-structures",
                "description": "Ultimate parent hierarchy and control levels.",
                "sample_query": {"ids": "AAPL-US"},
            },
        ],
    },
    {
        "package": "fds.sdk.FactSetFundamentals",
        "label": "Fundamentals",
        "homepage": "https://github.com/FactSet/enterprise-sdk/tree/main/code/python/FactSetFundamentals/v2",
        "operations": [
            {
                "api_class": "CompanyReportsApi",
                "operation_id": "get_fundamentals",
                "method": "GET",
                "path": "/company-reports/fundamentals",
                "description": "Company fundamentals for a list of identifiers.",
                "sample_query": {"ids": "AAPL-US", "metrics": "FF_SALES,FF_EPS"},
            },
            {
                "api_class": "FactSetFundamentalsApi",
                "operation_id": "get_fds_fundamentals_for_list",
                "method": "POST",
                "path": "/fundamentals",
                "description": "Bulk fundamentals retrieval via POST payload.",
                "sample_body": {"ids": ["AAPL-US", "MSFT-US"], "metrics": ["FF_SALES", "FF_EPS"]},
            },
            {
                "api_class": "FundamentalsPointInTimeApi",
                "operation_id": "post_fundamentals_pit_data",
                "method": "POST",
                "path": "/point-in-time",
                "description": "Point-in-time fundamentals for historical testing.",
                "sample_body": {"ids": ["AAPL-US"], "metrics": ["FF_EPS"], "asOfDate": "2025-12-31"},
            },
        ],
    },
    {
        "package": "fds.sdk.FactSetEstimates",
        "label": "Estimates",
        "homepage": "https://github.com/FactSet/enterprise-sdk/tree/main/code/python/FactSetEstimates/v2",
        "operations": [
            {
                "api_class": "ActualsApi",
                "operation_id": "get_actuals",
                "method": "GET",
                "path": "/factset-estimates/v2/actuals",
                "description": "Reported actual values for ids and fiscal periods.",
                "sample_query": {"ids": "AAPL-US", "metrics": "EPS", "fiscalPeriod": "2024Q4"},
            },
            {
                "api_class": "ConsensusApi",
                "operation_id": "get_fixed_consensus",
                "method": "GET",
                "path": "/factset-estimates/v2/fixed-consensus",
                "description": "Consensus estimates for fixed fiscal periods.",
                "sample_query": {"ids": "AAPL-US", "metrics": "EPS", "periodicity": "ANN"},
            },
            {
                "api_class": "BrokerDetailApi",
                "operation_id": "get_rolling_detail",
                "method": "GET",
                "path": "/factset-estimates/v2/rolling-detail",
                "description": "Broker-level estimate details for rolling periods.",
                "sample_query": {"ids": "AAPL-US", "metrics": "EPS"},
            },
        ],
    },
    {
        "package": "fds.sdk.FactSetPrices",
        "label": "Prices",
        "homepage": "https://github.com/FactSet/enterprise-sdk/tree/main/code/python/FactSetPrices/v1",
        "operations": [
            {
                "api_class": "PricesApi",
                "operation_id": "get_security_prices",
                "method": "GET",
                "path": "/factset-prices/v1/prices",
                "description": "Security price history for a date range.",
                "sample_query": {"ids": "AAPL-US", "startDate": "2025-01-01", "endDate": "2025-01-31"},
            },
            {
                "api_class": "DividendsApi",
                "operation_id": "get_security_dividends",
                "method": "GET",
                "path": "/factset-prices/v1/dividends",
                "description": "Dividend events for selected securities.",
                "sample_query": {"ids": "AAPL-US", "startDate": "2024-01-01", "endDate": "2025-01-01"},
            },
            {
                "api_class": "SplitsApi",
                "operation_id": "get_security_splits",
                "method": "GET",
                "path": "/factset-prices/v1/splits",
                "description": "Split history to validate adjusted-price workflows.",
                "sample_query": {"ids": "AAPL-US", "startDate": "2019-01-01", "endDate": "2026-01-01"},
            },
        ],
    },
    {
        "package": "fds.sdk.GlobalFilings",
        "label": "Global Filings",
        "homepage": "https://github.com/FactSet/enterprise-sdk/tree/main/code/python/GlobalFilings/v2",
        "operations": [
            {
                "api_class": "FilingsAPIApi",
                "operation_id": "get_filings",
                "method": "GET",
                "path": "/search",
                "description": "Search filing documents and metadata.",
                "sample_query": {"source": "EDGAR", "query": "10-K AAPL"},
            },
            {
                "api_class": "FilingsAPIApi",
                "operation_id": "get_count",
                "method": "GET",
                "path": "/count",
                "description": "Count filings for query planning.",
                "sample_query": {"source": "EDGAR", "query": "10-Q MSFT"},
            },
            {
                "api_class": "MetaApi",
                "operation_id": "get_sources",
                "method": "GET",
                "path": "/meta/sources",
                "description": "List available filing sources.",
                "sample_query": {},
            },
        ],
    },
    {
        "package": "fds.sdk.Formula",
        "label": "Formula",
        "homepage": "https://github.com/FactSet/enterprise-sdk/tree/main/code/python/Formula/v1",
        "operations": [
            {
                "api_class": "CrossSectionalApi",
                "operation_id": "get_cross_sectional_data_for_list",
                "method": "POST",
                "path": "/cross-sectional",
                "description": "Cross-sectional formula screen outputs.",
                "sample_body": {"ids": ["AAPL-US", "MSFT-US"], "formulas": ["FF_SALES(ANN_R,0)"]},
            },
            {
                "api_class": "TimeSeriesApi",
                "operation_id": "get_time_series_data_for_list",
                "method": "POST",
                "path": "/time-series",
                "description": "Time-series formula outputs for charting/backtests.",
                "sample_body": {"ids": ["AAPL-US"], "formulas": ["P_PRICE"], "calendar": "FIVEDAY"},
            },
            {
                "api_class": "BatchProcessingApi",
                "operation_id": "get_batch_status",
                "method": "GET",
                "path": "/batch-status",
                "description": "Track asynchronous Formula batch jobs.",
                "sample_query": {"batchId": "replace-with-batch-id"},
            },
        ],
    },
    {
        "package": "fds.sdk.FactSetNER",
        "label": "NER",
        "homepage": "https://github.com/FactSet/enterprise-sdk/tree/main/code/python/FactSetNER/v2",
        "operations": [
            {
                "api_class": "EntitiesApi",
                "operation_id": "post_entities_entities",
                "method": "POST",
                "path": "/cognitive/ner/v2/entities",
                "description": "Extract entities from text using FactSet NER.",
                "sample_body": {"data": [{"id": "doc-1", "text": "Apple and Microsoft reported earnings."}]},
            },
        ],
    },
]


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def clip_text(value: str, limit: int = 450) -> str:
    if len(value) <= limit:
        return value
    return value[:limit] + " ...<truncated>"


def parse_response_text(response: requests.Response) -> Any:
    content_type = (response.headers.get("Content-Type") or "").lower()
    if "application/json" in content_type:
        try:
            return response.json()
        except Exception:
            return clip_text(response.text)
    return clip_text(response.text)


def check_endpoint(label: str, url: str) -> dict[str, Any]:
    started = time.perf_counter()
    try:
        response = requests.get(url, timeout=REQUEST_TIMEOUT_SECONDS)
        elapsed_ms = round((time.perf_counter() - started) * 1000, 2)
        body = parse_response_text(response)
        return {
            "label": label,
            "url": url,
            "ok": response.ok,
            "status_code": response.status_code,
            "latency_ms": elapsed_ms,
            "content_type": response.headers.get("Content-Type"),
            "body": body,
            "error": None,
        }
    except Exception as exc:
        elapsed_ms = round((time.perf_counter() - started) * 1000, 2)
        return {
            "label": label,
            "url": url,
            "ok": False,
            "status_code": None,
            "latency_ms": elapsed_ms,
            "content_type": None,
            "body": None,
            "error": repr(exc),
        }


def collect_ollama_extra() -> dict[str, Any]:
    result: dict[str, Any] = {"installed_models": [], "loaded_models": [], "errors": []}

    tags = check_endpoint("Tags", "http://127.0.0.1:11434/api/tags")
    if tags.get("ok") and isinstance(tags.get("body"), dict):
        models = tags["body"].get("models") or []
        result["installed_models"] = [
            {
                "name": m.get("name"),
                "size": m.get("size"),
                "family": (m.get("details") or {}).get("family"),
                "params": (m.get("details") or {}).get("parameter_size"),
                "quant": (m.get("details") or {}).get("quantization_level"),
            }
            for m in models
        ]
    elif tags.get("error"):
        result["errors"].append(tags["error"])

    ps = check_endpoint("Loaded", "http://127.0.0.1:11434/api/ps")
    if ps.get("ok") and isinstance(ps.get("body"), dict):
        models = ps["body"].get("models") or []
        result["loaded_models"] = [
            {
                "name": m.get("name"),
                "size_vram": m.get("size_vram"),
                "context_length": m.get("context_length"),
                "expires_at": m.get("expires_at"),
            }
            for m in models
        ]
    elif ps.get("error"):
        result["errors"].append(ps["error"])

    return result


def probe_services() -> list[dict[str, Any]]:
    probed: list[dict[str, Any]] = []
    ordered = sorted(SERVICES, key=lambda item: item["priority"])
    now_ts = time.time()

    for service in ordered:
        checks = [check_endpoint(c["label"], c["url"]) for c in service["checks"]]
        any_ok = any(check["ok"] for check in checks)
        status = "up" if any_ok else "down"

        state = SERVICE_STATE.get(service["name"], {})
        previous_status = state.get("status")
        up_since = state.get("up_since")
        last_change = state.get("last_change")

        if status != previous_status:
            last_change = now_ts
            if status == "up":
                up_since = now_ts
            else:
                up_since = None
        elif status == "up" and up_since is None:
            up_since = now_ts

        SERVICE_STATE[service["name"]] = {
            "status": status,
            "up_since": up_since,
            "last_change": last_change,
        }

        uptime_seconds = int(max(0, now_ts - up_since)) if up_since else 0
        parsed = urlparse(service["base_url"])
        service_port = parsed.port

        service_result: dict[str, Any] = {
            "name": service["name"],
            "priority": service["priority"],
            "base_url": service["base_url"],
            "port": service_port,
            "status": status,
            "uptime_seconds": uptime_seconds,
            "up_since": datetime.fromtimestamp(up_since, tz=timezone.utc).isoformat() if up_since else None,
            "last_change": datetime.fromtimestamp(last_change, tz=timezone.utc).isoformat() if last_change else None,
            "checks": checks,
            "all_errors": [c["error"] for c in checks if c["error"]],
        }

        if service["name"] == "Ollama":
            service_result["extra"] = collect_ollama_extra()

        probed.append(service_result)

    return probed


def list_open_ports() -> list[dict[str, Any]]:
    ports: list[dict[str, Any]] = []
    for port in [3001, 3811, 5678, 6333, 11434]:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(PORT_SCAN_TIMEOUT_SECONDS)
        start = time.perf_counter()
        try:
            rc = s.connect_ex(("127.0.0.1", port))
            latency_ms = round((time.perf_counter() - start) * 1000, 2)
            ports.append(
                {
                    "host": "127.0.0.1",
                    "port": port,
                    "open": rc == 0,
                    "latency_ms": latency_ms,
                }
            )
        finally:
            s.close()

    # Also include quick system-wide listening socket list for expansion
    try:
        cmd = ["bash", "-lc", "ss -ltnH | awk '{print $4}'"]
        out = subprocess.check_output(cmd, text=True, timeout=3)
        listeners = sorted(set(line.strip() for line in out.splitlines() if line.strip()))
    except Exception as exc:
        listeners = [f"error: {exc!r}"]

    ports.append({"system_listeners": listeners})
    return ports


def write_backup_snapshot(data: dict[str, Any]) -> None:
    SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)
    SNAPSHOT_PATH.write_text(json.dumps(data, indent=2), encoding="utf-8")


def _factset_default_state() -> dict[str, Any]:
    return {
        "auth_method": "oauth2_client_credentials",
        "config_source": "not_set",
        "config_path": None,
        "uploaded_filename": None,
        "last_test_status": "never_tested",
        "last_tested_at": None,
        "token_expires_at": None,
        "last_error_summary": None,
    }


def _load_factset_state() -> dict[str, Any]:
    if not FACTSET_STATE_PATH.exists():
        return _factset_default_state()
    try:
        loaded = json.loads(FACTSET_STATE_PATH.read_text(encoding="utf-8"))
        merged = _factset_default_state()
        merged.update(loaded)
        return merged
    except Exception:
        state = _factset_default_state()
        state["last_test_status"] = "failed"
        state["last_error_summary"] = "FactSet integration state file is invalid."
        return state


def _save_factset_state(state: dict[str, Any]) -> None:
    FACTSET_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    FACTSET_STATE_PATH.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")


def _resolve_factset_config(state: dict[str, Any]) -> tuple[str | None, str]:
    if state.get("config_path"):
        return str(state.get("config_path")), str(state.get("config_source") or "path")
    env_path = os.getenv("FACTSET_APP_CONFIG_PATH", "").strip()
    if env_path:
        return env_path, "environment_variable"
    return None, "not_set"


def _package_status(name: str) -> dict[str, Any]:
    try:
        spec = importlib.util.find_spec(name)
    except ModuleNotFoundError:
        spec = None
    if spec is None:
        return {"installed": False, "version": None}
    try:
        version = importlib.metadata.version(name)
    except Exception:
        version = "installed"
    return {"installed": True, "version": version}


def _factset_sdk_status() -> dict[str, dict[str, Any]]:
    return {name: _package_status(name) for name in FACTSET_PACKAGES}


def _parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def _factset_token_status(expires_at: Any) -> str:
    dt = _parse_iso(expires_at)
    if dt is None:
        return "unknown"
    return "active" if dt > datetime.now(timezone.utc) else "expired"


def _sanitize_factset_error(exc: Exception) -> str:
    msg = str(exc) or exc.__class__.__name__
    lowered = msg.lower()
    for blocked in ["private key", "client_secret", "access_token", "refresh_token", "jwk", "passphrase"]:
        if blocked in lowered:
            return "Invalid FactSet OAuth configuration."
    if "-----begin" in lowered and "key-----" in lowered:
        return "Invalid FactSet OAuth configuration."
    return clip_text(msg, 240)


def _is_allowed_factset_config_path(path: Path) -> bool:
    enforce_scope = os.getenv("FACTSET_ENFORCE_PATH_SCOPE", "0").strip() == "1"
    if not enforce_scope:
        return True

    try:
        resolved = path.resolve(strict=False)
    except Exception:
        return False

    for base in FACTSET_ALLOWED_BASE_DIRS:
        try:
            resolved.relative_to(base.resolve(strict=False))
            return True
        except ValueError:
            continue
    return False


def _decode_jwt_exp(token: str) -> str | None:
    parts = token.split(".")
    if len(parts) < 2:
        return None
    payload = parts[1] + "=" * ((4 - len(parts[1]) % 4) % 4)
    try:
        decoded = base64.urlsafe_b64decode(payload.encode("utf-8"))
        payload_obj = json.loads(decoded.decode("utf-8"))
        exp = payload_obj.get("exp")
        if exp is None:
            return None
        return datetime.fromtimestamp(int(exp), tz=timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    except Exception:
        return None


def _extract_token_expiry(token_obj: Any) -> str | None:
    if token_obj is None:
        return None
    if isinstance(token_obj, dict):
        if token_obj.get("expires_at"):
            return str(token_obj.get("expires_at"))
        if token_obj.get("expiration_time"):
            return str(token_obj.get("expiration_time"))
        if token_obj.get("expires_in"):
            try:
                return (datetime.now(timezone.utc) + timedelta(seconds=int(token_obj.get("expires_in")))).replace(microsecond=0).isoformat().replace("+00:00", "Z")
            except Exception:
                return None
        access_token = token_obj.get("access_token")
        if isinstance(access_token, str):
            return _decode_jwt_exp(access_token)
        return None
    if isinstance(token_obj, str):
        return _decode_jwt_exp(token_obj)
    if hasattr(token_obj, "expires_at"):
        return str(getattr(token_obj, "expires_at"))
    if hasattr(token_obj, "expiration_time"):
        return str(getattr(token_obj, "expiration_time"))
    return None


def get_factset_status_payload() -> dict[str, Any]:
    state = _load_factset_state()
    config_path, config_source = _resolve_factset_config(state)
    sdk = _factset_sdk_status()
    sdk_installed = all(item.get("installed") for item in sdk.values())
    return {
        "configured": bool(config_path),
        "auth_method": "oauth2_client_credentials",
        "config_source": config_source,
        "config_path_display": config_path,
        "uploaded_filename": state.get("uploaded_filename"),
        "last_test_status": state.get("last_test_status", "never_tested"),
        "last_tested_at": state.get("last_tested_at"),
        "token_status": _factset_token_status(state.get("token_expires_at")),
        "token_expires_at": state.get("token_expires_at"),
        "last_error_summary": state.get("last_error_summary"),
        "sdk_utilities_installed": sdk_installed,
        "sdk": sdk,
    }


def get_factset_sdk_catalog_payload() -> dict[str, Any]:
    package_status = _factset_sdk_status()
    packages: list[dict[str, Any]] = []
    operations_total = 0

    for pkg in FACTSET_SDK_DOCS:
        pkg_name = str(pkg.get("package") or "")
        status = package_status.get(pkg_name, {"installed": False, "version": None})
        operations: list[dict[str, Any]] = []

        for op in pkg.get("operations", []):
            operations_total += 1
            method = str(op.get("method") or "GET")
            path = str(op.get("path") or "")
            operation_id = str(op.get("operation_id") or "")
            operation_key = f"{pkg_name}::{operation_id}::{method.upper()}::{path}"
            sample_query = op.get("sample_query")
            sample_body = op.get("sample_body")
            operations.append(
                {
                    "operation_key": operation_key,
                    "api_class": op.get("api_class"),
                    "operation_id": operation_id,
                    "method": method,
                    "path": path,
                    "description": op.get("description"),
                    "sample_query": sample_query,
                    "sample_body": sample_body,
                    "test_stub": {
                        "name": f"factset_{op.get('operation_id')}",
                        "package": pkg_name,
                        "request": {
                            "method": method,
                            "path": op.get("path"),
                            "query": sample_query or {},
                            "body": sample_body,
                        },
                        "assertions": [
                            "status_code in [200, 202]",
                            "response has non-empty data payload",
                        ],
                        "notes": [
                            "Requires valid FactSet OAuth credentials.",
                            "Replace placeholder ids and dates before running.",
                        ],
                    },
                }
            )

        packages.append(
            {
                "package": pkg_name,
                "label": pkg.get("label"),
                "homepage": pkg.get("homepage"),
                "installed": bool(status.get("installed")),
                "version": status.get("version"),
                "operations": operations,
            }
        )

    installed_count = sum(1 for pkg in packages if pkg.get("installed"))
    return {
        "generated_at": now_iso(),
        "packages_total": len(packages),
        "packages_installed": installed_count,
        "operations_total": operations_total,
        "packages": packages,
    }


def _find_factset_sdk_operation(operation_key: str) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    for pkg in FACTSET_SDK_DOCS:
        pkg_name = str(pkg.get("package") or "")
        for op in pkg.get("operations", []):
            method = str(op.get("method") or "GET").upper()
            path = str(op.get("path") or "")
            op_id = str(op.get("operation_id") or "")
            key = f"{pkg_name}::{op_id}::{method}::{path}"
            if key == operation_key:
                return pkg, op
    return None, None


def _resolve_factset_access_token() -> tuple[str | None, dict[str, Any] | None]:
    state = _load_factset_state()
    config_path, _config_source = _resolve_factset_config(state)
    if not config_path:
        return None, {
            "error": "FactSet config path is not set. Configure OAuth first.",
            "error_code": "FACTSET_CONFIG_NOT_SET",
            "failure_class": "client",
        }

    path = Path(config_path).expanduser()
    if not path.exists() or not path.is_file() or not os.access(path, os.R_OK):
        return None, {
            "error": "FactSet config path is missing or unreadable.",
            "error_code": "FACTSET_CONFIG_UNREADABLE",
            "failure_class": "client",
        }

    try:
        auth_mod = importlib.import_module("fds.sdk.utils.authentication")
    except Exception:
        return None, {
            "error": "FactSet SDK utilities are not installed.",
            "error_code": "FACTSET_SDK_UTILS_MISSING",
            "failure_class": "client",
        }

    try:
        client = auth_mod.ConfidentialClient(str(path))
        token_obj = client.get_access_token()
    except Exception as exc:
        return None, {
            "error": _sanitize_factset_error(exc),
            "error_code": "FACTSET_OAUTH_FAILED",
            "failure_class": "client",
        }

    if isinstance(token_obj, str):
        token = token_obj.strip()
        return (token or None), (None if token else {
            "error": "FactSet token response was empty.",
            "error_code": "FACTSET_OAUTH_EMPTY_TOKEN",
            "failure_class": "client",
        })

    if isinstance(token_obj, dict):
        token = str(token_obj.get("access_token") or "").strip()
        return (token or None), (None if token else {
            "error": "FactSet token response did not include access_token.",
            "error_code": "FACTSET_OAUTH_EMPTY_TOKEN",
            "failure_class": "client",
        })

    token = str(token_obj or "").strip()
    return (token or None), (None if token else {
        "error": "FactSet token response was empty.",
        "error_code": "FACTSET_OAUTH_EMPTY_TOKEN",
        "failure_class": "client",
    })


def run_factset_sdk_sample(operation_key: str, query: dict[str, Any], body: Any) -> dict[str, Any]:
    pkg, op = _find_factset_sdk_operation(operation_key)
    if not pkg or not op:
        return {
            "ok": False,
            "error": "Unsupported operation key.",
            "error_code": "FACTSET_SDK_RUN_UNSUPPORTED_OPERATION",
            "failure_class": "client",
            "operation_key": operation_key,
            "ran_at": now_iso(),
        }

    package_name = str(pkg.get("package") or "")
    method = str(op.get("method") or "GET").upper()
    path = str(op.get("path") or "")
    operation_id = str(op.get("operation_id") or "")

    token, token_error = _resolve_factset_access_token()
    if token_error:
        return {
            "ok": False,
            "error": token_error.get("error"),
            "error_code": token_error.get("error_code"),
            "failure_class": token_error.get("failure_class"),
            "operation_key": operation_key,
            "operation_id": operation_id,
            "package": package_name,
            "ran_at": now_iso(),
        }

    try:
        sdk_mod = importlib.import_module(package_name)
        host = str(sdk_mod.Configuration().host or "").rstrip("/")
    except Exception:
        return {
            "ok": False,
            "error": f"Failed to import SDK package: {package_name}",
            "error_code": "FACTSET_SDK_PACKAGE_IMPORT_FAILED",
            "failure_class": "client",
            "operation_key": operation_key,
            "operation_id": operation_id,
            "package": package_name,
            "ran_at": now_iso(),
        }

    if not host:
        return {
            "ok": False,
            "error": f"No API host configured for package: {package_name}",
            "error_code": "FACTSET_SDK_PACKAGE_HOST_MISSING",
            "failure_class": "client",
            "operation_key": operation_key,
            "operation_id": operation_id,
            "package": package_name,
            "ran_at": now_iso(),
        }

    url = f"{host}/{path.lstrip('/')}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    request_kwargs: dict[str, Any] = {
        "method": method,
        "url": url,
        "headers": headers,
        "timeout": 60,
    }
    if query:
        request_kwargs["params"] = query
    if method in {"POST", "PUT", "PATCH"} and body is not None:
        request_kwargs["json"] = body

    started = time.perf_counter()
    try:
        response = requests.request(**request_kwargs)
        latency_ms = round((time.perf_counter() - started) * 1000, 2)
        return {
            "ok": response.ok,
            "status_code": response.status_code,
            "error_code": None if response.ok else "FACTSET_UPSTREAM_HTTP_ERROR",
            "failure_class": "upstream" if not response.ok else None,
            "latency_ms": latency_ms,
            "package": package_name,
            "operation_id": operation_id,
            "operation_key": operation_key,
            "method": method,
            "url": url,
            "query": query,
            "request_body": body,
            "response": parse_response_text(response),
            "error": None if response.ok else f"FactSet API returned HTTP {response.status_code}",
            "ran_at": now_iso(),
        }
    except Exception as exc:
        latency_ms = round((time.perf_counter() - started) * 1000, 2)
        return {
            "ok": False,
            "status_code": None,
            "error_code": "FACTSET_UPSTREAM_REQUEST_ERROR",
            "failure_class": "server",
            "latency_ms": latency_ms,
            "package": package_name,
            "operation_id": operation_id,
            "operation_key": operation_key,
            "method": method,
            "url": url,
            "query": query,
            "request_body": body,
            "response": None,
            "error": _sanitize_factset_error(exc),
            "ran_at": now_iso(),
        }


def build_snapshot() -> dict[str, Any]:
    snapshot = {
        "generated_at": now_iso(),
        "services": probe_services(),
        "ports": list_open_ports(),
    }
    write_backup_snapshot(snapshot)
    return snapshot


def _run_localwiki_command(args: list[str]) -> dict[str, Any]:
    if not LOCALWIKI_VENV_PYTHON.exists():
        return {
            "ok": False,
            "exit_code": None,
            "stdout": "",
            "stderr": f"Virtualenv python not found at {LOCALWIKI_VENV_PYTHON}",
            "ran_at": now_iso(),
        }

    try:
        result = subprocess.run(
            [str(LOCALWIKI_VENV_PYTHON), *args],
            cwd=str(LOCALWIKI_ROOT),
            text=True,
            capture_output=True,
            timeout=120,
        )
        return {
            "ok": result.returncode == 0,
            "exit_code": result.returncode,
            "stdout": clip_text(result.stdout, 4000),
            "stderr": clip_text(result.stderr, 4000),
            "ran_at": now_iso(),
        }
    except Exception as exc:
        return {
            "ok": False,
            "exit_code": None,
            "stdout": "",
            "stderr": repr(exc),
            "ran_at": now_iso(),
        }


def _source_loaded_summary(conn: sqlite3.Connection, source_id: str, root_uri: str) -> dict[str, Any]:
    cur = conn.cursor()
    cur.execute(
        "SELECT source_item_id, display_uri, uri, status FROM source_items WHERE source_id = ?",
        (source_id,),
    )
    rows = cur.fetchall()
    loaded = [
        {
            "source_item_id": row[0],
            "display_uri": row[1],
            "uri": row[2],
            "status": row[3],
        }
        for row in rows
    ]

    loaded_uris = {item["uri"] for item in loaded if item.get("uri")}
    root = Path(root_uri)
    discovered: list[str] = []
    if root.exists():
        if root.is_file():
            discovered = [str(root.resolve())]
        elif root.is_dir():
            discovered = [str(p.resolve()) for p in root.rglob("*") if p.is_file()]

    not_loaded = [path for path in discovered if path not in loaded_uris]

    return {
        "loaded_count": len(loaded),
        "not_loaded_count": len(not_loaded),
        "loaded": loaded,
        "not_loaded": not_loaded,
    }


def collect_localwiki_status() -> dict[str, Any]:
    payload: dict[str, Any] = {
        "checked_at": now_iso(),
        "root": str(LOCALWIKI_ROOT),
        "venv_python": str(LOCALWIKI_VENV_PYTHON),
        "venv_ready": LOCALWIKI_VENV_PYTHON.exists(),
        "db_path": str(LOCALWIKI_DB),
        "db_exists": LOCALWIKI_DB.exists(),
        "sources": [],
        "totals": {
            "sources": 0,
            "source_items": 0,
            "loaded_completed": 0,
            "loaded_pending": 0,
            "loaded_other": 0,
            "not_loaded": 0,
        },
        "errors": [],
    }

    if not LOCALWIKI_DB.exists():
        return payload

    try:
        conn = sqlite3.connect(str(LOCALWIKI_DB))
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        cur.execute("SELECT source_id, source_type, root_uri, display_name, added_at FROM sources ORDER BY added_at DESC")
        source_rows = cur.fetchall()
        payload["totals"]["sources"] = len(source_rows)

        for source in source_rows:
            summary = _source_loaded_summary(conn, source["source_id"], source["root_uri"])
            source_data = {
                "source_id": source["source_id"],
                "source_type": source["source_type"],
                "root_uri": source["root_uri"],
                "display_name": source["display_name"],
                "added_at": source["added_at"],
                **summary,
            }
            payload["sources"].append(source_data)

            payload["totals"]["source_items"] += source_data["loaded_count"]
            payload["totals"]["not_loaded"] += source_data["not_loaded_count"]
            for loaded_item in source_data["loaded"]:
                status = (loaded_item.get("status") or "").lower()
                if status == "completed":
                    payload["totals"]["loaded_completed"] += 1
                elif status == "pending":
                    payload["totals"]["loaded_pending"] += 1
                else:
                    payload["totals"]["loaded_other"] += 1

        conn.close()
    except Exception as exc:
        payload["errors"].append(repr(exc))

    return payload


def run_anythingllm_query(prompt: str, workspace_slug: str, api_key: str) -> dict[str, Any]:
    if not prompt.strip():
        return {"ok": False, "error": "Prompt is required"}

    url = f"{ANYTHINGLLM_BASE_URL}/api/v1/workspace/{workspace_slug}/chat"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "message": prompt,
        "mode": "query",
    }

    started = time.perf_counter()
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=60)
        latency_ms = round((time.perf_counter() - started) * 1000, 2)
        body: Any
        try:
            body = response.json()
        except Exception:
            body = {"raw": clip_text(response.text, 4000)}

        text_answer = ""
        if isinstance(body, dict):
            text_answer = str(
                body.get("textResponse")
                or body.get("response")
                or body.get("text")
                or ""
            )

        return {
            "ok": response.ok,
            "status_code": response.status_code,
            "latency_ms": latency_ms,
            "answer": text_answer,
            "body": body,
            "error": None if response.ok else f"AnythingLLM returned HTTP {response.status_code}",
        }
    except Exception as exc:
        latency_ms = round((time.perf_counter() - started) * 1000, 2)
        return {
            "ok": False,
            "status_code": None,
            "latency_ms": latency_ms,
            "answer": "",
            "body": None,
            "error": repr(exc),
        }


def restart_service(service_name: str) -> dict[str, Any]:
    command = SERVICE_RESTART_COMMANDS.get(service_name)
    if not command:
        return {
            "ok": False,
            "exit_code": None,
            "stdout": "",
            "stderr": f"Unsupported service: {service_name}",
            "service": service_name,
            "ran_at": now_iso(),
        }

    started = time.perf_counter()
    try:
        result = subprocess.run(
            ["bash", "-lc", command],
            text=True,
            capture_output=True,
            timeout=60,
        )
        latency_ms = round((time.perf_counter() - started) * 1000, 2)
        return {
            "ok": result.returncode == 0,
            "exit_code": result.returncode,
            "stdout": clip_text(result.stdout, 4000),
            "stderr": clip_text(result.stderr, 4000),
            "service": service_name,
            "latency_ms": latency_ms,
            "ran_at": now_iso(),
        }
    except Exception as exc:
        latency_ms = round((time.perf_counter() - started) * 1000, 2)
        return {
            "ok": False,
            "exit_code": None,
            "stdout": "",
            "stderr": repr(exc),
            "service": service_name,
            "latency_ms": latency_ms,
            "ran_at": now_iso(),
        }


def restart_ui_service() -> dict[str, Any]:
    if not UI_RESTART_SCRIPT.exists():
        return {
            "ok": False,
            "exit_code": None,
            "stdout": "",
            "stderr": f"Restart script not found: {UI_RESTART_SCRIPT}",
            "service": "Warlock UI",
            "latency_ms": 0,
            "ran_at": now_iso(),
        }

    started = time.perf_counter()
    try:
        subprocess.Popen(
            ["bash", "-lc", f"sleep 1 && \"{UI_RESTART_SCRIPT}\""],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        latency_ms = round((time.perf_counter() - started) * 1000, 2)
        return {
            "ok": True,
            "exit_code": 0,
            "stdout": "Scheduled UI restart in 1 second.",
            "stderr": "",
            "service": "Warlock UI",
            "latency_ms": latency_ms,
            "ran_at": now_iso(),
        }
    except Exception as exc:
        latency_ms = round((time.perf_counter() - started) * 1000, 2)
        return {
            "ok": False,
            "exit_code": None,
            "stdout": "",
            "stderr": repr(exc),
            "service": "Warlock UI",
            "latency_ms": latency_ms,
            "ran_at": now_iso(),
        }


@app.route("/")
def index() -> str:
    return render_template(
        "landing.html",
        anythingllm_base_url=ANYTHINGLLM_BASE_URL,
        default_workspace=ANYTHINGLLM_DEFAULT_WORKSPACE,
    )


@app.route("/dashboard")
def dashboard() -> str:
    return render_template("index.html")


@app.route("/api/status")
def api_status():
    payload = build_snapshot()
    return jsonify(payload)


@app.route("/api/localwiki/status")
def api_localwiki_status():
    return jsonify(collect_localwiki_status())


@app.route("/api/localwiki/venv/start", methods=["POST"])
def api_localwiki_venv_start():
    # Lightweight check that executes inside the localwiki virtualenv.
    result = _run_localwiki_command(["-m", "src.cli", "status"])
    return jsonify(result)


@app.route("/api/localwiki/command", methods=["POST"])
def api_localwiki_command():
    body = request.get_json(silent=True) or {}
    command_key = str(body.get("command") or "")
    args = LOCALWIKI_ALLOWED_COMMANDS.get(command_key)
    if args is None:
        return jsonify({"ok": False, "error": "Unsupported command"}), 400

    result = _run_localwiki_command(args)
    result["command"] = command_key
    result["args"] = args
    return jsonify(result)


@app.route("/api/localwiki/sources/update", methods=["POST"])
def api_localwiki_sources_update():
    body = request.get_json(silent=True) or {}
    path_value = str(body.get("path") or LOCALWIKI_DEFAULT_SOURCES_PATH).strip()
    no_auto_ingest = bool(body.get("no_auto_ingest", False))

    expanded_path = str(Path(path_value).expanduser())
    args = ["-m", "src.cli", "sources", "update", "--path", expanded_path]
    if no_auto_ingest:
        args.append("--no-auto-ingest")

    result = _run_localwiki_command(args)
    result["command"] = "sources_update"
    result["path"] = expanded_path
    result["no_auto_ingest"] = no_auto_ingest
    return jsonify(result)


@app.route("/api/anythingllm/query", methods=["POST"])
def api_anythingllm_query():
    body = request.get_json(silent=True) or {}
    prompt = str(body.get("prompt") or "").strip()
    workspace_slug = str(body.get("workspace_slug") or ANYTHINGLLM_DEFAULT_WORKSPACE).strip()
    api_key = str(body.get("api_key") or "").strip()

    if not api_key:
        return jsonify({"ok": False, "error": "AnythingLLM API key is required"}), 400

    result = run_anythingllm_query(prompt, workspace_slug, api_key)
    result["workspace_slug"] = workspace_slug
    result["requested_at"] = now_iso()
    return jsonify(result), (200 if result.get("ok") else 502)


@app.route("/api/services/restart", methods=["POST"])
def api_service_restart():
    body = request.get_json(silent=True) or {}
    service_name = str(body.get("service") or "").strip()
    result = restart_service(service_name)
    return jsonify(result), (200 if result.get("ok") else 502)


@app.route("/api/services/restart-all", methods=["POST"])
def api_services_restart_all():
    results: list[dict[str, Any]] = []
    ok = True
    for service in sorted(SERVICES, key=lambda item: item["priority"]):
        outcome = restart_service(service["name"])
        results.append(outcome)
        if not outcome.get("ok"):
            ok = False

    return jsonify({"ok": ok, "results": results, "ran_at": now_iso()}), (200 if ok else 502)


@app.route("/api/services/restart-ui", methods=["POST"])
def api_service_restart_ui():
    result = restart_ui_service()
    return jsonify(result), (202 if result.get("ok") else 502)


@app.route("/api/integrations/factset/status")
def api_factset_status():
    return jsonify(get_factset_status_payload())


@app.route("/api/integrations/factset/sdk-options")
def api_factset_sdk_options():
    return jsonify(get_factset_sdk_catalog_payload())


@app.route("/api/integrations/factset/sdk-run-sample", methods=["POST"])
def api_factset_sdk_run_sample():
    content_length = request.content_length or 0
    if content_length > FACTSET_SDK_RUN_MAX_BYTES:
        return jsonify({"ok": False, "error": "Request payload too large.", "error_code": "FACTSET_SDK_RUN_PAYLOAD_TOO_LARGE"}), 413

    try:
        raw_body = request.get_data(cache=False)
    except Exception:
        return jsonify({"ok": False, "error": "Unable to read request body.", "error_code": "FACTSET_SDK_RUN_BODY_READ_ERROR"}), 400

    if len(raw_body) > FACTSET_SDK_RUN_MAX_BYTES:
        return jsonify({"ok": False, "error": "Request payload too large.", "error_code": "FACTSET_SDK_RUN_PAYLOAD_TOO_LARGE"}), 413

    if not raw_body:
        body = {}
    else:
        try:
            decoded = raw_body.decode("utf-8")
            parsed = json.loads(decoded)
        except Exception:
            return jsonify({"ok": False, "error": "Invalid JSON payload.", "error_code": "FACTSET_SDK_RUN_INVALID_JSON"}), 400
        body = parsed

    if not isinstance(body, dict):
        return jsonify({"ok": False, "error": "Request body must be a JSON object.", "error_code": "FACTSET_SDK_RUN_INVALID_BODY"}), 400

    try:
        serialized = json.dumps(body)
        if len(serialized.encode("utf-8")) > FACTSET_SDK_RUN_MAX_BYTES:
            return jsonify({"ok": False, "error": "Request payload too large.", "error_code": "FACTSET_SDK_RUN_PAYLOAD_TOO_LARGE"}), 413
    except Exception:
        return jsonify({"ok": False, "error": "Invalid JSON payload.", "error_code": "FACTSET_SDK_RUN_INVALID_JSON"}), 400

    operation_key = str(body.get("operation_key") or "").strip()
    if not operation_key:
        return jsonify({"ok": False, "error": "operation_key is required."}), 400

    query = body.get("query")
    if query is None:
        query = {}
    if not isinstance(query, dict):
        return jsonify({"ok": False, "error": "query must be a JSON object."}), 400

    request_body = body.get("body")
    result = run_factset_sdk_sample(operation_key, query, request_body)
    if result.get("status_code") is not None:
        return jsonify(result), 200

    if str(result.get("failure_class") or "") == "client":
        return jsonify(result), 400

    return jsonify(result), 502


@app.route("/api/integrations/factset/config", methods=["POST"])
def api_factset_config():
    body = request.get_json(silent=True) or {}
    config_path = str(body.get("config_path") or "").strip()
    if not config_path:
        return jsonify({"ok": False, "configured": False, "message": "Config path is required."}), 400

    path = Path(config_path).expanduser()
    if not path.exists():
        return jsonify({"ok": False, "configured": False, "message": "Config file not found."}), 400
    if not path.is_file():
        return jsonify({"ok": False, "configured": False, "message": "Config path is not a file."}), 400
    if not os.access(path, os.R_OK):
        return jsonify({"ok": False, "configured": False, "message": "Config file is unreadable."}), 400
    if path.suffix.lower() != ".json":
        return jsonify({"ok": False, "configured": False, "message": "Config filename should end with .json."}), 400
    if not _is_allowed_factset_config_path(path):
        return jsonify({"ok": False, "configured": False, "message": "Config path is outside allowed directories.", "error_code": "FACTSET_CONFIG_PATH_NOT_ALLOWED"}), 400

    state = _load_factset_state()
    state["auth_method"] = "oauth2_client_credentials"
    state["config_source"] = "path"
    state["config_path"] = str(path)
    state["uploaded_filename"] = None
    _save_factset_state(state)
    return jsonify({"ok": True, "configured": True, "message": "FactSet config path saved."})


@app.route("/api/integrations/factset/upload", methods=["POST"])
def api_factset_upload():
    content_length = request.content_length or 0
    if content_length > FACTSET_UPLOAD_MAX_BYTES:
        return jsonify({"ok": False, "message": "Upload too large.", "error_code": "FACTSET_UPLOAD_TOO_LARGE"}), 413

    body = request.get_json(silent=True) or {}
    filename = str(body.get("filename") or "").strip()
    content = str(body.get("content") or "")
    if not filename:
        return jsonify({"ok": False, "message": "Uploaded file name is required.", "error_code": "FACTSET_UPLOAD_INVALID"}), 400
    if not filename.lower().endswith(".json"):
        return jsonify({"ok": False, "message": "Uploaded filename must end with .json.", "error_code": "FACTSET_UPLOAD_INVALID"}), 400
    if not content:
        return jsonify({"ok": False, "message": "Uploaded file content is empty.", "error_code": "FACTSET_UPLOAD_EMPTY"}), 400
    try:
        parsed = json.loads(content)
        if not isinstance(parsed, dict):
            raise ValueError("invalid")
    except Exception:
        return jsonify({"ok": False, "message": "Invalid FactSet OAuth configuration.", "error_code": "FACTSET_UPLOAD_INVALID_JSON"}), 400

    FACTSET_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    os.chmod(FACTSET_UPLOAD_DIR, stat.S_IRWXU)
    FACTSET_UPLOAD_PATH.write_text(content, encoding="utf-8")
    os.chmod(FACTSET_UPLOAD_PATH, stat.S_IRUSR | stat.S_IWUSR)

    state = _load_factset_state()
    state["auth_method"] = "oauth2_client_credentials"
    state["config_source"] = "uploaded_file"
    state["config_path"] = str(FACTSET_UPLOAD_PATH)
    state["uploaded_filename"] = Path(filename).name
    _save_factset_state(state)
    return jsonify({
        "ok": True,
        "configured": True,
        "message": "FactSet config uploaded successfully.",
        "config_path_display": str(FACTSET_UPLOAD_PATH),
        "uploaded_filename": Path(filename).name,
    })


@app.route("/api/integrations/factset/test", methods=["POST"])
def api_factset_test():
    state = _load_factset_state()
    config_path, _config_source = _resolve_factset_config(state)
    if not config_path:
        state["last_test_status"] = "failed"
        state["last_tested_at"] = now_iso()
        state["last_error_summary"] = "Config path is not set."
        _save_factset_state(state)
        return jsonify({"ok": False, "status": "failed", "message": "Config path is not set.", "error_code": "FACTSET_CONFIG_NOT_SET"}), 400

    path = Path(config_path).expanduser()
    if not path.exists():
        state["last_test_status"] = "failed"
        state["last_tested_at"] = now_iso()
        state["last_error_summary"] = "Config file not found."
        _save_factset_state(state)
        return jsonify({"ok": False, "status": "failed", "message": "Config file not found.", "error_code": "FACTSET_CONFIG_MISSING"}), 400
    if not path.is_file():
        return jsonify({"ok": False, "status": "failed", "message": "Config path is not a file.", "error_code": "FACTSET_CONFIG_INVALID_PATH"}), 400
    if not os.access(path, os.R_OK):
        return jsonify({"ok": False, "status": "failed", "message": "Config file is unreadable.", "error_code": "FACTSET_CONFIG_UNREADABLE"}), 400

    try:
        auth_mod = importlib.import_module("fds.sdk.utils.authentication")
    except Exception:
        state["last_test_status"] = "failed"
        state["last_tested_at"] = now_iso()
        state["last_error_summary"] = "SDK utility package is not installed."
        _save_factset_state(state)
        return jsonify({"ok": False, "status": "failed", "message": "SDK utility package is not installed.", "error_code": "FACTSET_SDK_UTILS_MISSING"}), 400

    try:
        client = auth_mod.ConfidentialClient(str(path))
        token_obj = client.get_access_token()
        if not token_obj:
            state["last_test_status"] = "failed"
            state["last_tested_at"] = now_iso()
            state["last_error_summary"] = "Token request failed."
            _save_factset_state(state)
            return jsonify({"ok": False, "status": "failed", "message": "Token request failed.", "error_code": "FACTSET_OAUTH_FAILED"}), 400

        expires_at = _extract_token_expiry(token_obj)
        state["last_test_status"] = "success"
        state["last_tested_at"] = now_iso()
        state["token_expires_at"] = expires_at
        state["last_error_summary"] = None
        _save_factset_state(state)
        return jsonify({
            "ok": True,
            "status": "success",
            "message": "OAuth token acquired successfully. FactSet credentials are valid.",
            "token_expires_at": expires_at,
        })
    except Exception as exc:
        safe_message = _sanitize_factset_error(exc)
        state["last_test_status"] = "failed"
        state["last_tested_at"] = now_iso()
        state["last_error_summary"] = safe_message
        _save_factset_state(state)
        return jsonify({"ok": False, "status": "failed", "message": safe_message, "error_code": "FACTSET_OAUTH_FAILED"}), 400


@app.route("/api/integrations/factset/clear", methods=["POST"])
def api_factset_clear():
    state = _load_factset_state()
    state["auth_method"] = "oauth2_client_credentials"
    state["config_source"] = "not_set"
    state["config_path"] = None
    state["uploaded_filename"] = None
    state["last_test_status"] = "never_tested"
    state["last_tested_at"] = None
    state["token_expires_at"] = None
    state["last_error_summary"] = None
    _save_factset_state(state)
    return jsonify({"ok": True, "configured": False, "message": "FactSet integration config cleared."})


@app.route("/imager")
def imager() -> str:
    return render_template(
        "imager.html",
        facefusion_root=str(IMAGER_ROOT),
        source_default=str(IMAGER_SOURCE_DEFAULT),
        target_default=str(IMAGER_TARGET_DEFAULT),
        output_default=str(IMAGER_OUTPUT_DEFAULT),
    )


@app.route("/api/imager/status")
def api_imager_status():
    return jsonify(_imager_status_payload())


@app.route("/api/imager/cookies/upload", methods=["POST"])
def api_imager_cookies_upload():
    content_length = request.content_length or 0
    if content_length > IMAGER_COOKIES_MAX_BYTES:
        return jsonify({"ok": False, "error": "Cookies upload exceeds size limit.", "error_code": "IMAGER_COOKIES_TOO_LARGE"}), 413

    file_storage = request.files.get("cookies_file")
    if not file_storage or not file_storage.filename:
        return jsonify({"ok": False, "error": "Missing uploaded file field: cookies_file.", "error_code": "IMAGER_COOKIES_MISSING"}), 400

    filename = Path(file_storage.filename).name.lower()
    if not filename.endswith(".txt"):
        return jsonify({"ok": False, "error": "Cookies file must be a .txt export.", "error_code": "IMAGER_COOKIES_INVALID_NAME"}), 400

    try:
        raw = file_storage.stream.read(IMAGER_COOKIES_MAX_BYTES + 1)
    except Exception as exc:
        return jsonify({"ok": False, "error": repr(exc), "error_code": "IMAGER_COOKIES_READ_FAILED"}), 500

    if not raw:
        return jsonify({"ok": False, "error": "Cookies file is empty.", "error_code": "IMAGER_COOKIES_EMPTY"}), 400
    if len(raw) > IMAGER_COOKIES_MAX_BYTES:
        return jsonify({"ok": False, "error": "Cookies upload exceeds size limit.", "error_code": "IMAGER_COOKIES_TOO_LARGE"}), 413

    try:
        content = raw.decode("utf-8", errors="strict")
    except Exception:
        return jsonify({"ok": False, "error": "Cookies file must be UTF-8 text.", "error_code": "IMAGER_COOKIES_INVALID_ENCODING"}), 400

    # Simple Netscape cookies format sanity check.
    lowered = content.lower()
    if "instagram.com" not in lowered and "# netscape http cookie file" not in lowered:
        return jsonify({"ok": False, "error": "Uploaded cookies file does not look like a browser cookie export.", "error_code": "IMAGER_COOKIES_INVALID_FORMAT"}), 400

    IMAGER_COOKIES_DIR.mkdir(parents=True, exist_ok=True)
    os.chmod(IMAGER_COOKIES_DIR, stat.S_IRWXU)
    IMAGER_COOKIES_PATH.write_text(content, encoding="utf-8")
    os.chmod(IMAGER_COOKIES_PATH, stat.S_IRUSR | stat.S_IWUSR)

    return jsonify({
        "ok": True,
        "message": "Instagram cookies uploaded.",
        "cookies_path": str(IMAGER_COOKIES_PATH),
        "size_bytes": len(raw),
    })


@app.route("/api/imager/cookies/clear", methods=["POST"])
def api_imager_cookies_clear():
    try:
        IMAGER_COOKIES_PATH.unlink(missing_ok=True)
    except Exception as exc:
        return jsonify({"ok": False, "error": repr(exc), "error_code": "IMAGER_COOKIES_CLEAR_FAILED"}), 500
    return jsonify({"ok": True, "message": "Instagram cookies cleared."})


@app.route("/api/imager/target/upload", methods=["POST"])
def api_imager_target_upload():
    content_length = request.content_length or 0
    if content_length > IMAGER_TARGET_MAX_BYTES:
        return jsonify({
            "ok": False,
            "error": "Upload exceeds 50MB limit.",
            "error_code": "IMAGER_TARGET_TOO_LARGE",
        }), 413

    file_storage = request.files.get("target_video")
    if not file_storage or not file_storage.filename:
        return jsonify({"ok": False, "error": "Missing uploaded file field: target_video.", "error_code": "IMAGER_TARGET_UPLOAD_MISSING"}), 400

    source_name = _sanitize_upload_name(file_storage.filename)
    src_ext = Path(source_name).suffix.lower()
    if src_ext and src_ext not in IMAGER_ALLOWED_VIDEO_EXTENSIONS:
        return jsonify({"ok": False, "error": "Unsupported video extension.", "error_code": "IMAGER_TARGET_UPLOAD_UNSUPPORTED"}), 400

    IMAGER_INPUT_DIR.mkdir(parents=True, exist_ok=True)
    file_ext = src_ext if src_ext in IMAGER_ALLOWED_VIDEO_EXTENSIONS else ".mp4"
    out_name = _ensure_video_filename(f"uploaded-{int(time.time())}-{source_name}", file_ext)
    output_path = IMAGER_INPUT_DIR / out_name

    total = 0
    try:
        with output_path.open("wb") as handle:
            while True:
                chunk = file_storage.stream.read(1024 * 1024)
                if not chunk:
                    break
                total += len(chunk)
                if total > IMAGER_TARGET_MAX_BYTES:
                    handle.close()
                    try:
                        output_path.unlink(missing_ok=True)
                    except Exception:
                        pass
                    return jsonify({
                        "ok": False,
                        "error": "Upload exceeds 50MB limit.",
                        "error_code": "IMAGER_TARGET_TOO_LARGE",
                    }), 413
                handle.write(chunk)
    except Exception as exc:
        return jsonify({"ok": False, "error": repr(exc), "error_code": "IMAGER_TARGET_UPLOAD_FAILED"}), 500

    return jsonify({
        "ok": True,
        "target_path": str(output_path),
        "size_bytes": total,
        "message": "Uploaded target video is ready.",
    })


@app.route("/api/imager/target/download", methods=["POST"])
def api_imager_target_download():
    body = request.get_json(silent=True) or {}
    source_url = str(body.get("url") or "").strip()
    if not source_url:
        return jsonify({"ok": False, "error": "url is required.", "error_code": "IMAGER_TARGET_URL_REQUIRED"}), 400

    parsed = urlparse(source_url)
    if parsed.scheme not in {"http", "https"}:
        return jsonify({"ok": False, "error": "Only http/https URLs are supported.", "error_code": "IMAGER_TARGET_URL_INVALID"}), 400

    is_public, host_error = _is_public_http_host(parsed.hostname or "")
    if not is_public:
        return jsonify({"ok": False, "error": host_error or "URL host is not allowed.", "error_code": "IMAGER_TARGET_URL_BLOCKED"}), 400

    if _is_extractor_url(source_url):
        output_path, extractor_error = _download_with_extractor(source_url)
        if extractor_error:
            error_code = str(extractor_error.get("error_code") or "")
            if error_code == "IMAGER_TARGET_TOO_LARGE":
                status = 413
            elif error_code in {"IMAGER_EXTRACTOR_AUTH_REQUIRED", "IMAGER_EXTRACTOR_AUTH_FAILED"}:
                status = 401
            elif error_code == "IMAGER_EXTRACTOR_MISSING":
                status = 400
            else:
                status = 502
            return jsonify(extractor_error), status
        assert output_path is not None
        try:
            size_bytes = output_path.stat().st_size
        except Exception:
            size_bytes = None
        return jsonify({
            "ok": True,
            "target_path": str(output_path),
            "size_bytes": size_bytes,
            "source_url": source_url,
            "download_method": "extractor",
            "message": "Downloaded target video is ready.",
        })

    IMAGER_INPUT_DIR.mkdir(parents=True, exist_ok=True)

    try:
        current_url = source_url
        redirects_followed = 0
        max_redirects = 5
        while True:
            current_parsed = urlparse(current_url)
            if current_parsed.scheme not in {"http", "https"}:
                return jsonify({"ok": False, "error": "Redirect target uses unsupported scheme.", "error_code": "IMAGER_TARGET_URL_INVALID"}), 400
            is_public, host_error = _is_public_http_host(current_parsed.hostname or "")
            if not is_public:
                return jsonify({"ok": False, "error": host_error or "Redirect target host is not allowed.", "error_code": "IMAGER_TARGET_URL_BLOCKED"}), 400

            resp = requests.get(current_url, stream=True, timeout=(10, 180), allow_redirects=False)
            status_code = resp.status_code
            if status_code in {301, 302, 303, 307, 308}:
                location = str(resp.headers.get("location") or "").strip()
                resp.close()
                if not location:
                    return jsonify({"ok": False, "error": "Redirect response missing Location header.", "error_code": "IMAGER_TARGET_DOWNLOAD_HTTP_ERROR", "status_code": status_code}), 502
                if redirects_followed >= max_redirects:
                    return jsonify({"ok": False, "error": "Too many redirects while downloading URL.", "error_code": "IMAGER_TARGET_TOO_MANY_REDIRECTS"}), 502
                current_url = requests.compat.urljoin(current_url, location)
                redirects_followed += 1
                continue
            break

        with resp:
            status_code = resp.status_code
            if status_code >= 400:
                return jsonify({
                    "ok": False,
                    "error": f"Download failed with HTTP {status_code}.",
                    "error_code": "IMAGER_TARGET_DOWNLOAD_HTTP_ERROR",
                    "status_code": status_code,
                }), 502

            final_url = str(resp.url or current_url or source_url)
            final_parsed = urlparse(final_url)
            hinted_name = Path(final_parsed.path).name or "downloaded-video"
            content_type = str(resp.headers.get("content-type") or "")
            content_length_header = resp.headers.get("content-length")

            if content_length_header:
                try:
                    expected_size = int(content_length_header)
                    if expected_size > IMAGER_TARGET_MAX_BYTES:
                        return jsonify({
                            "ok": False,
                            "error": "Remote file exceeds 50MB limit.",
                            "error_code": "IMAGER_TARGET_TOO_LARGE",
                        }), 413
                except Exception:
                    pass

            raw_ext = Path(hinted_name).suffix.lower()
            is_video_content_type = content_type.lower().startswith("video/")
            if not is_video_content_type:
                return jsonify({
                    "ok": False,
                    "error": "URL response is not a video content type.",
                    "error_code": "IMAGER_TARGET_URL_NOT_VIDEO",
                    "content_type": content_type,
                }), 400

            ext = raw_ext if raw_ext in IMAGER_ALLOWED_VIDEO_EXTENSIONS else _detect_extension_from_content_type(content_type)

            out_name = _ensure_video_filename(f"downloaded-{int(time.time())}-{hinted_name}", ext)
            output_path = IMAGER_INPUT_DIR / out_name

            total = 0
            with output_path.open("wb") as handle:
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    if not chunk:
                        continue
                    total += len(chunk)
                    if total > IMAGER_TARGET_MAX_BYTES:
                        handle.close()
                        try:
                            output_path.unlink(missing_ok=True)
                        except Exception:
                            pass
                        return jsonify({
                            "ok": False,
                            "error": "Remote file exceeds 50MB limit.",
                            "error_code": "IMAGER_TARGET_TOO_LARGE",
                        }), 413
                    handle.write(chunk)

    except requests.RequestException as exc:
        return jsonify({"ok": False, "error": repr(exc), "error_code": "IMAGER_TARGET_DOWNLOAD_FAILED"}), 502
    except Exception as exc:
        return jsonify({"ok": False, "error": repr(exc), "error_code": "IMAGER_TARGET_DOWNLOAD_FAILED"}), 500

    return jsonify({
        "ok": True,
        "target_path": str(output_path),
        "size_bytes": total,
        "source_url": source_url,
        "message": "Downloaded target video is ready.",
    })


@app.route("/api/imager/run", methods=["POST"])
def api_imager_run():
    if not IMAGER_PYTHON.exists() or not IMAGER_SCRIPT.exists():
        return jsonify({
            "ok": False,
            "error": "FaceFusion runtime is missing. Verify ~/facefusion and .venv setup.",
            "error_code": "IMAGER_RUNTIME_MISSING",
        }), 400

    body = request.get_json(silent=True) or {}
    if not isinstance(body, dict):
        return jsonify({"ok": False, "error": "Request body must be a JSON object.", "error_code": "IMAGER_INVALID_BODY"}), 400

    cfg, cfg_error = _merge_imager_config(body)
    if cfg_error:
        return jsonify({"ok": False, "error": cfg_error, "error_code": "IMAGER_INVALID_CONFIG"}), 400
    assert cfg is not None

    source = Path(cfg["source_path"])
    target = Path(cfg["target_path"])
    output = Path(cfg["output_path"])
    if not source.exists() or not source.is_file():
        return jsonify({"ok": False, "error": f"Source image missing: {source}", "error_code": "IMAGER_SOURCE_MISSING"}), 400
    if not target.exists() or not target.is_file():
        return jsonify({"ok": False, "error": f"Target video missing: {target}", "error_code": "IMAGER_TARGET_MISSING"}), 400

    output.parent.mkdir(parents=True, exist_ok=True)
    command = _imager_command_from_config(cfg)
    run_id = uuid.uuid4().hex[:12]
    log_path = IMAGER_LOG_DIR / f"facefusion-{run_id}.log"

    with IMAGER_STATE_LOCK:
        _refresh_imager_state_locked()
        if IMAGER_STATE.get("current") is not None:
            current = IMAGER_STATE.get("current") or {}
            return jsonify({
                "ok": False,
                "error": "An imager run is already in progress.",
                "error_code": "IMAGER_ALREADY_RUNNING",
                "current_run_id": current.get("run_id"),
            }), 409

        try:
            with log_path.open("w", encoding="utf-8") as log_handle:
                process = subprocess.Popen(
                    command,
                    cwd=str(IMAGER_ROOT),
                    stdout=log_handle,
                    stderr=subprocess.STDOUT,
                    text=True,
                )
        except Exception as exc:
            return jsonify({"ok": False, "error": repr(exc), "error_code": "IMAGER_SPAWN_FAILED"}), 500

        job = {
            "run_id": run_id,
            "status": "running",
            "variant": cfg.get("variant"),
            "started_at": now_iso(),
            "finished_at": None,
            "exit_code": None,
            "pid": process.pid,
            "log_path": str(log_path),
            "source_path": cfg["source_path"],
            "target_path": cfg["target_path"],
            "output_path": cfg["output_path"],
            "command": command,
            "config": cfg,
            "process": process,
        }
        IMAGER_STATE["current"] = job

    return jsonify({
        "ok": True,
        "run_id": run_id,
        "status": "running",
        "output_path": cfg["output_path"],
        "log_path": str(log_path),
        "started_at": now_iso(),
        "command": command,
    }), 202


@app.route("/api/imager/stop", methods=["POST"])
def api_imager_stop():
    with IMAGER_STATE_LOCK:
        _refresh_imager_state_locked()
        current = IMAGER_STATE.get("current")
        if not current:
            return jsonify({"ok": False, "error": "No imager run is active.", "error_code": "IMAGER_NOT_RUNNING"}), 400

        process: subprocess.Popen[str] | None = current.get("process")
        if process is None:
            return jsonify({"ok": False, "error": "No live process found for current run.", "error_code": "IMAGER_PROCESS_MISSING"}), 400

        try:
            process.terminate()
            try:
                process.wait(timeout=8)
            except Exception:
                process.kill()
                process.wait(timeout=5)
            exit_code = process.returncode
        except Exception as exc:
            return jsonify({"ok": False, "error": repr(exc), "error_code": "IMAGER_STOP_FAILED"}), 500

        current["status"] = "stopped"
        current["exit_code"] = exit_code
        current["finished_at"] = now_iso()
        current.pop("process", None)
        IMAGER_STATE["last"] = current
        IMAGER_STATE["current"] = None
        return jsonify({"ok": True, "status": "stopped", "run_id": current.get("run_id"), "exit_code": exit_code, "finished_at": current.get("finished_at")})


@app.route("/api/imager/logs")
def api_imager_logs():
    max_bytes = 20000
    with IMAGER_STATE_LOCK:
        _refresh_imager_state_locked()
        current = IMAGER_STATE.get("current")
        last = IMAGER_STATE.get("last")

        run_id = str(request.args.get("run_id") or "").strip()
        selected = None
        for candidate in [current, last]:
            if not candidate:
                continue
            if not run_id or str(candidate.get("run_id")) == run_id:
                selected = candidate
                break

    if not selected:
        return jsonify({"ok": False, "error": "No run logs available.", "error_code": "IMAGER_LOG_NOT_FOUND"}), 404

    log_path = Path(str(selected.get("log_path") or ""))
    if not log_path.exists() or not log_path.is_file():
        return jsonify({"ok": False, "error": f"Log file not found: {log_path}", "error_code": "IMAGER_LOG_FILE_MISSING"}), 404

    try:
        raw = log_path.read_text(encoding="utf-8", errors="replace")
    except Exception as exc:
        return jsonify({"ok": False, "error": repr(exc), "error_code": "IMAGER_LOG_READ_FAILED"}), 500

    clipped = raw[-max_bytes:]
    return jsonify({
        "ok": True,
        "run_id": selected.get("run_id"),
        "status": selected.get("status"),
        "log_path": str(log_path),
        "truncated": len(raw) > len(clipped),
        "output": clipped,
    })


@app.route("/api/imager/debug", methods=["POST"])
def api_imager_debug():
    body = request.get_json(silent=True) or {}
    action = str(body.get("action") or "").strip().lower()
    source_path = str(Path(str(body.get("source_path") or IMAGER_SOURCE_DEFAULT)).expanduser())
    target_path = str(Path(str(body.get("target_path") or IMAGER_TARGET_DEFAULT)).expanduser())

    if action == "verify_inputs":
        result = _run_imager_debug_command(["file", source_path, target_path], timeout=20)
        result["action"] = action
        return jsonify(result)
    if action == "help":
        result = _run_imager_debug_command([str(IMAGER_PYTHON), str(IMAGER_SCRIPT), "headless-run", "--help"], timeout=45)
        result["action"] = action
        return jsonify(result)
    if action == "list_outputs":
        outputs = []
        for path in sorted(IMAGER_ROOT.glob("*.mp4")):
            try:
                stat_res = path.stat()
                outputs.append({"path": str(path), "size_bytes": stat_res.st_size, "modified_at": datetime.fromtimestamp(stat_res.st_mtime, tz=timezone.utc).isoformat()})
            except Exception:
                continue
        return jsonify({"ok": True, "action": action, "outputs": outputs, "count": len(outputs), "ran_at": now_iso()})
    if action == "providers":
        code = "import onnxruntime as ort; print(ort.get_available_providers())"
        result = _run_imager_debug_command([str(IMAGER_PYTHON), "-c", code], timeout=30)
        result["action"] = action
        return jsonify(result)
    if action == "rocm_smi":
        result = _run_imager_debug_command(["rocm-smi"], timeout=20)
        result["action"] = action
        return jsonify(result)

    return jsonify({"ok": False, "error": "Unsupported debug action.", "error_code": "IMAGER_DEBUG_UNSUPPORTED_ACTION"}), 400


if __name__ == "__main__":
    app.run(host=HOST, port=PORT, debug=False)
