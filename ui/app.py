from __future__ import annotations

import json
import base64
import importlib
import importlib.metadata
import importlib.util
import os
import socket
import sqlite3
import stat
import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
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


if __name__ == "__main__":
    app.run(host=HOST, port=PORT, debug=False)
