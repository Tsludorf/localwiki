"""Shared helpers for management_quotes DSN validation."""

from __future__ import annotations

from urllib.parse import urlparse


_PG_DSN_SCHEMES = {
    "postgres",
    "postgresql",
    "postgresql+psycopg",
    "postgresql+psycopg2",
}


def is_management_quotes_placeholder_dsn(dsn: str | None) -> bool:
    """Return True when DSN uses an unresolved placeholder host value."""

    if not dsn:
        return False

    parsed = urlparse(dsn)
    scheme = (parsed.scheme or "").lower()
    if scheme not in _PG_DSN_SCHEMES:
        return False

    host = (parsed.hostname or "").lower()
    return host == "host"
