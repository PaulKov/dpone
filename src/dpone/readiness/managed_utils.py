"""Shared helpers for managed readiness services."""

from __future__ import annotations

import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any

_SECRET_TOKENS = ("password", "secret", "token", "apikey", "api_key", "authorization", "credential")


def _table(schema: str | None, table: str | None) -> str:
    return f"{schema}.{table}" if schema and table else "-"


def _qualified_table(table: Mapping[str, Any]) -> str:
    return f"{table.get('schema', 'landing')}.{table.get('name', 'table')}"


def _source_columns(source_options: Mapping[str, Any]) -> list[tuple[str, str]]:
    columns = source_options.get("columns", [])
    if isinstance(columns, Mapping):
        return [(str(name), str(dtype)) for name, dtype in columns.items()]
    if isinstance(columns, list):
        return [
            (str(item["name"]), str(item.get("type", item.get("dtype", "string"))))
            for item in columns
            if isinstance(item, Mapping) and "name" in item
        ]
    return []


def _list_option(value: Any) -> list[str]:
    """Normalize one scalar-or-list planning option."""

    if value is None:
        return []
    return [value] if isinstance(value, str) else [str(item) for item in value]


def _configured_columns(raw: Mapping[str, Any]) -> list[str]:
    """Return configured source column names from a process mapping."""

    source = raw.get("source", {}) if isinstance(raw.get("source"), Mapping) else {}
    options = source.get("options", {}) if isinstance(source.get("options"), Mapping) else {}
    return _list_option(options.get("columns"))


def _schema_contract(raw: Mapping[str, Any], sink_options: Mapping[str, Any]) -> dict[str, Any]:
    contract = sink_options.get("schema_contract", raw.get("schema_contract", {}))
    return dict(contract) if isinstance(contract, Mapping) else {}


def _redact(value: Any) -> Any:
    if isinstance(value, Mapping):
        out: dict[str, Any] = {}
        for key, item in value.items():
            if any(token in str(key).lower() for token in _SECRET_TOKENS):
                out[str(key)] = "***"
            else:
                out[str(key)] = _redact(item)
        return out
    if isinstance(value, list):
        return [_redact(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_redact(item) for item in value)
    return value


def _safe_identifier(value: str) -> str:
    cleaned = "".join(ch.lower() if ch.isalnum() else "_" for ch in str(value)).strip("_")
    return cleaned or "connector"


def _safe_filename(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in str(value))


def _env_key(value: str) -> str:
    return _safe_identifier(value).upper()


def _pascal(value: str) -> str:
    return "".join(part.capitalize() for part in _safe_identifier(value).split("_"))


def temporary_artifact_dir() -> Path:
    return Path(tempfile.mkdtemp(prefix="dpone_managed_"))


__all__ = [
    "_env_key",
    "_configured_columns",
    "_list_option",
    "_pascal",
    "_qualified_table",
    "_redact",
    "_safe_filename",
    "_safe_identifier",
    "_schema_contract",
    "_source_columns",
    "_table",
    "temporary_artifact_dir",
]
