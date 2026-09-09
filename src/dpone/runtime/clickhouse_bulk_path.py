"""Shared ClickHouse bulk-path resolution for sources and sinks."""

from __future__ import annotations

import os
import shutil
from collections.abc import Callable, Mapping
from typing import Any, Literal

from dpone.runtime.bulk_options import ClickHouseBulkOptionsResolver

ClickHouseBulkPath = Literal["http", "client", "native_tcp", "python"]

_CLIENT_MODES = {"client", "clickhouse-client", "native_client"}
_PYTHON_MODES = {"python", "driver", "native_driver"}


def resolve_clickhouse_bulk_path(
    options: Mapping[str, Any] | None,
    *,
    env: Mapping[str, str] | None = None,
    executable_lookup: Callable[[str], str | None] | None = None,
) -> ClickHouseBulkPath:
    """Return the actual ClickHouse bulk path implied by runtime options."""

    raw = dict(options or {})
    bulk = ClickHouseBulkOptionsResolver.resolve(raw)
    mode = bulk.mode.lower()
    if mode in _PYTHON_MODES:
        return "python"
    if mode in {"native_tcp", "native-tcp"}:
        return "native_tcp"
    if mode == "http":
        return "http"
    if mode in _CLIENT_MODES:
        return "client"
    if bulk.native_tcp.enabled:
        return "native_tcp"
    if bulk.http.host or raw.get("clickhouse_http_url"):
        return "http"
    runtime_env = env if env is not None else os.environ
    if bulk.client.command or runtime_env.get("DPONE_CLICKHOUSE_CLIENT"):
        return "client"
    lookup = executable_lookup or shutil.which
    return "client" if lookup("clickhouse-client") else "python"


def clickhouse_path_supports_native_tsv(path: ClickHouseBulkPath) -> bool:
    """Return true when MSSQL can emit ClickHouse-ready TabSeparated files."""

    return path in {"http", "client"}


__all__ = [
    "ClickHouseBulkPath",
    "clickhouse_path_supports_native_tsv",
    "resolve_clickhouse_bulk_path",
]
