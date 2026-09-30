"""Closed public selector for native ClickHouse source snapshot semantics.

Omission preserves the legacy query-visible route. A raw selector must name
the one server or connected replica whose rows and metadata are authoritative.
The parser performs no connection, credential lookup, or vendor I/O.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal


@dataclass(frozen=True)
class ClickHouseSourceSnapshotPolicy:
    """Declared semantics and replica scope; source qualification occurs later."""

    mode: Literal["query_visible", "exact_raw_rows"]
    replica_scope: Literal["single_server", "connected_replica"] | None


def native_source_snapshot_policy(config: Any) -> ClickHouseSourceSnapshotPolicy:
    """Parse only the two approved selector fields, preserving omission.

    The returned query-visible policy is an in-memory default. Callers must not
    serialize it into manifests that omitted ``source_snapshot``.
    """

    options = getattr(config, "options", None)
    if options is None:
        options = {}
    if not isinstance(options, Mapping):
        raise ValueError("mssql_native.source_snapshot_mode_invalid")
    native = options.get("native_transfer", {})
    if native is None or native is False:
        return ClickHouseSourceSnapshotPolicy("query_visible", None)
    if not isinstance(native, Mapping):
        raise ValueError("mssql_native.source_snapshot_mode_invalid")
    if "source_snapshot" not in native:
        return ClickHouseSourceSnapshotPolicy("query_visible", None)
    value = native["source_snapshot"]
    if not isinstance(value, Mapping) or set(value) not in ({"mode"}, {"mode", "replica_scope"}):
        raise ValueError("mssql_native.source_snapshot_mode_invalid")
    mode, scope = value["mode"], value.get("replica_scope")
    if mode == "query_visible" and type(mode) is str and "replica_scope" not in value:
        return ClickHouseSourceSnapshotPolicy("query_visible", None)
    if mode == "exact_raw_rows" and type(mode) is str and scope == "single_server" and type(scope) is str:
        return ClickHouseSourceSnapshotPolicy("exact_raw_rows", "single_server")
    if mode == "exact_raw_rows" and type(mode) is str and scope == "connected_replica" and type(scope) is str:
        return ClickHouseSourceSnapshotPolicy("exact_raw_rows", "connected_replica")
    raise ValueError("mssql_native.source_snapshot_mode_invalid")
