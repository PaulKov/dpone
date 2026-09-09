from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from dpone.runtime.columnar_parquet_writer import mssql_source_type_supported
from dpone.runtime.columnar_snapshot_provider import ColumnarSnapshotRequest


def schema_blockers(schema: Sequence[tuple[str, str]]) -> list[str]:
    return [
        f"mssql_columnar_type_unsupported:{column}:{source_type}"
        for column, source_type in schema
        if not mssql_source_type_supported(source_type)
    ]


def read_contract_options(request: ColumnarSnapshotRequest) -> dict[str, Any]:
    options = dict(request.options or {})
    clickhouse_read_access = options.get("clickhouse_read_access")
    if isinstance(clickhouse_read_access, dict):
        return dict(clickhouse_read_access)
    object_storage = options.get("object_storage")
    if isinstance(object_storage, dict):
        nested_read_access = object_storage.get("clickhouse_read_access")
        if isinstance(nested_read_access, dict):
            return dict(nested_read_access)
    return {}
