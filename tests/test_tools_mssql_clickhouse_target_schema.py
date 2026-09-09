from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def _load_tool_module():
    path = Path("tools/mssql_clickhouse_target_schema.py")
    spec = importlib.util.spec_from_file_location("dpone_tools_mssql_clickhouse_target_schema", path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class _Mssql:
    def fetch_schema(self, schema: str, table: str):
        assert (schema, table) == ("src", "events")
        return [
            ("business_date", "date nullable"),
            ("created_at", "datetime2(7) nullable"),
        ]


class _ClickHouse:
    def __init__(self, rows: list[tuple[str, str]]) -> None:
        self.rows = rows

    def get_records(self, sql: str):
        assert "DESCRIBE TABLE `analytics`.`events`" in sql
        return self.rows


def test_target_schema_proves_exact_policy_types_and_nullability() -> None:
    module = _load_tool_module()
    metrics = module.clickhouse_target_schema_metrics(
        mssql=_Mssql(),
        clickhouse=_ClickHouse(
            [
                ("business_date", "Nullable(Date)"),
                ("created_at", "Nullable(DateTime64(7))"),
            ]
        ),
        source_schema="src",
        source_table="events",
        target_database="analytics",
        target_table="events",
        type_fidelity=None,
    )

    assert metrics.column_count == 2
    assert metrics.mismatch_count == 0
    assert metrics.observed_sha256 == metrics.expected_sha256


def test_target_schema_rejects_wider_temporal_types_or_missing_nullable() -> None:
    module = _load_tool_module()
    metrics = module.clickhouse_target_schema_metrics(
        mssql=_Mssql(),
        clickhouse=_ClickHouse(
            [
                ("business_date", "Nullable(Date32)"),
                ("created_at", "DateTime"),
            ]
        ),
        source_schema="src",
        source_table="events",
        target_database="analytics",
        target_table="events",
        type_fidelity=None,
    )

    assert metrics.mismatch_count == 2
    assert metrics.observed_sha256 != metrics.expected_sha256
