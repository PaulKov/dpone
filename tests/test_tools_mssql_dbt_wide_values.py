from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest


def _load_tool_module():
    path = Path("tools/mssql_dbt_wide_values.py")
    spec = importlib.util.spec_from_file_location("dpone_tools_mssql_dbt_wide_values", path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class _Connector:
    def __init__(self, *, drift: bool = False) -> None:
        self._drift = drift

    def fetch_schema(self, schema: str, table: str):
        if schema == "src":
            return [("order_id", "int"), ("created_at", "datetime2(7)"), ("payload", "varbinary(8)")]
        return [
            ("order_id", "int"),
            ("created_at", "datetime2(7)"),
            ("payload", "varbinary(8)"),
            ("dbt_calculated_amount", "decimal(38,8)"),
        ]

    def qualified_name(self, schema: str, table: str) -> str:
        return f"[{schema}].[{table}]"

    def get_records(self, sql: str):
        if "[src].[orders]" in sql:
            return [(1, "2026-08-10 12:00:00.1234567", b"payload")]
        if "dbt_calculated_amount" in sql:
            return [(1, "2026-08-10 12:00:00.1234567", b"payload", "4.00000000")]
        value = b"changed" if self._drift else b"payload"
        return [(1, "2026-08-10 12:00:00.1234567", value)]


def test_dbt_wide_value_metrics_hash_every_passthrough_and_calculated_value() -> None:
    module = _load_tool_module()
    metrics = module.dbt_wide_value_metrics(
        _Connector(),
        source_schema="src",
        source_table="orders",
        target_schema="dbt_calc",
        target_table="wide_dbt_result",
        rows=1,
    )

    assert metrics.source_sha256 == metrics.passthrough_sha256
    assert metrics.output_sha256.startswith("sha256:")
    assert metrics.output_sha256 != metrics.source_sha256


def test_dbt_wide_value_metrics_exposes_same_count_value_drift() -> None:
    module = _load_tool_module()
    metrics = module.dbt_wide_value_metrics(
        _Connector(drift=True),
        source_schema="src",
        source_table="orders",
        target_schema="dbt_calc",
        target_table="wide_dbt_result",
        rows=1,
    )

    assert metrics.source_sha256 != metrics.passthrough_sha256


def test_dbt_wide_value_metrics_rejects_partial_all_row_read() -> None:
    module = _load_tool_module()

    with pytest.raises(RuntimeError, match="row_count_mismatch"):
        module.dbt_wide_value_metrics(
            _Connector(),
            source_schema="src",
            source_table="orders",
            target_schema="dbt_calc",
            target_table="wide_dbt_result",
            rows=2,
        )
