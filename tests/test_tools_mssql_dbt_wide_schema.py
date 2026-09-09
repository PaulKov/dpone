from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def _load_tool_module():
    path = Path("tools/mssql_dbt_wide_schema.py")
    spec = importlib.util.spec_from_file_location("dpone_tools_mssql_dbt_wide_schema", path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class _Connector:
    def __init__(self, source, target) -> None:
        self._source = source
        self._target = target

    def get_records(self, sql: str):
        return self._source if "o.name = N'orders'" in sql else self._target


def test_dbt_wide_schema_preserves_all_types_and_normalizes_rowversion_projection() -> None:
    module = _load_tool_module()
    source = [
        ("order_id", "int", 4, 10, 0, False),
        ("created_at", "datetime2", 8, 27, 7, True),
        ("row_version", "timestamp", 8, 0, 0, False),
    ]
    target = [
        ("order_id", "int", 4, 10, 0, False),
        ("created_at", "datetime2", 8, 27, 7, True),
        ("row_version", "binary", 8, 0, 0, False),
        ("dbt_calculated_amount", "decimal", 17, 38, 8, True),
    ]

    metrics = module.dbt_wide_schema_metrics(
        _Connector(source, target),
        source_schema="src",
        source_table="orders",
        target_schema="dbt_calc",
        target_table="wide_dbt_result",
    )

    assert metrics.source_column_count == 3
    assert metrics.target_column_count == 4
    assert metrics.mismatch_count == 0
    assert metrics.source_sha256 == metrics.passthrough_sha256
    assert metrics.calculated_column_type == "decimal(38,8)"


def test_dbt_wide_schema_reports_passthrough_type_drift() -> None:
    module = _load_tool_module()
    source = [("created_at", "datetime2", 8, 27, 7, True)]
    target = [
        ("created_at", "datetime", 8, 23, 3, True),
        ("dbt_calculated_amount", "decimal", 17, 38, 8, True),
    ]

    metrics = module.dbt_wide_schema_metrics(
        _Connector(source, target),
        source_schema="src",
        source_table="orders",
        target_schema="dbt_calc",
        target_table="wide_dbt_result",
    )

    assert metrics.mismatch_count == 1
    assert metrics.source_sha256 != metrics.passthrough_sha256


def test_dbt_wide_schema_reports_nullability_drift() -> None:
    module = _load_tool_module()
    source = [("created_at", "datetime2", 8, 27, 7, True)]
    target = [
        ("created_at", "datetime2", 8, 27, 7, False),
        ("dbt_calculated_amount", "decimal", 17, 38, 8, True),
    ]

    metrics = module.dbt_wide_schema_metrics(
        _Connector(source, target),
        source_schema="src",
        source_table="orders",
        target_schema="dbt_calc",
        target_table="wide_dbt_result",
    )

    assert metrics.mismatch_count == 1


def test_dbt_wide_schema_rejects_arbitrary_201_column_inventory_and_nonnullable_calculation() -> None:
    module = _load_tool_module()
    source = [(f"forged_{index:03d}", "int", 4, 10, 0, True) for index in range(201)]
    target = [*source, ("dbt_calculated_amount", "decimal", 17, 38, 8, False)]

    metrics = module.dbt_wide_schema_metrics(
        _Connector(source, target),
        source_schema="src",
        source_table="orders",
        target_schema="dbt_calc",
        target_table="wide_dbt_result",
    )

    assert metrics.mismatch_count == 0
    assert metrics.canonical_source_mismatch_count == 201
    assert metrics.calculated_column_nullable is False
