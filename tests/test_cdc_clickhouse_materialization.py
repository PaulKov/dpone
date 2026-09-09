from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from dpone.runtime.cdc.materialization import (
    ClickHouseCdcMaterializationPlan,
    ClickHouseCdcMaterializationPolicy,
    ClickHouseCdcMaterializationService,
)


class _FakeClickHouseConnector:
    database = "default"

    def __init__(
        self,
        *,
        table_exists: bool = False,
        source_events: int = 0,
        materialized_rows: int = 0,
        deleted_rows: int = 0,
        fail_on: str | None = None,
    ) -> None:
        self.table_exists = table_exists
        self.source_events = source_events
        self.materialized_rows = materialized_rows
        self.deleted_rows = deleted_rows
        self.fail_on = fail_on
        self.queries: list[str] = []
        self.records_queries: list[str] = []

    def execute_query(self, query: str) -> int:
        self.queries.append(query)
        if self.fail_on and self.fail_on in query:
            raise RuntimeError("materialization failed")
        return 0

    def get_records(self, query: str, params: Any = None, as_dict: bool = False) -> list[Any]:
        del params
        self.records_queries.append(query)
        if "system.tables" in query:
            rows = [{"exists": 1 if self.table_exists else 0}]
        elif "count()" in query and "dpone_cdc_deleted = 1" in query:
            rows = [{"rows": self.deleted_rows}]
        elif "count()" in query and "__dpone_materialization_shadow" in query:
            rows = [{"rows": self.materialized_rows}]
        elif "count()" in query:
            rows = [{"rows": self.source_events}]
        else:
            rows = []
        if as_dict:
            return rows
        return [tuple(row.values()) for row in rows]


def _plan() -> ClickHouseCdcMaterializationPlan:
    return ClickHouseCdcMaterializationPlan.from_datasets(
        cdc_dataset="analytics.orders_cdc",
        target_dataset="serving.orders_current",
        unique_key=("order_id",),
        default_database="default",
    )


def test_clickhouse_cdc_materialization_plan_parses_datasets_and_quotes_identifiers() -> None:
    plan = _plan()

    assert plan.cdc_database == "analytics"
    assert plan.cdc_table == "orders_cdc"
    assert plan.target_database == "serving"
    assert plan.target_table == "orders_current"
    assert plan.qualified_cdc_table == "`analytics`.`orders_cdc`"
    assert plan.qualified_target_table == "`serving`.`orders_current`"
    assert plan.artifact_uri == "clickhouse://serving.orders_current"


def test_clickhouse_cdc_materialization_plan_rejects_empty_unique_key() -> None:
    with pytest.raises(ValueError, match="unique key"):
        ClickHouseCdcMaterializationPlan.from_datasets(
            cdc_dataset="analytics.orders_cdc",
            target_dataset="serving.orders_current",
            unique_key=(),
            default_database="default",
        )


def test_clickhouse_cdc_materialization_replaces_target_with_latest_non_deleted_rows(tmp_path: Path) -> None:
    connector = _FakeClickHouseConnector(
        table_exists=True,
        source_events=3,
        materialized_rows=1,
        deleted_rows=1,
    )

    report = ClickHouseCdcMaterializationService(connector).materialize(
        plan=_plan(),
        policy=ClickHouseCdcMaterializationPolicy(delete_mode="exclude_deleted"),
        output_dir=tmp_path,
    )

    all_sql = "\n".join(connector.queries)
    payload = json.loads((tmp_path / "cdc_materialization.json").read_text(encoding="utf-8"))
    assert report.passed is True
    assert report.delete_mode == "exclude_deleted"
    assert report.rows_source_events == 3
    assert report.rows_materialized == 1
    assert report.rows_deleted == 1
    assert report.blockers == tuple()
    assert "row_number() OVER" in all_sql
    assert "dpone_cdc_deleted = 0" in all_sql
    assert "RENAME TABLE" in all_sql
    assert payload["schema_version"] == "dpone.cdc_clickhouse_materialization.v1"
    assert payload["target_dataset"] == "serving.orders_current"
    assert (
        (tmp_path / "cdc_materialization.md").read_text(encoding="utf-8").startswith("# ClickHouse CDC materialization")
    )


def test_clickhouse_cdc_materialization_tombstone_mode_keeps_deleted_latest_rows(tmp_path: Path) -> None:
    connector = _FakeClickHouseConnector(
        source_events=3,
        materialized_rows=2,
        deleted_rows=1,
    )

    report = ClickHouseCdcMaterializationService(connector).materialize(
        plan=_plan(),
        policy=ClickHouseCdcMaterializationPolicy(delete_mode="tombstone"),
        output_dir=tmp_path,
    )

    all_sql = "\n".join(connector.queries)
    assert report.passed is True
    assert report.rows_materialized == 2
    assert report.rows_deleted == 1
    assert "dpone_cdc_deleted = 0" not in all_sql
    assert report.metrics["dedupe_key"] == "dpone_cdc_unique_key_hash"


def test_clickhouse_cdc_materialization_returns_failure_report_without_raising(tmp_path: Path) -> None:
    connector = _FakeClickHouseConnector(source_events=2, fail_on="INSERT INTO")

    report = ClickHouseCdcMaterializationService(connector).materialize(
        plan=_plan(),
        policy=ClickHouseCdcMaterializationPolicy(delete_mode="exclude_deleted"),
        output_dir=tmp_path,
    )

    payload = json.loads((tmp_path / "cdc_materialization.json").read_text(encoding="utf-8"))
    assert report.passed is False
    assert "clickhouse_cdc_materialization.failed" in report.blockers
    assert "materialization failed" in report.metrics["error"]
    assert payload["passed"] is False
