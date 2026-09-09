from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from dpone.runtime.cdc.typed_materialization import (
    ClickHouseCdcPayloadProjector,
    ClickHouseCdcTypedColumn,
    ClickHouseCdcTypedMaterializationPlan,
    ClickHouseCdcTypedMaterializationPolicy,
    ClickHouseCdcTypedMaterializationService,
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
            raise RuntimeError("typed materialization failed")
        return 0

    def get_records(self, query: str, params: Any = None, as_dict: bool = False) -> list[Any]:
        del params
        self.records_queries.append(query)
        if "system.tables" in query:
            rows = [{"exists": 1 if self.table_exists else 0}]
        elif "count()" in query and "dpone_cdc_deleted = 1" in query:
            rows = [{"rows": self.deleted_rows}]
        elif "count()" in query and "__dpone_typed_materialization_shadow" in query:
            rows = [{"rows": self.materialized_rows}]
        elif "count()" in query:
            rows = [{"rows": self.source_events}]
        else:
            rows = []
        if as_dict:
            return rows
        return [tuple(row.values()) for row in rows]


def _columns() -> tuple[ClickHouseCdcTypedColumn, ...]:
    return (
        ClickHouseCdcTypedColumn(name="order_id", clickhouse_type="Int32", required=True),
        ClickHouseCdcTypedColumn(name="status", clickhouse_type="Nullable(String)"),
        ClickHouseCdcTypedColumn(name="amount", clickhouse_type="Decimal(18,2)"),
        ClickHouseCdcTypedColumn(name="updated_at", clickhouse_type="Nullable(DateTime64(3, 'UTC'))"),
    )


def _plan() -> ClickHouseCdcTypedMaterializationPlan:
    return ClickHouseCdcTypedMaterializationPlan.from_datasets(
        cdc_dataset="analytics.orders_cdc",
        target_dataset="serving.orders_current_typed",
        unique_key=("order_id",),
        columns=_columns(),
        default_database="default",
    )


def test_clickhouse_cdc_typed_column_parser_validates_names_and_types() -> None:
    column = ClickHouseCdcTypedColumn.from_cli("amount=Decimal(18,2)")

    assert column.name == "amount"
    assert column.payload_key == "amount"
    assert column.clickhouse_type == "Decimal(18,2)"
    assert column.ddl_fragment == "`amount` Decimal(18,2)"

    with pytest.raises(ValueError, match="name=Type"):
        ClickHouseCdcTypedColumn.from_cli("amount")
    with pytest.raises(ValueError, match="Unsafe ClickHouse column name"):
        ClickHouseCdcTypedColumn.from_cli("bad-name=String")
    with pytest.raises(ValueError, match="Unsupported ClickHouse type"):
        ClickHouseCdcTypedColumn.from_cli("payload=Map(String,String)")


def test_clickhouse_cdc_payload_projector_renders_safe_clickhouse_json_expressions() -> None:
    projector = ClickHouseCdcPayloadProjector(_columns())
    select_sql = projector.select_expressions(payload_column="dpone_cdc_payload_json")

    assert "`order_id`" in select_sql
    assert "toInt32OrNull(JSONExtractRaw(dpone_cdc_payload_json, 'order_id'))" in select_sql
    assert "JSONExtractString(dpone_cdc_payload_json, 'status')" in select_sql
    assert "toDecimal128OrNull(JSONExtractString(dpone_cdc_payload_json, 'amount'), 2)" in select_sql
    assert (
        "parseDateTime64BestEffortOrNull(JSONExtractString(dpone_cdc_payload_json, 'updated_at'), 3, 'UTC')"
        in select_sql
    )


def test_clickhouse_cdc_typed_materialization_plan_parses_datasets_and_columns() -> None:
    plan = _plan()

    assert plan.qualified_cdc_table == "`analytics`.`orders_cdc`"
    assert plan.qualified_target_table == "`serving`.`orders_current_typed`"
    assert plan.qualified_shadow_table == "`serving`.`orders_current_typed__dpone_typed_materialization_shadow`"
    assert [column.name for column in plan.columns] == ["order_id", "status", "amount", "updated_at"]
    assert plan.artifact_uri == "clickhouse://serving.orders_current_typed"


def test_clickhouse_cdc_typed_materialization_replaces_target_with_typed_rows(tmp_path: Path) -> None:
    connector = _FakeClickHouseConnector(
        table_exists=True,
        source_events=4,
        materialized_rows=2,
        deleted_rows=1,
    )

    report = ClickHouseCdcTypedMaterializationService(connector).materialize(
        plan=_plan(),
        policy=ClickHouseCdcTypedMaterializationPolicy(delete_mode="tombstone"),
        output_dir=tmp_path,
    )

    all_sql = "\n".join(connector.queries)
    payload = json.loads((tmp_path / "cdc_typed_materialization.json").read_text(encoding="utf-8"))
    assert report.passed is True
    assert report.rows_source_events == 4
    assert report.rows_materialized == 2
    assert report.rows_deleted == 1
    assert report.typed_columns == ("order_id", "status", "amount", "updated_at")
    assert "`amount` Decimal(18,2)" in all_sql
    assert "toDecimal128OrNull(JSONExtractString(dpone_cdc_payload_json, 'amount'), 2)" in all_sql
    assert "RENAME TABLE" in all_sql
    assert payload["schema_version"] == "dpone.cdc_clickhouse_typed_materialization.v1"
    assert payload["schema_policy"]["mode"] == "shadow_replace"
    assert payload["typed_columns"][2]["clickhouse_type"] == "Decimal(18,2)"
    assert (
        (tmp_path / "cdc_typed_materialization.md")
        .read_text(encoding="utf-8")
        .startswith("# ClickHouse CDC typed materialization")
    )


def test_clickhouse_cdc_typed_materialization_excludes_deleted_rows_by_default(tmp_path: Path) -> None:
    connector = _FakeClickHouseConnector(source_events=3, materialized_rows=1, deleted_rows=1)

    report = ClickHouseCdcTypedMaterializationService(connector).materialize(
        plan=_plan(),
        policy=ClickHouseCdcTypedMaterializationPolicy(delete_mode="exclude_deleted"),
        output_dir=tmp_path,
    )

    all_sql = "\n".join(connector.queries)
    assert report.passed is True
    assert "dpone_cdc_deleted = 0" in all_sql
    assert report.metrics["projection_mode"] == "clickhouse_json_extract"


def test_clickhouse_cdc_typed_materialization_returns_failure_report(tmp_path: Path) -> None:
    connector = _FakeClickHouseConnector(source_events=2, fail_on="INSERT INTO")

    report = ClickHouseCdcTypedMaterializationService(connector).materialize(
        plan=_plan(),
        policy=ClickHouseCdcTypedMaterializationPolicy(delete_mode="exclude_deleted"),
        output_dir=tmp_path,
    )

    payload = json.loads((tmp_path / "cdc_typed_materialization.json").read_text(encoding="utf-8"))
    assert report.passed is False
    assert "clickhouse_cdc_typed_materialization.failed" in report.blockers
    assert "typed materialization failed" in report.metrics["error"]
    assert payload["passed"] is False
