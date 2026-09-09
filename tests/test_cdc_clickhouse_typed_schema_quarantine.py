from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from dpone.runtime.cdc.typed_materialization import (
    ClickHouseCdcTypedColumn,
    ClickHouseCdcTypedMaterializationPlan,
    ClickHouseCdcTypedMaterializationPolicy,
    ClickHouseCdcTypedMaterializationService,
    ClickHouseCdcTypedQualityPolicy,
)
from dpone.runtime.cdc.typed_materialization_quarantine_sql import (
    create_parse_quarantine_table_sql,
    insert_parse_quarantine_sql,
    parse_failure_counts_sql,
)


class _QualityFakeClickHouseConnector:
    database = "default"

    def __init__(
        self,
        *,
        payloads: list[str] | None = None,
        parse_failures: list[dict[str, object]] | None = None,
        materialized_rows: int = 1,
    ) -> None:
        self.payloads = payloads or ['{"amount":"20.00","order_id":2,"status":"open"}']
        self.parse_failures = parse_failures or []
        self.materialized_rows = materialized_rows
        self.queries: list[str] = []
        self.records_queries: list[str] = []

    def execute_query(self, query: str) -> int:
        self.queries.append(query)
        return 0

    def get_records(self, query: str, params: Any = None, as_dict: bool = False) -> list[Any]:
        del params
        self.records_queries.append(query)
        if "AS failed_rows" in query:
            rows: list[dict[str, object]] = self.parse_failures
        elif "SELECT dpone_cdc_payload_json" in query:
            rows = [{"dpone_cdc_payload_json": payload} for payload in self.payloads]
        elif "system.tables" in query:
            rows = [{"exists": 0}]
        elif "count()" in query and "dpone_cdc_deleted = 1" in query:
            rows = [{"rows": 0}]
        elif "count()" in query and "__dpone_typed_materialization_shadow" in query:
            rows = [{"rows": self.materialized_rows}]
        elif "count()" in query:
            rows = [{"rows": len(self.payloads)}]
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
    )


def _plan() -> ClickHouseCdcTypedMaterializationPlan:
    return ClickHouseCdcTypedMaterializationPlan.from_datasets(
        cdc_dataset="analytics.orders_cdc",
        target_dataset="serving.orders_current_typed",
        unique_key=("order_id",),
        columns=_columns(),
        default_database="default",
    )


def test_quality_policy_serializes_fail_closed_defaults() -> None:
    policy = ClickHouseCdcTypedQualityPolicy(
        fail_on_parse_errors=True,
        max_parse_error_ratio=0.01,
        quarantine_dataset="serving.orders_current_typed_quarantine",
        schema_drift_mode="strict",
        sample_limit=25,
    )

    payload = policy.to_dict()
    assert payload == {
        "enabled": True,
        "fail_on_parse_errors": True,
        "max_parse_error_ratio": 0.01,
        "quarantine_dataset": "serving.orders_current_typed_quarantine",
        "sample_limit": 25,
        "schema_drift_mode": "strict",
    }


def test_parse_quarantine_sql_renders_diagnostic_predicates() -> None:
    plan = _plan()
    policy = ClickHouseCdcTypedMaterializationPolicy(delete_mode="exclude_deleted")

    counts_sql = parse_failure_counts_sql(plan=plan, policy=policy)
    create_sql = create_parse_quarantine_table_sql("`serving`.`orders_parse_quarantine`")
    insert_sql = insert_parse_quarantine_sql(
        plan=plan,
        policy=policy,
        qualified_quarantine_table="`serving`.`orders_parse_quarantine`",
    )

    assert "isNull(toDecimal128OrNull(JSONExtractString(dpone_cdc_payload_json, 'amount'), 2))" in counts_sql
    assert "JSONExtractRaw(dpone_cdc_payload_json, 'amount') != ''" in counts_sql
    assert "dpone_cdc_deleted = 0" in counts_sql
    assert "`column_name` LowCardinality(String)" in create_sql
    assert "`clickhouse_type` String" in create_sql
    assert "INSERT INTO `serving`.`orders_parse_quarantine`" in insert_sql
    assert "'amount' AS `column_name`" in insert_sql
    assert "`dpone_cdc_payload_json`" in insert_sql


def test_typed_materialization_fails_closed_and_writes_parse_quarantine_evidence(tmp_path: Path) -> None:
    connector = _QualityFakeClickHouseConnector(
        parse_failures=[{"column_name": "amount", "clickhouse_type": "Decimal(18,2)", "failed_rows": 1}]
    )

    report = ClickHouseCdcTypedMaterializationService(connector).materialize(
        plan=_plan(),
        policy=ClickHouseCdcTypedMaterializationPolicy(
            delete_mode="exclude_deleted",
            quality=ClickHouseCdcTypedQualityPolicy(
                fail_on_parse_errors=True,
                max_parse_error_ratio=0.0,
                quarantine_dataset="serving.orders_parse_quarantine",
            ),
        ),
        output_dir=tmp_path,
    )

    payload = json.loads((tmp_path / "cdc_typed_materialization.json").read_text(encoding="utf-8"))
    quarantine = json.loads((tmp_path / "cdc_typed_parse_quarantine.json").read_text(encoding="utf-8"))
    all_sql = "\n".join(connector.queries)
    assert report.passed is False
    assert "clickhouse_cdc_typed_materialization.parse_quarantine" in report.blockers
    assert "CREATE TABLE IF NOT EXISTS `serving`.`orders_parse_quarantine`" in all_sql
    assert "INSERT INTO `serving`.`orders_parse_quarantine`" in all_sql
    assert "RENAME TABLE" not in all_sql
    assert payload["quality_evidence"]["parse_quarantine"]["failed_rows"] == 1
    assert payload["quality_evidence"]["parse_quarantine"]["failures"][0]["column_name"] == "amount"
    assert quarantine["failed_rows"] == 1
    assert (tmp_path / "cdc_typed_parse_quarantine.md").exists()


def test_typed_materialization_blocks_missing_required_payload_key(tmp_path: Path) -> None:
    connector = _QualityFakeClickHouseConnector(payloads=['{"amount":"20.00","status":"open"}'])

    report = ClickHouseCdcTypedMaterializationService(connector).materialize(
        plan=_plan(),
        policy=ClickHouseCdcTypedMaterializationPolicy(
            delete_mode="exclude_deleted",
            quality=ClickHouseCdcTypedQualityPolicy(schema_drift_mode="strict"),
        ),
        output_dir=tmp_path,
    )

    payload = json.loads((tmp_path / "cdc_typed_materialization.json").read_text(encoding="utf-8"))
    assert report.passed is False
    assert "clickhouse_cdc_typed_materialization.schema_drift" in report.blockers
    assert payload["quality_evidence"]["schema_drift"]["missing_required_keys"] == ["order_id"]


def test_typed_materialization_warns_on_additive_payload_keys(tmp_path: Path) -> None:
    connector = _QualityFakeClickHouseConnector(
        payloads=['{"amount":"20.00","extra_note":"new","order_id":2,"status":"open"}'],
        materialized_rows=1,
    )

    report = ClickHouseCdcTypedMaterializationService(connector).materialize(
        plan=_plan(),
        policy=ClickHouseCdcTypedMaterializationPolicy(
            delete_mode="exclude_deleted",
            quality=ClickHouseCdcTypedQualityPolicy(schema_drift_mode="warn_additive"),
        ),
        output_dir=tmp_path,
    )

    payload = json.loads((tmp_path / "cdc_typed_materialization.json").read_text(encoding="utf-8"))
    assert report.passed is True
    assert "clickhouse_cdc_typed_materialization.additive_payload_keys" in report.warnings
    assert payload["quality_evidence"]["schema_drift"]["additive_payload_keys"] == ["extra_note"]
    assert payload["rows_materialized"] == 1
