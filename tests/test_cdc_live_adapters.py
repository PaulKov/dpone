from __future__ import annotations

from dpone.readiness.cdc import CDCBackend
from dpone.runtime.cdc.base import CDCBatch, CDCChange, CDCOperation
from dpone.runtime.cdc.live_adapters import (
    ClickHouseCdcApplyPlan,
    ClickHouseCdcSinkApplier,
    cdc_event_hash,
)
from dpone.runtime.cdc.runtime_models import CdcRuntimeStream


class _Connection:
    def __init__(self, *, fail_insert: bool = False) -> None:
        self.fail_insert = fail_insert
        self.calls: list[tuple[str, object | None]] = []

    def execute(self, query: str, params: object | None = None) -> list[object]:
        self.calls.append((query, params))
        if self.fail_insert and query.startswith("INSERT INTO"):
            raise RuntimeError("insert failed")
        return []


class _ClickHouseConnector:
    database = "default"

    def __init__(self, *, fail_insert: bool = False) -> None:
        self.connection = _Connection(fail_insert=fail_insert)
        self.queries: list[str] = []

    def execute_query(self, query: str) -> int:
        self.queries.append(query)
        self.connection.execute(query)
        return 0


def _stream() -> CdcRuntimeStream:
    return CdcRuntimeStream(
        pipeline_name="orders-cdc",
        source="mssql",
        sink="clickhouse",
        backend=CDCBackend.MSSQL_CDC,
        source_schema="dbo",
        source_table="orders",
        target_dataset="analytics.orders_cdc",
        unique_key=("order_id",),
    )


def _change(position: str, *, operation: CDCOperation = CDCOperation.UPDATE, order_id: int = 1) -> CDCChange:
    return CDCChange(
        operation=operation,
        data={"order_id": order_id, "status": "paid"},
        position=position,
        source_schema="dbo",
        source_table="orders",
        sequence=7,
        metadata={"backend": "mssql_cdc"},
    )


def test_clickhouse_cdc_apply_plan_parses_target_dataset_and_quotes_table() -> None:
    plan = ClickHouseCdcApplyPlan.from_stream(_stream(), default_database="default")

    assert plan.database == "analytics"
    assert plan.table == "orders_cdc"
    assert plan.qualified_table == "`analytics`.`orders_cdc`"
    assert plan.apply_mode == "clickhouse_append_cdc_log"


def test_cdc_event_hash_is_deterministic_for_canonical_change_payload() -> None:
    first = cdc_event_hash(_change("0x11"), unique_key=("order_id",))
    second = cdc_event_hash(_change("0x11"), unique_key=("order_id",))
    different_position = cdc_event_hash(_change("0x12"), unique_key=("order_id",))

    assert first == second
    assert first != different_position
    assert len(first) == 64


def test_clickhouse_cdc_sink_applier_writes_durable_append_only_cdc_rows() -> None:
    connector = _ClickHouseConnector()
    batch = CDCBatch(
        changes=(
            _change("0x11", operation=CDCOperation.UPDATE),
            _change("0x12", operation=CDCOperation.DELETE),
        ),
        next_offset=None,
        high_watermark="0x12",
    )

    receipt = ClickHouseCdcSinkApplier(connector).apply(stream=_stream(), batch=batch)

    insert_calls = [call for call in connector.connection.calls if call[0].startswith("INSERT INTO")]
    assert receipt.passed is True
    assert receipt.durable is True
    assert receipt.rows_applied == 2
    assert receipt.rows_deleted == 1
    assert receipt.artifact_uri == "clickhouse://analytics.orders_cdc"
    assert receipt.metrics["apply_mode"] == "clickhouse_append_cdc_log"
    assert receipt.metrics["batch_hash"]
    assert "CREATE TABLE IF NOT EXISTS `analytics`.`orders_cdc`" in "\n".join(connector.queries)
    assert len(insert_calls) == 1
    assert len(insert_calls[0][1]) == 2
    assert insert_calls[0][1][0]["dpone_cdc_operation"] == "update"
    assert insert_calls[0][1][1]["dpone_cdc_deleted"] == 1


def test_clickhouse_cdc_sink_applier_returns_failure_receipt_without_raising() -> None:
    connector = _ClickHouseConnector(fail_insert=True)
    batch = CDCBatch(changes=(_change("0x11"),), next_offset=None, high_watermark="0x11")

    receipt = ClickHouseCdcSinkApplier(connector).apply(stream=_stream(), batch=batch)

    assert receipt.passed is False
    assert receipt.durable is False
    assert receipt.rows_applied == 0
    assert "clickhouse_cdc_apply.failed" in receipt.blockers
    assert "insert failed" in str(receipt.metrics["error"])
