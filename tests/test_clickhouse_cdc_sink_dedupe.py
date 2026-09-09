from __future__ import annotations

from typing import Any

from dpone.readiness.cdc import CDCBackend, CDCOffset
from dpone.runtime.cdc.base import CDCBatch, CDCChange, CDCOperation
from dpone.runtime.cdc.live_adapters import ClickHouseCdcSinkApplier
from dpone.runtime.cdc.runtime_models import CdcRuntimeStream


class _Connection:
    def __init__(self, connector: _ClickHouseConnector) -> None:
        self._connector = connector

    def execute(self, query: str, rows: list[dict[str, Any]]) -> None:
        del query
        self._connector.inserted_rows.extend(rows)
        self._connector.existing_hashes.update(str(row["dpone_cdc_event_hash"]) for row in rows)


class _ClickHouseConnector:
    database = "analytics"

    def __init__(self) -> None:
        self.connection = _Connection(self)
        self.queries: list[str] = []
        self.inserted_rows: list[dict[str, Any]] = []
        self.existing_hashes: set[str] = set()

    def execute_query(self, query: str) -> int:
        self.queries.append(query)
        return 0

    def get_records(self, query: str, *args: object, as_dict: bool = False) -> list[dict[str, Any]]:
        del args, as_dict
        if "dpone_cdc_event_hash" not in query:
            return []
        return [{"dpone_cdc_event_hash": event_hash} for event_hash in sorted(self.existing_hashes)]


def _stream() -> CdcRuntimeStream:
    return CdcRuntimeStream(
        pipeline_name="orders-cdc",
        source="mssql",
        sink="clickhouse",
        backend=CDCBackend.MSSQL_CHANGE_TRACKING,
        source_schema="dbo",
        source_table="orders",
        target_dataset="analytics.orders_cdc",
        unique_key=("order_id",),
    )


def _batch() -> CDCBatch:
    return CDCBatch(
        changes=(
            CDCChange(
                operation=CDCOperation.INSERT,
                data={"order_id": 1, "status": "new"},
                position="42",
                source_schema="dbo",
                source_table="orders",
                sequence=1,
            ),
        ),
        next_offset=CDCOffset(backend=CDCBackend.MSSQL_CHANGE_TRACKING, token="42", snapshot_complete=True),
        high_watermark="42",
    )


def test_clickhouse_cdc_sink_applier_skips_existing_event_hash_on_replay() -> None:
    connector = _ClickHouseConnector()
    applier = ClickHouseCdcSinkApplier(connector)

    first = applier.apply(stream=_stream(), batch=_batch())
    replay = applier.apply(stream=_stream(), batch=_batch())

    assert first.passed is True
    assert first.durable is True
    assert first.rows_applied == 1
    assert replay.passed is True
    assert replay.durable is True
    assert replay.rows_applied == 0
    assert replay.metrics["duplicate_events_skipped"] == 1
    assert len(connector.inserted_rows) == 1
