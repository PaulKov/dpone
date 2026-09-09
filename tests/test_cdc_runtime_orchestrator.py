from __future__ import annotations

import json
from pathlib import Path

from dpone.readiness.cdc import CDCBackend, CDCOffset
from dpone.runtime.cdc.base import CDCBatch, CDCChange, CDCOperation
from dpone.runtime.cdc.runtime_models import CdcApplyReceipt, CdcRuntimePolicy, CdcRuntimeStream
from dpone.runtime.cdc.runtime_orchestrator import CdcRuntimeOrchestrator


class _Reader:
    def __init__(self, batch: CDCBatch) -> None:
        self.batch = batch
        self.start_offsets: list[CDCOffset | None] = []

    def setup(self) -> None:
        return None

    def read_batch(self, *, start_offset: CDCOffset | None = None, max_changes: int = 10000) -> CDCBatch:
        del max_changes
        self.start_offsets.append(start_offset)
        return self.batch


class _OffsetStore:
    def __init__(self, offset: CDCOffset | None = None) -> None:
        self.offset = offset
        self.saved: list[CDCOffset] = []

    def load_offset(self, stream: CdcRuntimeStream) -> CDCOffset | None:
        del stream
        return self.offset

    def save_offset(self, stream: CdcRuntimeStream, offset: CDCOffset) -> None:
        del stream
        self.saved.append(offset)
        self.offset = offset


class _Applier:
    def __init__(self, receipt: CdcApplyReceipt) -> None:
        self.receipt = receipt
        self.calls: list[tuple[CdcRuntimeStream, CDCBatch]] = []

    def apply(self, *, stream: CdcRuntimeStream, batch: CDCBatch) -> CdcApplyReceipt:
        self.calls.append((stream, batch))
        return self.receipt


def _stream() -> CdcRuntimeStream:
    return CdcRuntimeStream(
        pipeline_name="orders-cdc",
        source="mssql",
        sink="clickhouse",
        backend=CDCBackend.MSSQL_CDC,
        source_schema="dbo",
        source_table="orders",
        target_dataset="analytics.orders",
        unique_key=("order_id",),
    )


def _change(position: str, *, order_id: int = 1, sequence: int = 1) -> CDCChange:
    return CDCChange(
        operation=CDCOperation.UPDATE,
        data={"order_id": order_id, "status": "paid"},
        position=position,
        source_schema="dbo",
        source_table="orders",
        sequence=sequence,
        metadata={"backend": "mssql_cdc"},
    )


def test_cdc_runtime_orchestrator_commits_offset_after_durable_apply(tmp_path: Path) -> None:
    next_offset = CDCOffset(backend=CDCBackend.MSSQL_CDC, token="0x12", snapshot_complete=True)
    reader = _Reader(CDCBatch(changes=(_change("0x11"),), next_offset=next_offset, high_watermark="0x12"))
    store = _OffsetStore(CDCOffset(backend=CDCBackend.MSSQL_CDC, token="0x10", snapshot_complete=True))
    applier = _Applier(
        CdcApplyReceipt(
            passed=True,
            durable=True,
            rows_applied=1,
            rows_deleted=0,
            blockers=tuple(),
            warnings=tuple(),
            metrics={"target": "clickhouse"},
            artifact_uri="memory://receipt",
        )
    )

    report = CdcRuntimeOrchestrator().run_once(
        stream=_stream(),
        reader=reader,
        offset_store=store,
        sink_applier=applier,
        output_dir=tmp_path / "runtime",
        policy=CdcRuntimePolicy(max_changes=50),
    )

    payload = json.loads(Path(report.json_path).read_text(encoding="utf-8"))
    assert report.passed is True
    assert report.committed is True
    assert report.rows_read == 1
    assert store.saved == [next_offset]
    assert applier.calls
    assert reader.start_offsets == [CDCOffset(backend=CDCBackend.MSSQL_CDC, token="0x10", snapshot_complete=True)]
    assert payload["schema_version"] == "dpone.cdc_runtime_run.v1"
    assert payload["stream"]["route_id"] == "mssql_to_clickhouse__cdc"
    assert payload["next_offset"]["token"] == "0x12"


def test_cdc_runtime_orchestrator_does_not_commit_when_sink_is_not_durable(tmp_path: Path) -> None:
    next_offset = CDCOffset(backend=CDCBackend.MSSQL_CDC, token="0x12", snapshot_complete=True)
    reader = _Reader(CDCBatch(changes=(_change("0x11"),), next_offset=next_offset, high_watermark="0x12"))
    store = _OffsetStore()
    applier = _Applier(
        CdcApplyReceipt(
            passed=True,
            durable=False,
            rows_applied=1,
            rows_deleted=0,
            blockers=tuple(),
            warnings=tuple(),
            metrics={},
            artifact_uri="memory://receipt",
        )
    )

    report = CdcRuntimeOrchestrator().run_once(
        stream=_stream(),
        reader=reader,
        offset_store=store,
        sink_applier=applier,
        output_dir=tmp_path / "runtime",
    )

    assert report.passed is False
    assert report.committed is False
    assert store.saved == []
    assert "cdc_runtime.sink_not_durable" in report.blockers


def test_cdc_runtime_orchestrator_blocks_duplicate_events_before_apply(tmp_path: Path) -> None:
    duplicate = _change("0x11")
    reader = _Reader(
        CDCBatch(
            changes=(duplicate, duplicate),
            next_offset=CDCOffset(backend=CDCBackend.MSSQL_CDC, token="0x12", snapshot_complete=True),
            high_watermark="0x12",
        )
    )
    store = _OffsetStore()
    applier = _Applier(
        CdcApplyReceipt(
            passed=True,
            durable=True,
            rows_applied=2,
            rows_deleted=0,
            blockers=tuple(),
            warnings=tuple(),
            metrics={},
            artifact_uri="memory://receipt",
        )
    )

    report = CdcRuntimeOrchestrator().run_once(
        stream=_stream(),
        reader=reader,
        offset_store=store,
        sink_applier=applier,
        output_dir=tmp_path / "runtime",
    )

    assert report.passed is False
    assert report.committed is False
    assert applier.calls == []
    assert store.saved == []
    assert "cdc_runtime.duplicate_events" in report.blockers
    assert report.metrics["duplicate_events"] == 1
