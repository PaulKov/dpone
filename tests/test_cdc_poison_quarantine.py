from __future__ import annotations

import json
from pathlib import Path

from dpone.readiness.cdc import CDCBackend, CDCOffset
from dpone.runtime.cdc.base import CDCBatch, CDCChange, CDCOperation
from dpone.runtime.cdc.poison import CdcPoisonClassifier
from dpone.runtime.cdc.runtime_models import CdcApplyReceipt, CdcRuntimePolicy, CdcRuntimeStream
from dpone.runtime.cdc.runtime_orchestrator import CdcRuntimeOrchestrator


class _Reader:
    def __init__(self, batch: CDCBatch) -> None:
        self.batch = batch

    def setup(self) -> None:
        return None

    def read_batch(self, *, start_offset: CDCOffset | None = None, max_changes: int = 10000) -> CDCBatch:
        del start_offset, max_changes
        return self.batch


class _OffsetStore:
    def __init__(self) -> None:
        self.saved: list[CDCOffset] = []

    def load_offset(self, stream: CdcRuntimeStream) -> CDCOffset | None:
        del stream
        return None

    def save_offset(self, stream: CdcRuntimeStream, offset: CDCOffset) -> None:
        del stream
        self.saved.append(offset)


class _Applier:
    def __init__(self) -> None:
        self.calls: list[CDCBatch] = []

    def apply(self, *, stream: CdcRuntimeStream, batch: CDCBatch) -> CdcApplyReceipt:
        del stream
        self.calls.append(batch)
        return CdcApplyReceipt(
            passed=True,
            durable=True,
            rows_applied=batch.row_count,
            rows_deleted=sum(1 for change in batch.changes if change.is_delete),
            blockers=tuple(),
            warnings=tuple(),
            metrics={"artifact_rows": batch.row_count},
            artifact_uri="memory://cdc-sink",
        )


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


def _change(
    position: str,
    *,
    operation: CDCOperation = CDCOperation.UPDATE,
    data: dict[str, object] | None = None,
    sequence: int = 1,
) -> CDCChange:
    return CDCChange(
        operation=operation,
        data=data if data is not None else {"order_id": 1, "status": "paid"},
        position=position,
        source_schema="dbo",
        source_table="orders",
        sequence=sequence,
        metadata={"backend": "mssql_change_tracking"},
    )


def test_poison_classifier_normalizes_missing_key_unsupported_operation_and_duplicate() -> None:
    duplicate = _change("42", sequence=2)

    decision = CdcPoisonClassifier().classify(
        stream=_stream(),
        changes=(
            _change("40", data={"status": "missing-key"}),
            _change("41", operation=CDCOperation.UPDATE_BEFORE),
            duplicate,
            duplicate,
        ),
    )

    assert [record.reason for record in decision.records] == [
        "cdc_poison.unique_key_missing",
        "cdc_poison.unsupported_operation",
        "cdc_poison.duplicate_event",
    ]
    assert all(record.action == "quarantine" for record in decision.records)
    assert all(len(record.event_id) == 64 for record in decision.records)
    assert [change.position for change in decision.clean_changes] == ["42"]


def test_cdc_runtime_fail_closed_blocks_poison_without_apply_or_commit(tmp_path: Path) -> None:
    next_offset = CDCOffset(backend=CDCBackend.MSSQL_CHANGE_TRACKING, token="43", snapshot_complete=True)
    reader = _Reader(
        CDCBatch(
            changes=(_change("42", operation=CDCOperation.UPDATE_BEFORE),),
            next_offset=next_offset,
            high_watermark="43",
        )
    )
    store = _OffsetStore()
    applier = _Applier()

    report = CdcRuntimeOrchestrator().run_once(
        stream=_stream(),
        reader=reader,
        offset_store=store,
        sink_applier=applier,
        output_dir=tmp_path / "runtime",
    )

    payload = json.loads(Path(report.json_path).read_text(encoding="utf-8"))
    assert report.passed is False
    assert report.committed is False
    assert applier.calls == []
    assert store.saved == []
    assert "cdc_runtime.poison_events" in report.blockers
    assert payload["poison_quarantine"]["record_count"] == 1
    assert Path(payload["poison_quarantine"]["json_path"]).exists()


def test_cdc_runtime_quarantine_and_continue_applies_clean_events_and_commits(tmp_path: Path) -> None:
    next_offset = CDCOffset(backend=CDCBackend.MSSQL_CHANGE_TRACKING, token="43", snapshot_complete=True)
    clean = _change("42", data={"order_id": 2, "status": "paid"}, sequence=2)
    reader = _Reader(
        CDCBatch(
            changes=(
                _change("41", operation=CDCOperation.UPDATE_BEFORE),
                clean,
            ),
            next_offset=next_offset,
            high_watermark="43",
        )
    )
    store = _OffsetStore()
    applier = _Applier()

    report = CdcRuntimeOrchestrator().run_once(
        stream=_stream(),
        reader=reader,
        offset_store=store,
        sink_applier=applier,
        output_dir=tmp_path / "runtime",
        policy=CdcRuntimePolicy(poison_mode="quarantine_and_continue"),
    )

    quarantine_payload = json.loads((tmp_path / "runtime" / "cdc_poison_quarantine.json").read_text(encoding="utf-8"))
    assert report.passed is True
    assert report.committed is True
    assert store.saved == [next_offset]
    assert len(applier.calls) == 1
    assert applier.calls[0].changes == (clean,)
    assert quarantine_payload["record_count"] == 1
    assert quarantine_payload["records"][0]["reason"] == "cdc_poison.unsupported_operation"
