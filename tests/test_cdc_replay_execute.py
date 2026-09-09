from __future__ import annotations

import json
from pathlib import Path

from dpone.ops.cdc.quarantine import CdcQuarantineInspectionService
from dpone.readiness.cdc import CDCBackend
from dpone.runtime.cdc.base import CDCBatch
from dpone.runtime.cdc.replay_execution import CdcReplayExecutionService
from dpone.runtime.cdc.runtime_models import CdcApplyReceipt, CdcRuntimeStream


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
            artifact_uri="memory://replay",
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


def _write_quarantine(path: Path) -> Path:
    payload = {
        "schema_version": "dpone.cdc_poison_quarantine.v1",
        "stream": _stream().to_dict(),
        "record_count": 1,
        "records": [
            {
                "record_id": "orders-cdc:42:1",
                "event_id": "e" * 64,
                "reason": "cdc_poison.unsupported_operation",
                "action": "quarantine",
                "replayable": True,
                "summary": "Unsupported CDC operation was quarantined",
                "change": {
                    "operation": "update",
                    "data": {"order_id": 1, "status": "paid"},
                    "position": "42",
                    "source_schema": "dbo",
                    "source_table": "orders",
                    "sequence": 1,
                    "metadata": {"backend": "mssql_change_tracking"},
                },
            }
        ],
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def test_cdc_replay_execution_replays_quarantined_events_without_offsets(tmp_path: Path) -> None:
    quarantine_json = _write_quarantine(tmp_path / "cdc_poison_quarantine.json")
    applier = _Applier()

    report = CdcReplayExecutionService(sink_applier=applier).execute(
        output_dir=tmp_path / "replay",
        quarantine_json=quarantine_json,
        stream=_stream(),
    )

    payload = json.loads((tmp_path / "replay" / "cdc_replay_execution.json").read_text(encoding="utf-8"))
    assert report.passed is True
    assert report.committed is False
    assert report.replayed_events == 1
    assert len(applier.calls) == 1
    assert applier.calls[0].changes[0].position == "42"
    assert payload["schema_version"] == "dpone.cdc_replay_execution.v1"
    assert payload["committed"] is False
    assert payload["sink_receipt"]["rows_applied"] == 1


def test_cdc_quarantine_inspection_summarizes_reasons_and_actions(tmp_path: Path) -> None:
    quarantine_json = _write_quarantine(tmp_path / "cdc_poison_quarantine.json")

    report = CdcQuarantineInspectionService().inspect(
        output_dir=tmp_path / "inspect",
        quarantine_json=quarantine_json,
    )

    payload = json.loads((tmp_path / "inspect" / "cdc_quarantine_inspection.json").read_text(encoding="utf-8"))
    assert report.passed is True
    assert report.record_count == 1
    assert report.reason_counts == {"cdc_poison.unsupported_operation": 1}
    assert report.action_counts == {"quarantine": 1}
    assert payload["schema_version"] == "dpone.cdc_quarantine_inspection.v1"
