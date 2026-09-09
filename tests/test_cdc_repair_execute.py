from __future__ import annotations

import json
from pathlib import Path

from dpone.readiness.cdc import CDCBackend
from dpone.runtime.cdc.base import CDCBatch
from dpone.runtime.cdc.repair import CdcRepairExecutionService
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
            artifact_uri="memory://repair",
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


def _write_repair_plan(path: Path) -> Path:
    payload = {
        "schema_version": "dpone.cdc_repair_plan.v1",
        "stream": _stream().to_dict(),
        "action_count": 2,
        "actions": [
            {
                "action_id": "repair:orders-cdc:1",
                "kind": "value_mismatch",
                "operation": "update",
                "key": {"order_id": 1},
                "payload": {"order_id": 1, "status": "paid"},
                "target_payload": {"order_id": 1, "status": "stale"},
                "reason": "source and target hashes differ",
                "replayable": True,
            },
            {
                "action_id": "repair:orders-cdc:3",
                "kind": "extra_in_target",
                "operation": "delete",
                "key": {"order_id": 3},
                "payload": {"order_id": 3},
                "target_payload": {"order_id": 3, "status": "orphan"},
                "reason": "target key is absent from source",
                "replayable": True,
            },
        ],
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def test_cdc_repair_execution_applies_repair_actions_without_offsets(tmp_path: Path) -> None:
    repair_plan_json = _write_repair_plan(tmp_path / "cdc_repair_plan.json")
    applier = _Applier()

    report = CdcRepairExecutionService(sink_applier=applier).execute(
        output_dir=tmp_path / "repair",
        repair_plan_json=repair_plan_json,
        stream=_stream(),
    )

    payload = json.loads((tmp_path / "repair" / "cdc_repair_execution.json").read_text(encoding="utf-8"))
    assert report.passed is True
    assert report.committed is False
    assert report.repair_actions == 2
    assert len(applier.calls) == 1
    assert [change.operation.value for change in applier.calls[0].changes] == ["update", "delete"]
    assert applier.calls[0].changes[0].position.startswith("repair:")
    assert applier.calls[0].changes[0].metadata["repair_action_id"] == "repair:orders-cdc:1"
    assert payload["schema_version"] == "dpone.cdc_repair_execution.v1"
    assert payload["committed"] is False
    assert payload["sink_receipt"]["rows_applied"] == 2
