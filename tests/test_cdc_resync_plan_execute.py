from __future__ import annotations

import json
from pathlib import Path

from dpone.readiness.cdc import CDCBackend, CDCOffset
from dpone.runtime.cdc.base import CDCBatch
from dpone.runtime.cdc.resync import CdcResyncExecutionService
from dpone.runtime.cdc.retention import CdcResyncPlanner, CdcRetentionGapService, CdcRetentionPolicy
from dpone.runtime.cdc.retention_models import CdcRetentionBounds
from dpone.runtime.cdc.retention_probes import StaticCdcRetentionProbe
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
            artifact_uri="memory://resync",
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


def _write_rows(path: Path) -> Path:
    path.write_text(
        json.dumps(
            {"rows": [{"order_id": 1, "status": "paid"}, {"order_id": 2, "status": "open"}]},
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return path


def _gap_report(tmp_path: Path):
    return CdcRetentionGapService(policy=CdcRetentionPolicy(at_risk_margin=5)).evaluate(
        output_dir=tmp_path / "retention",
        stream=_stream(),
        committed_offset=CDCOffset(CDCBackend.MSSQL_CHANGE_TRACKING, "99", True),
        probe=StaticCdcRetentionProbe(
            CdcRetentionBounds(
                backend=CDCBackend.MSSQL_CHANGE_TRACKING,
                min_available_offset="100",
                high_watermark="150",
                current_offset="150",
            )
        ),
    )


def test_resync_planner_creates_bounded_snapshot_plan_for_gap(tmp_path: Path) -> None:
    retention_report = _gap_report(tmp_path)
    rows_json = _write_rows(tmp_path / "rows.json")

    plan_report = CdcResyncPlanner().plan(
        output_dir=tmp_path / "plan",
        retention_report=retention_report,
        rows_json=rows_json,
        max_rows=1,
    )

    payload = json.loads((tmp_path / "plan" / "cdc_resync_plan.json").read_text(encoding="utf-8"))
    assert plan_report.passed is True
    assert plan_report.plan.action_count == 1
    assert plan_report.plan.actions[0].operation == "upsert"
    assert plan_report.plan.actions[0].payload == {"order_id": 1, "status": "paid"}
    assert plan_report.metrics["max_rows"] == 1
    assert payload["schema_version"] == "dpone.cdc_resync_plan_report.v1"
    assert payload["plan"]["schema_version"] == "dpone.cdc_resync_plan.v1"
    assert (tmp_path / "plan" / "cdc_resync_plan.md").exists()


def test_resync_execution_applies_actions_without_committing_offsets(tmp_path: Path) -> None:
    retention_report = _gap_report(tmp_path)
    rows_json = _write_rows(tmp_path / "rows.json")
    plan_report = CdcResyncPlanner().plan(
        output_dir=tmp_path / "plan",
        retention_report=retention_report,
        rows_json=rows_json,
    )
    applier = _Applier()

    execution = CdcResyncExecutionService(sink_applier=applier).execute(
        output_dir=tmp_path / "execute",
        resync_plan_json=plan_report.plan.output_path or "",
        stream=_stream(),
        max_actions=1,
    )

    payload = json.loads((tmp_path / "execute" / "cdc_resync_execution.json").read_text(encoding="utf-8"))
    assert execution.passed is True
    assert execution.committed is False
    assert execution.resync_actions == 1
    assert len(applier.calls) == 1
    assert [change.operation.value for change in applier.calls[0].changes] == ["insert"]
    assert applier.calls[0].changes[0].position.startswith("resync:")
    assert applier.calls[0].changes[0].metadata["resync_action_id"].startswith("resync:")
    assert payload["schema_version"] == "dpone.cdc_resync_execution.v1"
    assert payload["committed"] is False
