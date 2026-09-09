from __future__ import annotations

import json
from pathlib import Path

from dpone.ops.route_refresh_execute import RouteRefreshExecutionService
from dpone.ops.route_refresh_plan import RouteRefreshPlanService
from dpone.ops.routes.refresh_execution_executor import RouteRefreshChunkExecutionResult


def _plan(
    tmp_path: Path,
    *,
    source: str = "mssql",
    sink: str = "clickhouse",
    strategy: str = "incremental_merge",
    destructive: bool = False,
    current_state: str = "",
    target_state: str = "",
    invalid_window: bool = False,
) -> Path:
    report = RouteRefreshPlanService().plan(
        output_dir=tmp_path / "plan",
        source=source,
        sink=sink,
        strategy=strategy,
        dataset="analytics.orders",
        reason="dq_repair",
        window_kind="integer",
        start="100" if invalid_window else "1",
        end="10" if invalid_window else "25",
        chunk_size=10,
        destructive=destructive,
        current_state=current_state,
        target_state=target_state,
    )
    return Path(report.json_path)


class _RecordingRefreshExecutor:
    def __init__(self, *, fail_ordinal: int | None = None) -> None:
        self.fail_ordinal = fail_ordinal
        self.calls: list[tuple[int, str, bool]] = []

    def execute_chunk(self, request: object) -> RouteRefreshChunkExecutionResult:
        ordinal = int(getattr(request, "ordinal"))
        idempotency_key = str(getattr(request, "idempotency_key"))
        execute = bool(getattr(request, "execute"))
        self.calls.append((ordinal, idempotency_key, execute))
        if ordinal == self.fail_ordinal:
            return RouteRefreshChunkExecutionResult(
                ordinal=ordinal,
                idempotency_key=idempotency_key,
                status="failed",
                passed=False,
                rows_read=10,
                rows_written=5,
                artifact_path="",
                summary="chunk failed",
                blockers=("sink_apply_failed",),
            )
        return RouteRefreshChunkExecutionResult(
            ordinal=ordinal,
            idempotency_key=idempotency_key,
            status="succeeded",
            passed=True,
            rows_read=10,
            rows_written=10,
            artifact_path="",
            summary="chunk applied",
        )


def test_route_refresh_execute_dry_run_writes_planned_receipt(tmp_path: Path) -> None:
    plan_json = _plan(tmp_path)

    report = RouteRefreshExecutionService().execute(
        route_refresh_plan_json=plan_json,
        output_dir=tmp_path / "execute",
        runner_id="operator-a",
    )

    payload = json.loads(Path(report.json_path).read_text(encoding="utf-8"))
    markdown = Path(report.markdown_path).read_text(encoding="utf-8")

    assert payload["schema_version"] == "dpone.route_refresh_execution.v1"
    assert payload["route"]["case_id"] == "mssql_to_clickhouse__incremental_merge"
    assert payload["dataset"] == "analytics.orders"
    assert payload["mode"] == "dry_run"
    assert payload["executed"] is False
    assert payload["status"] == "dry_run"
    assert payload["passed"] is True
    assert payload["ready_for_state_promotion"] is False
    assert [item["status"] for item in payload["chunks"]] == ["planned", "planned", "planned"]
    assert payload["plan_sha256"] != "0" * 64
    assert "Route refresh execution" in markdown


def test_route_refresh_execute_calls_injected_executor_when_explicitly_enabled(tmp_path: Path) -> None:
    plan_json = _plan(tmp_path)
    executor = _RecordingRefreshExecutor()

    report = RouteRefreshExecutionService(executor=executor).execute(
        route_refresh_plan_json=plan_json,
        output_dir=tmp_path / "execute",
        runner_id="operator-a",
        execute=True,
    )

    payload = json.loads(Path(report.json_path).read_text(encoding="utf-8"))

    assert report.passed is True
    assert payload["mode"] == "execute"
    assert payload["executed"] is True
    assert payload["status"] == "succeeded"
    assert payload["ready_for_state_promotion"] is True
    assert [call[0] for call in executor.calls] == [1, 2, 3]
    assert all(call[2] is True for call in executor.calls)
    assert [(item["start"], item["end"]) for item in payload["chunks"]] == [("1", "10"), ("11", "20"), ("21", "25")]
    assert [(item["source_boundary"], item["sink_boundary"]) for item in payload["chunks"]] == [
        ("1..10", "1..10"),
        ("11..20", "11..20"),
        ("21..25", "21..25"),
    ]
    assert payload["summary"]["chunks_total"] == 3
    assert payload["summary"]["chunks_succeeded"] == 3
    assert payload["summary"]["rows_written"] == 30


def test_route_refresh_execute_blocks_approval_required_plan_before_executor(tmp_path: Path) -> None:
    plan_json = _plan(
        tmp_path,
        source="postgres",
        sink="mssql",
        destructive=True,
        current_state="lsn:200",
        target_state="lsn:100",
    )
    executor = _RecordingRefreshExecutor()

    report = RouteRefreshExecutionService(executor=executor).execute(
        route_refresh_plan_json=plan_json,
        output_dir=tmp_path / "execute",
        runner_id="operator-a",
        execute=True,
    )

    assert report.status == "approval_required"
    assert report.passed is False
    assert "route_refresh_execution.plan_approval_required" in report.blockers
    assert executor.calls == []


def test_route_refresh_execute_stops_after_failed_required_chunk(tmp_path: Path) -> None:
    plan_json = _plan(tmp_path)
    executor = _RecordingRefreshExecutor(fail_ordinal=2)

    report = RouteRefreshExecutionService(executor=executor).execute(
        route_refresh_plan_json=plan_json,
        output_dir=tmp_path / "execute",
        runner_id="operator-a",
        execute=True,
    )

    assert report.status == "partial_failure"
    assert report.passed is False
    assert "route_refresh_execution.chunk_failed:2" in report.blockers
    assert [item.status for item in report.chunks] == ["succeeded", "failed", "skipped"]
    assert [call[0] for call in executor.calls] == [1, 2]
    assert report.ready_for_state_promotion is False


def test_route_refresh_execute_blocks_route_mismatch_and_invalid_plan(tmp_path: Path) -> None:
    mismatch = RouteRefreshExecutionService().execute(
        route_refresh_plan_json=_plan(tmp_path),
        output_dir=tmp_path / "mismatch",
        runner_id="operator-a",
        source="postgres",
        sink="mssql",
        strategy="incremental_merge",
    )
    invalid = RouteRefreshExecutionService().execute(
        route_refresh_plan_json=_plan(tmp_path / "invalid", invalid_window=True),
        output_dir=tmp_path / "invalid-execute",
        runner_id="operator-a",
    )

    assert mismatch.status == "blocked"
    assert "route_refresh_execution.route_mismatch" in mismatch.blockers
    assert invalid.status == "blocked"
    assert "route_refresh_execution.plan_blocked" in invalid.blockers
