from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from dpone.ops.route_execution import RouteExecutionService
from dpone.ops.routes.catalog import RouteProfileCatalog
from dpone.ops.routes.execution_models import RouteExecutionStage, RouteExecutionStatus
from dpone.ops.routes.models import RouteKey


class _Clock:
    def __init__(self) -> None:
        self.value = datetime(2026, 6, 13, 9, 0, tzinfo=UTC)

    def now(self) -> datetime:
        return self.value

    def advance(self, delta: timedelta) -> None:
        self.value += delta


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def test_route_execution_stage_order_is_append_only() -> None:
    assert RouteExecutionStage.PLANNED.can_transition_to(RouteExecutionStage.EXTRACTING)
    assert RouteExecutionStage.LOADED_TO_STAGING.can_transition_to(RouteExecutionStage.FINALIZED)
    assert not RouteExecutionStage.FINALIZED.can_transition_to(RouteExecutionStage.LOADED_TO_STAGING)
    assert RouteExecutionStatus.COMMITTED.is_terminal
    assert RouteExecutionStatus.FAILED.is_terminal


def test_route_profiles_require_execution_ledger_evidence() -> None:
    profile = RouteProfileCatalog.default().get(RouteKey.of("mssql", "clickhouse", "cdc"))

    assert profile is not None
    assert "route_execution_ledger" in profile.required_evidence


def test_route_execution_service_records_idempotent_steps_and_artifact_hashes(tmp_path: Path) -> None:
    artifact = _write_json(tmp_path / "apply.json", {"passed": True, "rows": 2})
    clock = _Clock()
    service = RouteExecutionService(clock=clock.now)

    first = service.record_step(
        output_dir=tmp_path / "ledger",
        source="mssql",
        sink="clickhouse",
        strategy="cdc",
        dataset="dbo.orders",
        run_id="run-1",
        stage="loaded_to_staging",
        status="succeeded",
        runner_id="worker-a",
        source_boundary="lsn:001",
        sink_boundary="clickhouse:events:10",
        artifact_paths={"cdc_apply": artifact},
        idempotency_key="load-window-001",
    )
    replay = service.record_step(
        output_dir=tmp_path / "ledger",
        source="mssql",
        sink="clickhouse",
        strategy="cdc",
        dataset="dbo.orders",
        run_id="run-1",
        stage="loaded_to_staging",
        status="succeeded",
        runner_id="worker-a",
        source_boundary="lsn:001",
        sink_boundary="clickhouse:events:10",
        artifact_paths={"cdc_apply": artifact},
        idempotency_key="load-window-001",
    )

    payload = json.loads(Path(replay.json_path).read_text(encoding="utf-8"))

    assert first.passed is True
    assert replay.passed is True
    assert "route_execution.idempotent_replay" in replay.warnings
    assert payload["schema_version"] == "dpone.route_execution_ledger.v1"
    assert payload["route"]["case_id"] == "mssql_to_clickhouse__cdc"
    assert payload["dataset"] == "dbo.orders"
    assert payload["step_count"] == 1
    assert payload["steps"][0]["stage"] == "loaded_to_staging"
    assert payload["steps"][0]["artifact_hashes"]["cdc_apply"]["sha256"] != "0" * 64
    assert Path(payload["json_path"]).exists()
    assert Path(payload["markdown_path"]).exists()
    markdown = Path(payload["markdown_path"]).read_text(encoding="utf-8")
    assert "- `route_execution.idempotent_replay`" in markdown


def test_route_execution_service_blocks_idempotency_conflicts(tmp_path: Path) -> None:
    service = RouteExecutionService(clock=_Clock().now)

    service.record_step(
        output_dir=tmp_path,
        source="postgres",
        sink="mssql",
        strategy="incremental_merge",
        dataset="public.orders",
        run_id="run-1",
        stage="loaded_to_staging",
        status="succeeded",
        runner_id="worker-a",
        source_boundary="xmin:10",
        sink_boundary="staging:1",
        idempotency_key="partition-001",
    )
    conflict = service.record_step(
        output_dir=tmp_path,
        source="postgres",
        sink="mssql",
        strategy="incremental_merge",
        dataset="public.orders",
        run_id="run-1",
        stage="loaded_to_staging",
        status="succeeded",
        runner_id="worker-a",
        source_boundary="xmin:10",
        sink_boundary="staging:2",
        idempotency_key="partition-001",
    )

    assert conflict.passed is False
    assert "route_execution.idempotency_conflict" in conflict.blockers


def test_commit_protocol_blocks_state_commit_before_durable_sink_success(tmp_path: Path) -> None:
    report = RouteExecutionService(clock=_Clock().now).record_step(
        output_dir=tmp_path,
        source="mssql",
        sink="clickhouse",
        strategy="cdc",
        dataset="dbo.orders",
        run_id="run-1",
        stage="state_committed",
        status="committed",
        runner_id="worker-a",
        source_boundary="lsn:001",
        sink_boundary="checkpoint:001",
    )

    assert report.passed is False
    assert "route_execution.state_commit_before_sink_success" in report.blockers
    assert (
        "Record a successful `loaded_to_staging`, `finalized`, or `quality_checked` step before committing state."
        in (report.next_actions)
    )


def test_route_execution_lease_fences_parallel_runners_until_ttl_expires(tmp_path: Path) -> None:
    clock = _Clock()
    service = RouteExecutionService(clock=clock.now)

    first = service.record_step(
        output_dir=tmp_path,
        source="mssql",
        sink="clickhouse",
        strategy="cdc",
        dataset="dbo.orders",
        run_id="run-1",
        stage="extracting",
        status="running",
        runner_id="worker-a",
        lease_ttl_seconds=60,
    )
    blocked = service.record_step(
        output_dir=tmp_path,
        source="mssql",
        sink="clickhouse",
        strategy="cdc",
        dataset="dbo.orders",
        run_id="run-2",
        stage="extracting",
        status="running",
        runner_id="worker-b",
        lease_ttl_seconds=60,
    )
    clock.advance(timedelta(seconds=61))
    takeover = service.record_step(
        output_dir=tmp_path,
        source="mssql",
        sink="clickhouse",
        strategy="cdc",
        dataset="dbo.orders",
        run_id="run-2",
        stage="extracting",
        status="running",
        runner_id="worker-b",
        lease_ttl_seconds=60,
    )

    assert first.passed is True
    assert blocked.passed is False
    assert "route_execution.lease_held_by_another_runner" in blocked.blockers
    assert takeover.passed is True
    assert takeover.lease is not None
    assert takeover.lease.owner == "worker-b"
