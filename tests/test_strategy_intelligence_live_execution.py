from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.cli import main as cli_main
from dpone.strategy_intelligence.adaptive import AdaptiveBatchController, BatchObservation
from dpone.strategy_intelligence.live_preflight import LivePreflightProbe, LivePreflightService, ProbeCheck
from dpone.strategy_intelligence.replay import ReplayExecutionRequest, ReplayExecutionService


class _LoggerStub:
    def error(self, message: str) -> None:
        del message

    def info(self, message: str) -> None:
        del message


class _Probe(LivePreflightProbe):
    def check_odbc(self) -> ProbeCheck:
        return ProbeCheck("odbc", True, "ODBC Driver 18 is installed")

    def check_permissions(self, schema: str) -> ProbeCheck:
        return ProbeCheck("permissions", schema == "staging", f"schema={schema}")

    def check_staging_schema(self, schema: str) -> ProbeCheck:
        return ProbeCheck("staging_schema", True, f"schema={schema}")

    def check_lock_risk(self, schema: str, table: str) -> ProbeCheck:
        return ProbeCheck("lock_risk", table != "locked_orders", f"{schema}.{table}")


def _patch_context(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def test_live_preflight_aggregates_probe_checks_and_actions() -> None:
    result = LivePreflightService(probe=_Probe()).check_target(
        source_type="postgres",
        sink_type="mssql",
        target_schema="landing",
        target_table="locked_orders",
        staging_schema="staging",
    )

    assert result.ready is False
    assert result.native_fast_path == "postgres_copy_to_mssql_bcp"
    assert result.checks["odbc"].passed is True
    assert result.checks["permissions"].passed is True
    assert result.checks["lock_risk"].passed is False
    assert any("Resolve lock risk" in action for action in result.actions)


def test_adaptive_batch_controller_increases_decreases_and_bounds_batch_size() -> None:
    controller = AdaptiveBatchController(initial_batch_size=50_000, min_batch_size=10_000, max_batch_size=200_000)

    faster = controller.observe(BatchObservation(rows=50_000, duration_seconds=1.0, target_backpressure=0.05))
    slower = controller.observe(BatchObservation(rows=10_000, duration_seconds=10.0, target_backpressure=0.90))

    assert faster.next_batch_size == 75_000
    assert faster.reason == "increase_throughput"
    assert slower.next_batch_size == 37_500
    assert slower.reason == "target_backpressure"


def test_replay_execution_service_is_dry_run_by_default_and_requires_yes_for_execution(tmp_path: Path) -> None:
    service = ReplayExecutionService(artifact_dir=tmp_path)

    preview = service.execute(
        ReplayExecutionRequest(
            action="resync",
            run_id="RUN_ID",
            source_type="postgres",
            sink_type="mssql",
            strategy_mode="partition_replace",
            partitions=("2026-01-01",),
            yes=False,
        )
    )
    executed = service.execute(
        ReplayExecutionRequest(
            action="resync",
            run_id="RUN_ID",
            source_type="postgres",
            sink_type="mssql",
            strategy_mode="partition_replace",
            partitions=("2026-01-01",),
            yes=True,
        )
    )

    assert preview.mode == "plan"
    assert preview.executed is False
    assert preview.artifact_path.exists()
    assert executed.mode == "execute"
    assert executed.executed is True
    assert json.loads(executed.artifact_path.read_text(encoding="utf-8"))["status"] == "executed"


def test_resync_cli_defaults_to_plan_and_executes_only_with_yes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_context(monkeypatch)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "resync",
                "--run-id",
                "RUN_ID",
                "--source-type",
                "postgres",
                "--sink-type",
                "mssql",
                "--strategy",
                "partition_replace",
                "--partition",
                "2026-01-01",
                "--artifact-dir",
                str(tmp_path),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["mode"] == "plan"
    assert payload["executed"] is False

    with pytest.raises(SystemExit) as exc_yes:
        cli_main.main(
            [
                "resync",
                "--run-id",
                "RUN_ID",
                "--source-type",
                "postgres",
                "--sink-type",
                "mssql",
                "--strategy",
                "partition_replace",
                "--partition",
                "2026-01-01",
                "--artifact-dir",
                str(tmp_path),
                "--yes",
                "--format",
                "json",
            ]
        )

    assert exc_yes.value.code == 0
    payload_yes = json.loads(capsys.readouterr().out)
    assert payload_yes["mode"] == "execute"
    assert payload_yes["executed"] is True


def test_resume_cli_builds_resume_artifact(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    _patch_context(monkeypatch)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "resume",
                "RUN_ID",
                "--from-stage",
                "stage",
                "--source-type",
                "postgres",
                "--sink-type",
                "mssql",
                "--strategy",
                "incremental_merge",
                "--artifact-dir",
                str(tmp_path),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["action"] == "resume"
    assert payload["mode"] == "plan"
    assert payload["failed_stage"] == "stage"
