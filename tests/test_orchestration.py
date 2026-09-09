from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.cli import main as cli_main
from dpone.contracts.process_types import ProcessResult
from dpone.orchestration.handoff import SchedulerHandoffBuilder
from dpone.orchestration.locks import LocalRunLockManager
from dpone.orchestration.run import OrchestratedRunRequest, OrchestratedRunService
from dpone.orchestration.state import LocalJobStateStore
from dpone.services.run_manifest import RunManifestResult


class _LoggerStub:
    def error(self, message: str) -> None:
        del message

    def info(self, message: str) -> None:
        del message


def _patch_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def _green_result(tmp_path: Path, *, run_id: str = "run_01") -> RunManifestResult:
    return RunManifestResult(
        manifest=str(tmp_path / "manifest.yml"),
        process="orders",
        selector="orders",
        run_id=run_id,
        passed=True,
        attempts=2,
        max_attempts=3,
        retry_backoff_seconds=1.5,
        result=ProcessResult(
            status="success",
            inserted_rows=10,
            updated_rows=2,
            final_rows=12,
            extracted_rows=12,
            duration_seconds=0.25,
            errors=[],
        ),
    )


def test_local_run_lock_blocks_concurrent_key_and_releases(tmp_path: Path) -> None:
    manager = LocalRunLockManager(tmp_path / "locks")

    first = manager.acquire("orders", ttl_seconds=60)
    second = manager.acquire("orders", ttl_seconds=60)
    manager.release(first)
    third = manager.acquire("orders", ttl_seconds=60)

    assert first.acquired is True
    assert second.acquired is False
    assert second.blocker == "lock.concurrent_run"
    assert third.acquired is True
    manager.release(third)


def test_orchestrated_run_service_runs_under_lock_and_writes_artifacts(tmp_path: Path) -> None:
    calls: list[OrchestratedRunRequest] = []

    def executor(request: OrchestratedRunRequest) -> RunManifestResult:
        calls.append(request)
        return _green_result(tmp_path, run_id=request.run_id or "orders")

    request = OrchestratedRunRequest(
        manifest_path=tmp_path / "manifest.yml",
        selector="orders",
        run_id="run_01",
        retry_attempts=2,
        retry_backoff_seconds=1.5,
        concurrency_key="orders",
    )

    report = OrchestratedRunService(
        lock_manager=LocalRunLockManager(tmp_path / "locks"),
        handoff_builder=SchedulerHandoffBuilder(),
        run_executor=executor,
    ).run(output_dir=tmp_path / "orchestration", request=request)

    assert report.passed is True
    assert report.lock_acquired is True
    assert report.run_id == "run_01"
    assert report.run_result["attempts"] == 2
    assert "dpone orchestrate run" in report.scheduler_handoff["cron_command"]
    assert (tmp_path / "orchestration" / "orchestrated_run.json").exists()
    assert (tmp_path / "orchestration" / "orchestrated_run.md").exists()
    assert calls == [request]

    free_after_run = LocalRunLockManager(tmp_path / "locks").acquire("orders", ttl_seconds=60)
    assert free_after_run.acquired is True


def test_local_job_state_store_records_lifecycle_and_resumable_failures(tmp_path: Path) -> None:
    store = LocalJobStateStore(tmp_path / "state")

    started = store.mark_started(
        run_id="run_01",
        process="orders",
        manifest_path=tmp_path / "manifest.yml",
        selector="orders",
        lock_key="orders",
        output_dir=tmp_path / "orchestration",
    )
    running = store.mark_running(run_id="run_01", attempt=1)
    failed = store.mark_failed(run_id="run_01", blockers=("run.not_passed",), run_result={"passed": False})

    assert started.status == "started"
    assert running.status == "running"
    assert failed.status == "failed"
    assert failed.resumable is True
    assert failed.transitions == ("started", "running", "failed")
    loaded = store.get("run_01")
    assert loaded is not None
    assert loaded.status == "failed"
    assert loaded.blockers == ("run.not_passed",)
    assert Path(loaded.state_path).exists()


def test_orchestrated_run_service_persists_committed_job_state(tmp_path: Path) -> None:
    store = LocalJobStateStore(tmp_path / "state")

    def executor(request: OrchestratedRunRequest) -> RunManifestResult:
        return _green_result(tmp_path, run_id=request.run_id or "orders")

    report = OrchestratedRunService(
        lock_manager=LocalRunLockManager(tmp_path / "locks"),
        handoff_builder=SchedulerHandoffBuilder(),
        run_executor=executor,
        job_state_store=store,
    ).run(
        output_dir=tmp_path / "orchestration",
        request=OrchestratedRunRequest(
            manifest_path=tmp_path / "manifest.yml",
            selector="orders",
            run_id="run_01",
            concurrency_key="orders",
        ),
    )

    state = store.get("run_01")
    assert state is not None
    assert state.status == "committed"
    assert state.resumable is False
    assert state.transitions == ("started", "running", "committed")
    assert state.run_result["passed"] is True
    assert report.job_state["status"] == "committed"
    assert report.job_state["state_path"] == state.state_path


def test_scheduler_handoff_uses_orchestrate_run_with_state_and_lock_controls() -> None:
    handoff = SchedulerHandoffBuilder().build(
        manifest_path="manifests/orders.yml",
        selector="orders",
        run_id="run_01",
        retry_attempts=2,
        retry_backoff_seconds=1.5,
        concurrency_key="orders_daily",
        lock_dir=".dpone/locks",
        state_dir=".dpone/orchestration-state",
        output_dir=".dpone/orchestration/run_01",
        lock_ttl_seconds=900,
        resume_policy="resume",
    )

    assert handoff.cron_command.startswith("dpone orchestrate run")
    assert "--manifest manifests/orders.yml" in handoff.cron_command
    assert "--selector orders" in handoff.cron_command
    assert "--concurrency-key orders_daily" in handoff.cron_command
    assert "--lock-dir .dpone/locks" in handoff.cron_command
    assert "--state-dir .dpone/orchestration-state" in handoff.cron_command
    assert "--output-dir .dpone/orchestration/run_01" in handoff.cron_command
    assert "--lock-ttl-seconds 900" in handoff.cron_command
    assert "--resume-policy resume" in handoff.cron_command
    assert "BashOperator" in handoff.airflow_task
    assert "@asset" in handoff.dagster_asset
    assert "kind: CronJob" in handoff.kubernetes_cronjob


def test_orchestrated_run_blocks_previous_resumable_state_by_default(tmp_path: Path) -> None:
    store = LocalJobStateStore(tmp_path / "state")
    store.mark_started(
        run_id="run_01",
        process="orders",
        manifest_path=tmp_path / "manifest.yml",
        selector="orders",
        lock_key="orders",
        output_dir=tmp_path / "orchestration",
    )
    store.mark_failed(run_id="run_01", blockers=("run.not_passed",), run_result={"passed": False})
    calls: list[OrchestratedRunRequest] = []

    def executor(request: OrchestratedRunRequest) -> RunManifestResult:
        calls.append(request)
        return _green_result(tmp_path, run_id="run_01")

    report = OrchestratedRunService(
        lock_manager=LocalRunLockManager(tmp_path / "locks"),
        handoff_builder=SchedulerHandoffBuilder(),
        run_executor=executor,
        job_state_store=store,
    ).run(
        output_dir=tmp_path / "orchestration",
        request=OrchestratedRunRequest(
            manifest_path=tmp_path / "manifest.yml",
            selector="orders",
            run_id="run_01",
            concurrency_key="orders",
        ),
    )

    assert report.passed is False
    assert report.blockers == ("job_state.resume_required",)
    assert calls == []


def test_orchestrated_run_resume_policy_continues_previous_failed_state(tmp_path: Path) -> None:
    store = LocalJobStateStore(tmp_path / "state")
    store.mark_started(
        run_id="run_01",
        process="orders",
        manifest_path=tmp_path / "manifest.yml",
        selector="orders",
        lock_key="orders",
        output_dir=tmp_path / "orchestration",
    )
    store.mark_failed(run_id="run_01", blockers=("run.not_passed",), run_result={"passed": False})

    def executor(request: OrchestratedRunRequest) -> RunManifestResult:
        return _green_result(tmp_path, run_id="run_01")

    report = OrchestratedRunService(
        lock_manager=LocalRunLockManager(tmp_path / "locks"),
        handoff_builder=SchedulerHandoffBuilder(),
        run_executor=executor,
        job_state_store=store,
    ).run(
        output_dir=tmp_path / "orchestration",
        request=OrchestratedRunRequest(
            manifest_path=tmp_path / "manifest.yml",
            selector="orders",
            run_id="run_01",
            concurrency_key="orders",
            resume_policy="resume",
        ),
    )

    assert report.passed is True
    assert report.job_state["status"] == "committed"
    assert report.job_state["transitions"] == ["started", "failed", "started", "running", "committed"]


def test_orchestrated_run_service_blocks_when_lock_is_active(tmp_path: Path) -> None:
    lock_manager = LocalRunLockManager(tmp_path / "locks")
    active_lock = lock_manager.acquire("orders", ttl_seconds=60)
    calls: list[OrchestratedRunRequest] = []

    def executor(request: OrchestratedRunRequest) -> RunManifestResult:
        calls.append(request)
        return _green_result(tmp_path)

    report = OrchestratedRunService(
        lock_manager=lock_manager,
        handoff_builder=SchedulerHandoffBuilder(),
        run_executor=executor,
    ).run(
        output_dir=tmp_path / "orchestration",
        request=OrchestratedRunRequest(
            manifest_path=tmp_path / "manifest.yml",
            selector="orders",
            run_id="run_01",
            concurrency_key="orders",
        ),
    )

    assert report.passed is False
    assert report.blockers == ("lock.concurrent_run",)
    assert calls == []
    lock_manager.release(active_lock)


def test_orchestrate_run_cli_outputs_json(monkeypatch: pytest.MonkeyPatch, capsys, tmp_path: Path) -> None:
    _patch_cli(monkeypatch)

    class Service:
        def run(self, **kwargs):
            assert kwargs["retry_attempts"] == 2
            assert kwargs["retry_backoff_seconds"] == 1.25
            assert kwargs["selector"] == "orders"
            return _green_result(tmp_path, run_id=kwargs["run_id"])

    import dpone.commands.orchestrate_cmd as orchestrate_cmd

    monkeypatch.setattr(orchestrate_cmd, "RunManifestService", Service)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "orchestrate",
                "run",
                "--manifest",
                str(tmp_path / "manifest.yml"),
                "--selector",
                "orders",
                "--run-id",
                "run_01",
                "--retry-attempts",
                "2",
                "--retry-backoff-seconds",
                "1.25",
                "--lock-dir",
                str(tmp_path / "locks"),
                "--state-dir",
                str(tmp_path / "state"),
                "--resume-policy",
                "resume",
                "--output-dir",
                str(tmp_path / "orchestration"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is True
    assert payload["run_id"] == "run_01"
    assert payload["lock_acquired"] is True
    assert payload["job_state"]["status"] == "committed"
    assert (tmp_path / "state" / "run_01.job_state.json").exists()


def test_orchestration_docs_cover_durable_state_contract() -> None:
    root = Path(__file__).resolve().parents[1]
    guide = (root / "docs" / "orchestration.md").read_text(encoding="utf-8")
    cli_reference = (root / "docs" / "cli-reference.md").read_text(encoding="utf-8")

    assert "--state-dir" in guide
    assert "--resume-policy fail" in guide
    assert "job_state.resume_required" in guide
    assert "dpone orchestrate run`, not bare `dpone run`" in guide
    assert "kubernetes_cronjob" in guide
    assert "LocalJobStateStore" in guide
    assert "stateDiagram-v2" in guide
    assert "--state-dir STATE_DIR" in cli_reference
    assert "--resume-policy {fail,resume,restart}" in cli_reference
