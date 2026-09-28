"""Synthetic Airflow invocation and outcome boundaries; no scheduler certification."""

from __future__ import annotations

import io
import json
from contextlib import redirect_stderr, redirect_stdout

import pytest

from dpone import api
from dpone.contracts.quality_replay import ReplayQualityEvidenceError
from dpone.gitops.airflow_runtime_executor import AirflowCommandResult, GitOpsAirflowRunSpecExecutor
from tests.test_declarative_replay_cli import install_replay_runtime, invoke, replay_manifest, run_args


def scheduler_environment(monkeypatch, *, run_id="scheduled__2026-09-28", attempt="1"):
    monkeypatch.setenv("DPONE_DAG_ID", "replay_demo")
    monkeypatch.setenv("DPONE_DAG_RUN_ID", run_id)
    monkeypatch.setenv("DPONE_TRY_NUMBER", attempt)


@pytest.mark.parametrize("target", [False, True], ids=["source-staged", "target"])
def test_in_process_airflow_attempt_change_preserves_operation(monkeypatch, tmp_path, target):
    runtime = install_replay_runtime(monkeypatch, target=target, initial_complete=False)
    path = replay_manifest(tmp_path, target=target)
    scheduler_environment(monkeypatch)
    original = api.run(path)
    scheduler_environment(monkeypatch, attempt="2")
    retried = api.run(path)
    assert original.passed and retried.passed
    assert original.run_id == retried.run_id == "scheduled__2026-09-28"
    assert runtime.identities == [original.run_id, retried.run_id]
    if target:
        assert runtime.rig.reader.scans == 1
    assert runtime.rig.ddl.dispatches == runtime.rig.ddl.cleanup_dispatches == 1


def test_in_process_quality_failure_raises_before_caller_success(monkeypatch, tmp_path):
    runtime = install_replay_runtime(monkeypatch, fail_quality=True)
    scheduler_environment(monkeypatch)
    path = replay_manifest(tmp_path)
    with pytest.raises(ReplayQualityEvidenceError):
        api.run(path)
    assert runtime.rig.ddl.dispatches == 1


def test_airflow_run_spec_stops_before_success_dependent_asset_step(monkeypatch, tmp_path):
    runtime = install_replay_runtime(monkeypatch, fail_quality=True)
    scheduler_environment(monkeypatch)
    args = run_args(replay_manifest(tmp_path), run_id=None)
    commands = []

    class InProcessCommandRunner:
        """Exercise the actual CLI entry without starting a database or pod."""

        def run(self, *, command, cwd):
            commands.append(command)
            assert command == "execute_replay", "success-dependent asset step must not run"
            stdout, stderr = io.StringIO(), io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                code = invoke(args)
            return AirflowCommandResult(code, stdout.getvalue(), stderr.getvalue())

    evidence = GitOpsAirflowRunSpecExecutor(runner=InProcessCommandRunner()).execute(
        run_spec_path="synthetic-run-spec.json",
        run_spec={
            "steps": [
                {"name": "refresh", "kind": "dpone_run", "command": "execute_replay", "required": True},
                {"name": "success_asset", "kind": "asset", "command": "emit_success", "required": True},
            ]
        },
        cwd=tmp_path,
    )
    assert evidence.status == "failed"
    assert commands == ["execute_replay"]
    assert len(evidence.steps) == 1
    assert evidence.steps[0].exit_code == 1
    assert json.loads(evidence.steps[0].stdout)["passed"] is False
    assert "DPONE_REPLAY_QUALITY_EVIDENCE_" in evidence.steps[0].stderr
    assert runtime.rig.ddl.dispatches == 1


def test_new_airflow_dag_run_reaches_a_new_source_operation(monkeypatch, tmp_path):
    runtime = install_replay_runtime(monkeypatch)
    path = replay_manifest(tmp_path)
    scheduler_environment(monkeypatch)
    assert api.run(path).passed
    runtime.allow_source_boundary = True
    scheduler_environment(monkeypatch, run_id="scheduled__2026-09-29")
    with pytest.raises(RuntimeError, match="synthetic new operation source boundary"):
        api.run(path)
    assert runtime.identities == ["scheduled__2026-09-28", "scheduled__2026-09-29"]
    assert runtime.source_boundaries == 1
    assert runtime.rig.ddl.dispatches == 1


def test_provider_import_and_declarative_parse_do_not_open_runtime_io():
    import subprocess
    import sys

    from tests.test_declarative_replay_cli import EXAMPLES, ROOT

    script = """
import socket
import subprocess
import sys
from pathlib import Path

def forbidden(*args, **kwargs):
    raise AssertionError("parse/import opened runtime I/O")

socket.socket.connect = forbidden
subprocess.Popen = forbidden
import dpone
import dpone_airflow_pack
from dpone.manifest.loader import ManifestLoaderRouter
for path in sys.argv[1:]:
    loaded = ManifestLoaderRouter().load(Path(path), metadata_only=True)
    assert loaded.processes[0].config.load_config.options["durable_quality_replay"] is True
assert "clickhouse_driver" not in sys.modules
"""
    result = subprocess.run(
        [sys.executable, "-c", script, *(str(path) for path in EXAMPLES)],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, result.stderr
