"""Offline command dispatch authorization; no SQL or Linux certification."""

from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.adapters.composition_dbt_command_runner import ProtectedDbtCommandRunner, TrustedDbtCommand
from dpone.contracts.composition_dbt_outcome import DbtCaptureError, DbtChildExit
from dpone.ports.dbt_publishing import DbtCommandResult
from tests.test_composition_dbt_capture import intent


def setup_runner(tmp_path, *, fault=None):
    value = replace(intent(tmp_path), supervisor_uid=0)
    command = TrustedDbtCommand("build", ("dbt", "build"), Path("/project"), 20)
    calls = []

    def register(attempt):
        calls.append("register")
        if fault == "register":
            raise DbtCaptureError("unknown")
        return replace(value, argv=("/wrong/dbt", "build")) if fault == "intent" else value

    def dispatch(attempt):
        calls.append("dispatch")
        if fault == "dispatch":
            raise DbtCaptureError("unknown")
        return DbtChildExit(123, 456, 7)

    runner = ProtectedDbtCommandRunner(
        attempt=value.attempt,
        trusted_commands=lambda: (command, TrustedDbtCommand("preflight", ("dbt", "parse"), Path("/project"), 20)),
        trusted_intent=lambda: value,
        store=SimpleNamespace(register=register),
        capture=SimpleNamespace(dispatch=dispatch),
        preflight_runner=SimpleNamespace(run=lambda *a, **kw: calls.append("parse") or DbtCommandResult(0)),
    )
    return runner, calls


def test_build_preserves_exit_and_rejects_replay(tmp_path):
    runner, calls = setup_runner(tmp_path)
    assert runner.run(("dbt", "build"), cwd=Path("/project"), timeout_seconds=20, redactions=()).exit_code == 7
    with pytest.raises(DbtCaptureError, match="replay"):
        runner.run(("dbt", "build"), cwd=Path("/project"), timeout_seconds=20, redactions=())
    assert calls == ["register", "dispatch"]


@pytest.mark.parametrize("fault", ["register", "dispatch", "intent"])
def test_uncertain_ack_or_intent_conflict_never_relaunches(tmp_path, fault):
    runner, calls = setup_runner(tmp_path, fault=fault)
    for _ in range(2):
        with pytest.raises(DbtCaptureError):
            runner.run(("dbt", "build"), cwd=Path("/project"), timeout_seconds=20, redactions=())
    assert calls.count("register") == 1
    assert calls.count("dispatch") == (1 if fault == "dispatch" else 0)


@pytest.mark.parametrize(
    "args,cwd,timeout",
    [
        (("dbt", "build", "--full-refresh"), "/project", 20),
        (("dbt", "build"), "/other", 20),
        (("dbt", "build"), "/project", 21),
        (("/evil/dbt", "build"), "/project", 20),
        (("dbt", "run-operation"), "/project", 20),
    ],
)
def test_substitution_rejected_before_side_effects(tmp_path, args, cwd, timeout):
    runner, calls = setup_runner(tmp_path)
    with pytest.raises(DbtCaptureError):
        runner.run(args, cwd=Path(cwd), timeout_seconds=timeout, redactions=("secret",))
    assert calls == []


def test_preflight_delegates_only_exact_authorized_command(tmp_path):
    runner, calls = setup_runner(tmp_path)
    assert runner.run(("dbt", "parse"), cwd=Path("/project"), timeout_seconds=20, redactions=()).exit_code == 0
    assert calls == ["parse"]


@pytest.mark.parametrize("change", [{"working_directory": "/other"}, {"timeout_seconds": 19}])
def test_intent_launch_settings_must_match_command(tmp_path, change):
    runner, calls = setup_runner(tmp_path)
    expected = runner._intent()
    runner._intent = lambda: replace(expected, **change)
    with pytest.raises(DbtCaptureError, match="command_mismatch"):
        runner.run(("dbt", "build"), cwd=Path("/project"), timeout_seconds=20, redactions=())
    assert calls == []
