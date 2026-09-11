"""Public verified-runtime execution admission for authenticated v3 composition.

Every composition test makes generic child startup explode, so a silent
fallback to native-v2 or shell execution cannot produce a passing result.
"""

from __future__ import annotations

import base64
import json
import subprocess
from dataclasses import replace

import pytest
from dpone_airflow_pack.run_identity import encode_composition_supervisor

from dpone.contracts.release_composition import COMPOSITION_ADMISSION
from dpone.runtime.composition_verified_dispatch import (
    COMPOSITION_DISPATCH_REJECTED,
    COMPOSITION_SUPERVISOR_B64_ENV,
)
from dpone.runtime.verified_pack_execution import execute_verified_pack_command
from dpone.runtime.verified_pack_launcher import (
    RUNTIME_RELEASE_ADMISSION_ENV,
    VerifiedPackCommand,
)

SENTINEL = "SENSITIVE_SENTINEL"
CREDENTIAL_ENV = "DPONE_TEST_CONNECTION_PASSWORD"
SUPERVISOR = {
    "schema": "dpone.composition-supervisor.v1",
    "persistent_volume_claim": "dpone-composition-supervisor",
    "child_uid_start": 1_000_000_000,
    "child_gid_start": 1_000_000_000,
    "child_identity_count": 1_000_000,
}
ORDINARY_ARGV = ("dpone", "run", "manifest.json", "--format", "json")
SELECTOR_ARGV = (*ORDINARY_ARGV, "--selector", "orders")
NATIVE_DBT_ARGV = ("dpone", "dbt", "execute-pack", "dbt/execution-pack.json", "--format", "json")


class RecordingDispatcher:
    """Injected worker seam recording every admitted composition request."""

    def __init__(self, *, status: int = 0, error: BaseException | None = None) -> None:
        self.requests: list = []
        self._status = status
        self._error = error

    def run(self, request):
        self.requests.append(request)
        if self._error is not None:
            raise self._error
        return self._status


class RecordedRun:
    """Minimal ``subprocess.run`` result for the preserved legacy child path."""

    def __init__(self, returncode: int) -> None:
        self.returncode = returncode


@pytest.fixture
def run_volume(tmp_path):
    return {
        "run_output_dir": tmp_path / "run",
        "xcom_return_path": tmp_path / "xcom" / "return.json",
    }


@pytest.fixture
def command(tmp_path):
    worktree = tmp_path / "worktree"
    worktree.mkdir()
    return VerifiedPackCommand(argv=ORDINARY_ARGV, env={}, working_directory=worktree)


@pytest.fixture
def denied_child(monkeypatch):
    def explode(*args, **kwargs):
        pytest.fail("generic child startup was attempted for a composition command")

    monkeypatch.setattr(subprocess, "Popen", explode)
    monkeypatch.setattr(subprocess, "run", explode)


@pytest.fixture
def admitted_environment(monkeypatch):
    monkeypatch.setenv(COMPOSITION_SUPERVISOR_B64_ENV, encode_composition_supervisor(SUPERVISOR))
    monkeypatch.setenv(CREDENTIAL_ENV, SENTINEL)
    monkeypatch.delenv(RUNTIME_RELEASE_ADMISSION_ENV, raising=False)


def admitted(command: VerifiedPackCommand, marker: str = COMPOSITION_ADMISSION) -> VerifiedPackCommand:
    return replace(command, env={RUNTIME_RELEASE_ADMISSION_ENV: marker, CREDENTIAL_ENV: SENTINEL})


def supervisor_value(payload: dict, *, canonical: bool = True) -> str:
    if canonical:
        return encode_composition_supervisor(payload)
    return base64.b64encode(json.dumps(payload).encode("ascii")).decode("ascii")


def leaked(run_volume) -> bool:
    directory = run_volume["run_output_dir"]
    files = [*directory.rglob("*"), run_volume["xcom_return_path"]]
    return any(SENTINEL in path.read_text(encoding="utf-8", errors="replace") for path in files if path.is_file())


def test_legacy_release_keeps_the_existing_generic_child(command, run_volume, monkeypatch):
    calls: list[dict] = []

    def record(argv, **kwargs):
        calls.append({"argv": argv, **kwargs})
        return RecordedRun(0)

    monkeypatch.setattr(subprocess, "run", record)
    dispatcher = RecordingDispatcher()

    assert execute_verified_pack_command(command, composition_dispatcher=dispatcher, **run_volume) == 0
    assert [call["argv"] for call in calls] == [list(ORDINARY_ARGV)]
    assert calls[0]["cwd"] == command.working_directory
    assert RUNTIME_RELEASE_ADMISSION_ENV not in calls[0]["env"]
    assert dispatcher.requests == []


@pytest.mark.parametrize("marker", ["", None])
def test_absent_or_empty_marker_stays_on_the_legacy_path(command, run_volume, monkeypatch, marker):
    monkeypatch.setattr(subprocess, "run", lambda argv, **kwargs: RecordedRun(0))
    monkeypatch.delenv(RUNTIME_RELEASE_ADMISSION_ENV, raising=False)
    env = {} if marker is None else {RUNTIME_RELEASE_ADMISSION_ENV: marker}
    dispatcher = RecordingDispatcher()

    assert (
        execute_verified_pack_command(replace(command, env=env), composition_dispatcher=dispatcher, **run_volume) == 0
    )
    assert dispatcher.requests == []


@pytest.mark.parametrize(
    ("argv", "kind", "verified_input", "selector"),
    [
        (ORDINARY_ARGV, "ordinary_transfer", "manifest.json", None),
        (SELECTOR_ARGV, "ordinary_transfer", "manifest.json", "orders"),
        (NATIVE_DBT_ARGV, "native_dbt", "dbt/execution-pack.json", None),
    ],
)
def test_authenticated_marker_dispatches_typed_requests(
    command, run_volume, admitted_environment, denied_child, argv, kind, verified_input, selector
):
    dispatcher = RecordingDispatcher(status=0)

    status = execute_verified_pack_command(
        admitted(replace(command, argv=argv)),
        composition_dispatcher=dispatcher,
        **run_volume,
    )

    assert status == 0
    request = dispatcher.requests[0]
    assert (request.kind, request.verified_input, request.process_selector) == (kind, verified_input, selector)
    assert request.argv == argv
    assert request.working_directory == command.working_directory
    assert request.supervisor.persistent_volume_claim == SUPERVISOR["persistent_volume_claim"]
    assert request.supervisor.child_uid_stop == 1_001_000_000
    assert request.env[CREDENTIAL_ENV] == SENTINEL
    assert SENTINEL not in repr(request)


@pytest.mark.parametrize(("policy", "expected"), [("child", 4), ("xcom_gate", 0)])
def test_dispatch_status_follows_the_existing_exit_policy(
    command, run_volume, admitted_environment, denied_child, policy, expected
):
    dispatched = replace(admitted(command), exit_code_policy=policy)

    status = execute_verified_pack_command(
        dispatched,
        composition_dispatcher=RecordingDispatcher(status=4),
        **run_volume,
    )

    assert status == expected
    assert json.loads(run_volume["xcom_return_path"].read_text())["status"] == "failed"


def test_v3_never_starts_generic_or_native_child(command, run_volume, admitted_environment, denied_child):
    dispatcher = RecordingDispatcher(error=RuntimeError(SENTINEL))

    assert execute_verified_pack_command(admitted(command), composition_dispatcher=dispatcher, **run_volume) == 5
    assert len(dispatcher.requests) == 1
    assert not leaked(run_volume)


def test_missing_dispatcher_rejects_instead_of_falling_back(
    command, run_volume, admitted_environment, denied_child, capsys
):
    assert execute_verified_pack_command(admitted(command), **run_volume) == 5
    assert COMPOSITION_DISPATCH_REJECTED in capsys.readouterr().err


@pytest.mark.parametrize(
    "marker",
    [
        "dpone.release-composition-admission.v2",
        COMPOSITION_ADMISSION.upper(),
        COMPOSITION_ADMISSION + " ",
        " " + COMPOSITION_ADMISSION,
        "dpone.release-set.v3",
    ],
)
def test_unknown_marker_rejects_before_dispatch(command, run_volume, admitted_environment, denied_child, marker):
    dispatcher = RecordingDispatcher()

    assert (
        execute_verified_pack_command(admitted(command, marker), composition_dispatcher=dispatcher, **run_volume) == 5
    )
    assert dispatcher.requests == []


def test_ambient_unknown_marker_cannot_reach_a_legacy_child(command, run_volume, monkeypatch, denied_child):
    monkeypatch.setenv(RUNTIME_RELEASE_ADMISSION_ENV, "forged")
    dispatcher = RecordingDispatcher()

    assert execute_verified_pack_command(command, composition_dispatcher=dispatcher, **run_volume) == 5
    assert dispatcher.requests == []


def test_ambient_marker_cannot_replace_verified_admission(
    command, run_volume, admitted_environment, denied_child, monkeypatch
):
    monkeypatch.setenv(RUNTIME_RELEASE_ADMISSION_ENV, "forged")
    dispatcher = RecordingDispatcher(status=0)

    assert execute_verified_pack_command(admitted(command), composition_dispatcher=dispatcher, **run_volume) == 0
    assert len(dispatcher.requests) == 1


def test_missing_supervisor_authority_rejects_before_dispatch(
    command, run_volume, admitted_environment, denied_child, monkeypatch
):
    monkeypatch.delenv(COMPOSITION_SUPERVISOR_B64_ENV)
    dispatcher = RecordingDispatcher()

    assert execute_verified_pack_command(admitted(command), composition_dispatcher=dispatcher, **run_volume) == 5
    assert dispatcher.requests == []
    assert not leaked(run_volume)


@pytest.mark.parametrize(
    "value",
    [
        "",
        "   ",
        "not base64 at all",
        base64.b64encode(b"{").decode("ascii"),
        base64.b64encode(b'["dpone"]').decode("ascii"),
        base64.b64encode(json.dumps(SUPERVISOR).encode("ascii")).decode("ascii"),
        base64.b64encode(json.dumps({**SUPERVISOR, "extra": 1}, separators=(",", ":")).encode("ascii")).decode("ascii"),
        encode_composition_supervisor({key: value for key, value in SUPERVISOR.items() if key != "child_uid_start"}),
        encode_composition_supervisor({**SUPERVISOR, "child_identity_count": 999_999}),
        encode_composition_supervisor({**SUPERVISOR, "persistent_volume_claim": "Invalid_Claim"}),
        encode_composition_supervisor({**SUPERVISOR, "schema": "dpone.composition-supervisor.v2"}),
        encode_composition_supervisor(SUPERVISOR).rstrip("="),
        encode_composition_supervisor(SUPERVISOR)[:8] + "\n" + encode_composition_supervisor(SUPERVISOR)[8:],
        base64.b64encode(json.dumps({**SUPERVISOR, "pad": "x" * 8192}, separators=(",", ":")).encode()).decode("ascii"),
    ],
)
def test_malformed_supervisor_authority_rejects_before_dispatch(
    command, run_volume, admitted_environment, denied_child, monkeypatch, value
):
    monkeypatch.setenv(COMPOSITION_SUPERVISOR_B64_ENV, value)
    dispatcher = RecordingDispatcher()

    assert execute_verified_pack_command(admitted(command), composition_dispatcher=dispatcher, **run_volume) == 5
    assert dispatcher.requests == []


def test_supervisor_authority_is_checked_without_a_wired_worker(
    command, run_volume, admitted_environment, denied_child, monkeypatch
):
    monkeypatch.delenv(COMPOSITION_SUPERVISOR_B64_ENV)

    assert execute_verified_pack_command(admitted(command), **run_volume) == 5

    evidence = json.loads((run_volume["run_output_dir"] / "runtime-evidence.json").read_text())
    assert evidence["reason"] == "composition_supervisor_authority_missing"


@pytest.mark.parametrize(
    "argv",
    [
        ("dpone", "run", "manifest.json"),
        ("dpone", "run", "manifest.json", "--format", "yaml"),
        ("dpone", "run", "manifest.json", "--format", "json", "--selector"),
        ("dpone", "run", "manifest.json", "--format", "json", "--extra", "orders"),
        ("dpone", "run", "manifest.json", "--selector", "orders", "--format", "json"),
        ("dpone", "run", "manifest.json", "--format", "json", "--selector", ""),
        ("dpone", "dbt", "execute-pack", "dbt/execution-pack.json"),
        ("dpone", "dbt", "execute-pack", "dbt/execution-pack.json", "--format", "json", "--selector", "orders"),
        ("dpone", "hooks", "execute", "hook.json", "--phase", "pre_hook", "--hook-id", "load"),
        ("bash", "-lc", "dpone run manifest.json --format json"),
        ("dpone", "run", "", "--format", "json"),
    ],
)
def test_unsupported_command_shapes_reject_before_dispatch(
    command, run_volume, admitted_environment, denied_child, argv
):
    dispatcher = RecordingDispatcher()

    assert (
        execute_verified_pack_command(
            admitted(replace(command, argv=argv)),
            composition_dispatcher=dispatcher,
            **run_volume,
        )
        == 5
    )
    assert dispatcher.requests == []


def test_rejection_publishes_the_stable_code_without_environment_values(
    command, run_volume, admitted_environment, denied_child, capsys
):
    assert execute_verified_pack_command(admitted(command), **run_volume) == 5

    evidence = json.loads((run_volume["run_output_dir"] / "runtime-evidence.json").read_text())
    assert evidence["error_code"] == COMPOSITION_DISPATCH_REJECTED
    assert evidence["reason"] == "composition_dispatcher_unavailable"
    logged = capsys.readouterr().err
    assert f"error_code={COMPOSITION_DISPATCH_REJECTED}" in logged
    assert SENTINEL not in logged
    assert not leaked(run_volume)
    assert json.loads(run_volume["xcom_return_path"].read_text())["status"] == "failed"
