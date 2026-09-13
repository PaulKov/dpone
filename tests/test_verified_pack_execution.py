"""Public verified-runtime execution admission for authenticated v3 composition.

Composition authority comes only from the authenticated ``VerifiedPackCommand``
environment. Every composition test makes generic child startup explode, so a
silent fallback to native-v2 or shell execution cannot produce a passing result.
"""

from __future__ import annotations

import base64
import json
import os
import subprocess
import time
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
OTHER_SUPERVISOR = {**SUPERVISOR, "child_uid_start": 1_500_000_000, "child_gid_start": 1_500_000_000}
PINNED_TRANSPORT = encode_composition_supervisor(SUPERVISOR)
ORDINARY_ARGV = ("dpone", "run", "manifest.json", "--format", "json")
SELECTOR_ARGV = (*ORDINARY_ARGV, "--selector", "orders")
NATIVE_DBT_ARGV = ("dpone", "dbt", "execute-pack", "dbt/execution-pack.json", "--format", "json")
EVIDENCE = b'{"kind":"gitops.airflow_runtime_evidence","status":"passed"}'


class RecordingDispatcher:
    """Injected worker seam recording every admitted composition request.

    ``evidence`` mirrors a real worker root writing its own attempt evidence to
    the run volume supplied by the request.
    """

    def __init__(
        self,
        *,
        status: int = 0,
        error: BaseException | None = None,
        evidence: bytes | None = EVIDENCE,
        evidence_mtime: float | None = None,
    ) -> None:
        self.requests: list = []
        self._status = status
        self._error = error
        self._evidence = evidence
        self._evidence_mtime = evidence_mtime

    def run(self, request):
        self.requests.append(request)
        if self._evidence is not None:
            request.run_volume.evidence_path.write_bytes(self._evidence)
            if self._evidence_mtime is not None:
                os.utime(request.run_volume.evidence_path, (self._evidence_mtime, self._evidence_mtime))
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
def clean_ambient(monkeypatch):
    """Prove authority is not ambient: the pod environment carries no marker."""

    monkeypatch.setenv(CREDENTIAL_ENV, SENTINEL)
    monkeypatch.delenv(RUNTIME_RELEASE_ADMISSION_ENV, raising=False)
    monkeypatch.delenv(COMPOSITION_SUPERVISOR_B64_ENV, raising=False)


def admitted(
    command: VerifiedPackCommand,
    *,
    marker: str = COMPOSITION_ADMISSION,
    supervisor: str | None = PINNED_TRANSPORT,
) -> VerifiedPackCommand:
    env = {CREDENTIAL_ENV: SENTINEL, RUNTIME_RELEASE_ADMISSION_ENV: marker}
    if supervisor is not None:
        env[COMPOSITION_SUPERVISOR_B64_ENV] = supervisor
    return replace(command, env=env)


def leaked(run_volume) -> bool:
    directory = run_volume["run_output_dir"]
    files = [*directory.rglob("*"), run_volume["xcom_return_path"]]
    return any(SENTINEL in path.read_text(encoding="utf-8", errors="replace") for path in files if path.is_file())


def evidence_payload(run_volume) -> bytes:
    return (run_volume["run_output_dir"] / "runtime-evidence.json").read_bytes()


def rejection_reason(run_volume) -> str:
    return json.loads((run_volume["run_output_dir"] / "runtime-startup-error.json").read_text())["reason"]


def test_legacy_release_keeps_the_existing_generic_child(command, run_volume, clean_ambient, monkeypatch):
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
def test_absent_or_empty_marker_stays_on_the_legacy_path(command, run_volume, clean_ambient, monkeypatch, marker):
    monkeypatch.setattr(subprocess, "run", lambda argv, **kwargs: RecordedRun(0))
    env = {} if marker is None else {RUNTIME_RELEASE_ADMISSION_ENV: marker}
    dispatcher = RecordingDispatcher()

    assert (
        execute_verified_pack_command(replace(command, env=env), composition_dispatcher=dispatcher, **run_volume) == 0
    )
    assert dispatcher.requests == []


def test_ambient_marker_and_supervisor_cannot_forge_composition(command, run_volume, monkeypatch):
    """A legacy v1/v2 command stays on its child path even with a perfect pod env."""

    monkeypatch.setenv(RUNTIME_RELEASE_ADMISSION_ENV, COMPOSITION_ADMISSION)
    monkeypatch.setenv(COMPOSITION_SUPERVISOR_B64_ENV, PINNED_TRANSPORT)
    calls: list[list[str]] = []
    monkeypatch.setattr(subprocess, "run", lambda argv, **kwargs: calls.append(argv) or RecordedRun(0))
    dispatcher = RecordingDispatcher()

    assert execute_verified_pack_command(command, composition_dispatcher=dispatcher, **run_volume) == 0
    assert calls == [list(ORDINARY_ARGV)]
    assert dispatcher.requests == []
    summary = json.loads(run_volume["xcom_return_path"].read_text())
    assert summary["status"] != "passed" or "composition" not in json.dumps(summary)


@pytest.mark.parametrize("ambient", ["", "forged", COMPOSITION_ADMISSION])
def test_ambient_marker_cannot_erase_or_replace_command_authority(
    command, run_volume, clean_ambient, denied_child, monkeypatch, ambient
):
    monkeypatch.setenv(RUNTIME_RELEASE_ADMISSION_ENV, ambient)
    monkeypatch.setenv(COMPOSITION_SUPERVISOR_B64_ENV, encode_composition_supervisor(OTHER_SUPERVISOR))
    dispatcher = RecordingDispatcher()

    assert execute_verified_pack_command(admitted(command), composition_dispatcher=dispatcher, **run_volume) == 0
    request = dispatcher.requests[0]
    assert request.supervisor.child_uid_start == SUPERVISOR["child_uid_start"]
    assert request.env[COMPOSITION_SUPERVISOR_B64_ENV] == PINNED_TRANSPORT


def test_ambient_supervisor_cannot_satisfy_missing_command_authority(
    command, run_volume, clean_ambient, denied_child, monkeypatch
):
    monkeypatch.setenv(COMPOSITION_SUPERVISOR_B64_ENV, PINNED_TRANSPORT)
    dispatcher = RecordingDispatcher()

    status = execute_verified_pack_command(
        admitted(command, supervisor=None),
        composition_dispatcher=dispatcher,
        **run_volume,
    )

    assert status == 5
    assert dispatcher.requests == []
    assert rejection_reason(run_volume) == "composition_supervisor_authority_missing"


@pytest.mark.parametrize(
    ("argv", "kind", "verified_input", "selector"),
    [
        (ORDINARY_ARGV, "ordinary_transfer", "manifest.json", None),
        (SELECTOR_ARGV, "ordinary_transfer", "manifest.json", "orders"),
        (NATIVE_DBT_ARGV, "native_dbt", "dbt/execution-pack.json", None),
    ],
)
def test_authenticated_command_dispatches_typed_requests(
    command, run_volume, clean_ambient, denied_child, argv, kind, verified_input, selector
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
    assert request.run_volume.evidence_path == run_volume["run_output_dir"] / "runtime-evidence.json"
    assert request.run_volume.stderr_path == run_volume["run_output_dir"] / "runtime-stderr.log"
    assert request.env[CREDENTIAL_ENV] == SENTINEL
    assert SENTINEL not in repr(request)
    assert json.loads(run_volume["xcom_return_path"].read_text())["status"] == "passed"


@pytest.mark.parametrize(("policy", "expected"), [("child", 4), ("xcom_gate", 0)])
def test_dispatch_status_follows_the_existing_exit_policy(
    command, run_volume, clean_ambient, denied_child, policy, expected
):
    dispatched = replace(admitted(command), exit_code_policy=policy)

    status = execute_verified_pack_command(
        dispatched,
        composition_dispatcher=RecordingDispatcher(status=4, evidence=None),
        **run_volume,
    )

    assert status == expected
    assert json.loads(run_volume["xcom_return_path"].read_text())["status"] == "failed"


def test_success_without_current_attempt_evidence_cannot_publish_a_pass(
    command, run_volume, clean_ambient, denied_child
):
    dispatcher = RecordingDispatcher(status=0, evidence=None)

    assert execute_verified_pack_command(admitted(command), composition_dispatcher=dispatcher, **run_volume) == 5
    assert rejection_reason(run_volume) == "composition_evidence_missing"
    assert json.loads(run_volume["xcom_return_path"].read_text())["status"] == "failed"


@pytest.mark.parametrize("payload", [b"", b"   ", b"[]", b"not json", b'{"a":1,"a":2}'])
def test_success_with_invalid_evidence_cannot_publish_a_pass(command, run_volume, clean_ambient, denied_child, payload):
    dispatcher = RecordingDispatcher(status=0, evidence=payload)

    assert execute_verified_pack_command(admitted(command), composition_dispatcher=dispatcher, **run_volume) == 5
    assert rejection_reason(run_volume) == "composition_evidence_invalid"


def test_success_with_stale_evidence_cannot_publish_a_pass(command, run_volume, clean_ambient, denied_child):
    dispatcher = RecordingDispatcher(status=0, evidence_mtime=time.time() - 3600)

    assert execute_verified_pack_command(admitted(command), composition_dispatcher=dispatcher, **run_volume) == 5
    assert rejection_reason(run_volume) == "composition_evidence_stale"
    assert evidence_payload(run_volume) == EVIDENCE


def test_failure_after_dispatch_preserves_worker_evidence(command, run_volume, clean_ambient, denied_child):
    dispatcher = RecordingDispatcher(error=RuntimeError(SENTINEL))

    assert execute_verified_pack_command(admitted(command), composition_dispatcher=dispatcher, **run_volume) == 5
    assert len(dispatcher.requests) == 1
    assert evidence_payload(run_volume) == EVIDENCE
    assert rejection_reason(run_volume) == "composition_dispatch_failed"
    assert not leaked(run_volume)


def test_missing_dispatcher_rejects_instead_of_falling_back(command, run_volume, clean_ambient, denied_child, capsys):
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
def test_unknown_marker_rejects_before_dispatch(command, run_volume, clean_ambient, denied_child, marker):
    dispatcher = RecordingDispatcher()

    status = execute_verified_pack_command(
        admitted(command, marker=marker),
        composition_dispatcher=dispatcher,
        **run_volume,
    )

    assert status == 5
    assert dispatcher.requests == []
    assert rejection_reason(run_volume) == "unknown_release_admission"


@pytest.mark.parametrize(
    "supervisor",
    [
        "",
        "   ",
        "not base64 at all",
        base64.b64encode(b"{").decode("ascii"),
        base64.b64encode(b'["dpone"]').decode("ascii"),
        base64.b64encode(json.dumps(SUPERVISOR).encode("ascii")).decode("ascii"),
        encode_composition_supervisor({**SUPERVISOR, "child_identity_count": 999_999}),
        encode_composition_supervisor({**SUPERVISOR, "persistent_volume_claim": "Invalid_Claim"}),
        encode_composition_supervisor({**SUPERVISOR, "schema": "dpone.composition-supervisor.v2"}),
        PINNED_TRANSPORT.rstrip("="),
        PINNED_TRANSPORT[:8] + "\n" + PINNED_TRANSPORT[8:],
        base64.b64encode(json.dumps({**SUPERVISOR, "pad": "x" * 8192}, separators=(",", ":")).encode()).decode("ascii"),
    ],
)
def test_malformed_command_supervisor_rejects_before_dispatch(
    command, run_volume, clean_ambient, denied_child, supervisor
):
    dispatcher = RecordingDispatcher()

    status = execute_verified_pack_command(
        admitted(command, supervisor=supervisor),
        composition_dispatcher=dispatcher,
        **run_volume,
    )

    assert status == 5
    assert dispatcher.requests == []
    assert rejection_reason(run_volume).startswith("composition_supervisor_authority_")


def test_supervisor_authority_is_checked_without_a_wired_worker(command, run_volume, clean_ambient, denied_child):
    assert execute_verified_pack_command(admitted(command, supervisor=None), **run_volume) == 5
    assert rejection_reason(run_volume) == "composition_supervisor_authority_missing"


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
def test_unsupported_command_shapes_reject_before_dispatch(command, run_volume, clean_ambient, denied_child, argv):
    dispatcher = RecordingDispatcher()

    status = execute_verified_pack_command(
        admitted(replace(command, argv=argv)),
        composition_dispatcher=dispatcher,
        **run_volume,
    )

    assert status == 5
    assert dispatcher.requests == []


@pytest.mark.parametrize(
    "verified_input",
    [
        "/etc/manifest.json",
        "../manifest.json",
        "runtime/../../manifest.json",
        "runtime\\manifest.json",
        "./manifest.json",
    ],
)
def test_unsafe_verified_input_rejects_before_dispatch(
    command, run_volume, clean_ambient, denied_child, verified_input
):
    dispatcher = RecordingDispatcher()

    status = execute_verified_pack_command(
        admitted(replace(command, argv=("dpone", "run", verified_input, "--format", "json"))),
        composition_dispatcher=dispatcher,
        **run_volume,
    )

    assert status == 5
    assert dispatcher.requests == []
    assert rejection_reason(run_volume) == "composition_command_path"


def test_rejection_publishes_the_stable_code_without_environment_values(
    command, run_volume, clean_ambient, denied_child, capsys
):
    assert execute_verified_pack_command(admitted(command), **run_volume) == 5

    evidence = json.loads(evidence_payload(run_volume))
    assert evidence["error_code"] == COMPOSITION_DISPATCH_REJECTED
    assert evidence["reason"] == "composition_dispatcher_unavailable"
    logged = capsys.readouterr().err
    assert f"error_code={COMPOSITION_DISPATCH_REJECTED}" in logged
    assert SENTINEL not in logged
    assert not leaked(run_volume)
    assert json.loads(run_volume["xcom_return_path"].read_text())["status"] == "failed"
