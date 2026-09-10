"""Verified hooks use run-volume artifacts without requiring an XCom mount."""

from __future__ import annotations

import argparse
import errno
import json
import os
import sys
from pathlib import Path

import pytest

from dpone.readiness import airflow_runtime_pack_exec as pack_exec
from dpone.runtime.verified_pack_diagnostics import VerifiedPackServiceError
from dpone.runtime.verified_pack_launcher import VerifiedPackCommand
from dpone.runtime.verified_pack_service_files import VerifiedPackServiceFiles


def test_startup_failure_summary_overrides_canonical_child_success(tmp_path: Path) -> None:
    files = VerifiedPackServiceFiles(tmp_path / "run", xcom_path=tmp_path / "xcom" / "return.json")
    files.prepare()
    evidence = json.dumps({"kind": "gitops.airflow_runtime_evidence", "status": "passed", "steps": []})
    files.evidence_path.write_text(evidence)
    diagnostic = VerifiedPackServiceError(
        OSError(errno.ENOSPC, "secret"), stage="publish_xcom", service_path="xcom_return"
    ).diagnostic
    files.record_failure(diagnostic, child_started=True)
    assert files.evidence_path.read_text() == evidence
    for path in (files.summary_path, files.xcom_path):
        summary = json.loads(path.read_text())
        assert summary["status"] == "failed"
        assert summary["blockers"]


def _command(tmp_path: Path, *, publish_xcom: bool = False) -> VerifiedPackCommand:
    return VerifiedPackCommand(
        argv=("secret-argv-value",),
        env={"PRIVATE_VALUE": "secret-env-value"},
        working_directory=tmp_path,
        exit_code_policy="child",
        publish_xcom=publish_xcom,
    )


def _fake_child(monkeypatch: pytest.MonkeyPatch, *, returncode: int = 0) -> None:
    def run(argv: list[str], **kwargs: object) -> object:
        del argv
        stdout = kwargs["stdout"]
        assert hasattr(stdout, "write")
        stdout.write(b'{"status":"SUCCESS"}')
        return argparse.Namespace(returncode=returncode)

    monkeypatch.setattr(pack_exec.subprocess, "run", run)


@pytest.mark.parametrize("returncode", [0, 17])
def test_hook_never_operates_on_xcom_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    returncode: int,
) -> None:
    class ForbiddenXComPath(type(Path())):
        def absolute(self) -> Path:
            pytest.fail("disabled publication must not normalize an XCom path")

    _fake_child(monkeypatch, returncode=returncode)
    code = pack_exec.execute_verified_pack_command(
        _command(tmp_path),
        run_output_dir=tmp_path / "run",
        xcom_return_path=ForbiddenXComPath(tmp_path / "xcom" / "return.json"),
    )

    assert code == returncode
    assert not (tmp_path / "xcom").exists()
    summary = json.loads((tmp_path / "run" / "runtime-summary.json").read_text())
    assert summary["status"] == ("passed" if returncode == 0 else "failed")
    log = capsys.readouterr().err
    assert "secret-argv-value" not in log
    assert "secret-env-value" not in log


@pytest.mark.parametrize("returncode", [0, 23])
def test_real_nonroot_hook_child_uses_readonly_workload(
    tmp_path: Path,
    returncode: int,
) -> None:
    if not hasattr(os, "getuid") or os.getuid() == 0:
        pytest.skip("requires a real non-root POSIX test process")
    worktree = tmp_path / "worktree"
    worktree.mkdir()
    script = worktree / "hook.py"
    script.write_text(
        "import json, os, pathlib, sys\n"
        "assert os.getuid() != 0\n"
        "try:\n"
        "    pathlib.Path('forbidden-write').write_text('no')\n"
        "except PermissionError:\n"
        "    pass\n"
        "else:\n"
        "    raise AssertionError('workload was writable')\n"
        "print(json.dumps({'status': 'SUCCESS', 'uid': os.getuid()}))\n"
        "print('hook stderr', file=sys.stderr)\n"
        f"sys.exit({returncode})\n"
    )
    script.chmod(0o444)
    worktree.chmod(0o555)
    blocker = tmp_path / "not-a-directory"
    blocker.write_text("blocks XCom directory creation")
    command = VerifiedPackCommand(
        argv=(sys.executable, "hook.py"),
        env={},
        working_directory=worktree,
        exit_code_policy="child",
        publish_xcom=False,
    )
    try:
        code = pack_exec.execute_verified_pack_command(
            command,
            run_output_dir=tmp_path / "run",
            xcom_return_path=blocker / "xcom" / "return.json",
        )
        assert code == returncode
        evidence = json.loads((tmp_path / "run" / "runtime-evidence.json").read_text())
        assert evidence["uid"] == os.getuid()
        assert (tmp_path / "run" / "runtime-stderr.log").read_text() == "hook stderr\n"
        summary = json.loads((tmp_path / "run" / "runtime-summary.json").read_text())
        assert summary["status"] == ("passed" if returncode == 0 else "failed")
        assert sorted(path.name for path in worktree.iterdir()) == ["hook.py"]
    finally:
        worktree.chmod(0o755)
        script.chmod(0o644)


@pytest.mark.parametrize(
    ("operation", "error_number", "stage", "role"),
    [
        ("run_directory", errno.EACCES, "prepare_run_directory", "run_directory"),
        ("xcom_directory", errno.EACCES, "prepare_xcom_directory", "xcom_directory"),
        ("stdout", errno.ENOSPC, "open_stdout", "runtime_evidence"),
        ("stderr", errno.EACCES, "open_stderr", "runtime_stderr"),
        ("spawn", errno.ENOENT, "spawn_child", "workload_command"),
        ("summary", errno.ENOSPC, "write_summary", "runtime_summary"),
        ("xcom", errno.ENOSPC, "publish_xcom", "xcom_return"),
    ],
)
def test_os_failures_have_safe_stage_type_errno_and_nonzero_exit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    operation: str,
    error_number: int,
    stage: str,
    role: str,
) -> None:
    run_dir = tmp_path / "run"
    xcom = tmp_path / "xcom" / "return.json"
    original_mkdir, original_open = Path.mkdir, Path.open
    _fake_child(monkeypatch)

    def fail() -> None:
        raise OSError(error_number, "secret-exception-value", "/secret-filename-value")

    def mkdir(path: Path, *args: object, **kwargs: object) -> None:
        if (operation == "run_directory" and path == run_dir) or (
            operation == "xcom_directory" and path == xcom.parent
        ):
            fail()
        original_mkdir(path, *args, **kwargs)

    def open_file(path: Path, mode: str = "r", *args: object, **kwargs: object) -> object:
        paths = {
            "stdout": run_dir / "runtime-evidence.json",
            "stderr": run_dir / "runtime-stderr.log",
            "summary": run_dir / "runtime-summary.json",
            "xcom": xcom,
        }
        if path == paths.get(operation) and "w" in mode:
            fail()
        return original_open(path, mode, *args, **kwargs)

    monkeypatch.setattr(Path, "mkdir", mkdir)
    monkeypatch.setattr(Path, "open", open_file)
    if operation == "spawn":
        monkeypatch.setattr(pack_exec.subprocess, "run", lambda *args, **kwargs: fail())

    code = pack_exec.execute_verified_pack_command(
        _command(tmp_path, publish_xcom=True),
        run_output_dir=run_dir,
        xcom_return_path=xcom,
    )

    assert code == 5
    log = capsys.readouterr().err
    error_type = type(OSError(error_number, "ignored")).__name__
    assert "DPONE_RUNTIME_PACK_EXEC_FAILED" in log
    assert f"stage={stage}" in log
    assert f"exception_type={error_type}" in log
    assert f"errno={error_number}" in log
    assert f"service_path={role}" in log
    if operation in {"summary", "xcom"}:
        assert "child_returncode=0" in log
    if operation != "run_directory":
        diagnostic = (run_dir / "runtime-startup-error.json").read_text()
        payload = json.loads(diagnostic)
        assert payload["stage"] == stage
        assert payload["errno"] == error_number
        assert payload["service_path"] == role
        log += diagnostic
    for secret in ("secret-argv-value", "secret-env-value", "secret-exception-value", "secret-filename-value"):
        assert secret not in log


def test_failed_retry_replaces_previous_success_and_does_not_reuse_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _fake_child(monkeypatch)
    run_dir = tmp_path / "run"
    xcom = tmp_path / "xcom" / "return.json"
    command = _command(tmp_path, publish_xcom=True)
    assert pack_exec.execute_verified_pack_command(command, run_output_dir=run_dir, xcom_return_path=xcom) == 0
    prior_digest = json.loads(xcom.read_text())["runtime_evidence_sha256"]

    def fail(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise FileNotFoundError(errno.ENOENT, "secret-child-command")

    monkeypatch.setattr(pack_exec.subprocess, "run", fail)
    assert pack_exec.execute_verified_pack_command(command, run_output_dir=run_dir, xcom_return_path=xcom) == 5
    summary = json.loads(xcom.read_text())
    assert summary["status"] == "failed"
    assert summary["runtime_evidence_sha256"] != prior_digest
    assert json.loads((run_dir / "runtime-summary.json").read_text())["status"] == "failed"


def test_os_failure_remains_nonzero_when_all_artifact_writes_fail(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def deny(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise PermissionError(errno.EACCES, "secret-unwritable-output")

    monkeypatch.setattr(Path, "mkdir", deny)
    monkeypatch.setattr(Path, "open", deny)
    code = pack_exec.execute_verified_pack_command(_command(tmp_path), run_output_dir=tmp_path / "run")

    assert code == 5
    log = capsys.readouterr().err
    assert "stage=prepare_run_directory" in log
    assert "secret-unwritable-output" not in log


def test_capture_failure_reaps_real_child_before_returning(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import subprocess

    children: list[subprocess.Popen[bytes]] = []
    original_popen = subprocess.Popen

    def start(*args: object, **kwargs: object) -> subprocess.Popen[bytes]:
        child = original_popen(*args, **kwargs)
        children.append(child)
        return child

    def fail_capture(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise OSError(errno.ENOSPC, "secret-full-volume")

    monkeypatch.setattr(pack_exec.subprocess, "Popen", start)
    monkeypatch.setattr(pack_exec, "_tee_stderr_stream", fail_capture)
    command = VerifiedPackCommand(
        argv=(sys.executable, "-c", "import time; time.sleep(60)"),
        env={},
        working_directory=tmp_path,
        exit_code_policy="child",
        publish_xcom=False,
    )
    try:
        code = pack_exec.execute_verified_pack_command(command, run_output_dir=tmp_path / "run")
        assert code == 5
        assert len(children) == 1
        assert children[0].poll() is not None
    finally:
        for child in children:
            if child.poll() is None:
                child.kill()
            child.wait()
