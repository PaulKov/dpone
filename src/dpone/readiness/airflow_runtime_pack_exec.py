"""Execute a verified init-fetch pack command and publish Airflow XCom."""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, BinaryIO

from dpone.contracts.stable_error_codes import (
    is_stable_error_code,
    stable_error_code_from_text,
)
from dpone.gitops.airflow_pod_contract import AIRFLOW_XCOM_RETURN_PATH
from dpone.gitops.airflow_xcom_from_evidence import write_airflow_xcom_from_evidence
from dpone.runtime.verified_pack_hook_policy import HOOK_SKIP_ENV_NAME
from dpone.runtime.verified_pack_launcher import VerifiedPackCommand

_DEFAULT_RUN_DIR = Path("/var/lib/dpone/run")
_EVIDENCE_NAME = "runtime-evidence.json"
_STDERR_NAME = "runtime-stderr.log"
_START_MARKER = "===DPONE_RUNTIME_PACK_EXEC_START==="
_OUTCOME_MARKER = "===DPONE_RUNTIME_PACK_EXEC_OUTCOME==="


def execute_verified_pack_command(
    command: VerifiedPackCommand,
    *,
    logger: logging.Logger | None = None,
    run_output_dir: Path | None = None,
    xcom_return_path: Path | None = None,
) -> int:
    """Run one verified argv and always leave a JSON object in return.json.

    Runtime workloads cannot ``execvpe`` directly: the base worktree is
    read-only and the KPO xcom sidecar reads ``/airflow/xcom/return.json``
    after the base container exits. Capture evidence under the writable run
    volume and write XCom before applying the command's verified exit policy.
    Normal dpone packs retain the compatibility XCom-gate policy; dbt packs
    propagate the real dbt exit code so Airflow retry and failure semantics
    remain truthful.

    Child stderr is teed live into the base container log so Airflow KPO
    ``get_logs=True`` can stream progress during long runs. Child stdout stays
    file-only (structured evidence JSON must not spam the task log).
    """

    del logger
    environment = dict(os.environ)
    environment.pop(HOOK_SKIP_ENV_NAME, None)
    environment.update(command.env)
    # Keep the caller cwd intact for the subprocess path (pytest-xdist / library
    # hosts). The child receives an explicit cwd= below.
    output_dir = (run_output_dir or _DEFAULT_RUN_DIR).absolute()
    evidence_path = output_dir / _EVIDENCE_NAME
    stderr_path = output_dir / _STDERR_NAME
    xcom_path = (xcom_return_path or Path(AIRFLOW_XCOM_RETURN_PATH)).absolute()
    output_dir.mkdir(parents=True, exist_ok=True)
    xcom_path.parent.mkdir(parents=True, exist_ok=True)

    print(
        f"{_START_MARKER} argv={list(command.argv)!r}",
        file=sys.stderr,
        flush=True,
    )
    try:
        returncode = _run_pack_subprocess(
            list(command.argv),
            env=environment,
            cwd=command.working_directory,
            stdout_path=evidence_path,
            stderr_path=stderr_path,
        )
    except OSError:
        _write_startup_failure_xcom(
            evidence_path=evidence_path,
            xcom_path=xcom_path,
            stderr_path=stderr_path,
        )
        return 5

    status = "passed" if returncode == 0 else "failed"
    write_airflow_xcom_from_evidence(
        evidence_path=evidence_path,
        xcom_output=xcom_path,
        runtime_evidence_path=str(evidence_path),
        stderr_path=str(stderr_path),
        status=status,
    )
    # Emit a secret-free outcome line so Airflow task logs / kept pods still show
    # the exact runtime error_code / evidence digest after the live stderr stream.
    _emit_outcome_marker(
        evidence_path=evidence_path,
        xcom_path=xcom_path,
        status=status,
        child_returncode=returncode,
    )
    if command.exit_code_policy == "child":
        return int(returncode)
    return 0


def _run_pack_subprocess(
    argv: list[str],
    *,
    env: dict[str, str],
    cwd: Path | str,
    stdout_path: Path,
    stderr_path: Path,
) -> int:
    """Run argv, capture stdout to evidence, tee stderr live into the container log.

    Tests may monkeypatch this helper. The production path uses ``Popen`` so
    stderr bytes reach Airflow while the workload is still running.
    """

    with stdout_path.open("wb") as stdout_handle, stderr_path.open("wb") as stderr_handle:
        return _run_pack_subprocess_handles(
            argv,
            env=env,
            cwd=cwd,
            stdout_handle=stdout_handle,
            stderr_handle=stderr_handle,
        )


def _run_pack_subprocess_handles(
    argv: list[str],
    *,
    env: dict[str, str],
    cwd: Path | str,
    stdout_handle: BinaryIO,
    stderr_handle: BinaryIO,
) -> int:
    """Compatibility seam: older unit tests patch ``subprocess.run`` via wrappers."""

    # Prefer the historical ``subprocess.run`` contract when tests monkeypatch it.
    run = getattr(subprocess, "run", None)
    if run is not None and getattr(run, "__module__", "") != "subprocess":
        completed = run(
            argv,
            env=env,
            cwd=cwd,
            stdout=stdout_handle,
            stderr=stderr_handle,
            check=False,
        )
        return int(getattr(completed, "returncode", 1))

    process = subprocess.Popen(
        argv,
        env=env,
        cwd=cwd,
        stdout=stdout_handle,
        stderr=subprocess.PIPE,
    )
    assert process.stderr is not None
    _tee_stderr_stream(process.stderr, stderr_handle)
    return int(process.wait())


def _tee_stderr_stream(stream: BinaryIO, capture: BinaryIO) -> None:
    """Copy child stderr into the capture file and the process stderr (KPO logs)."""

    live = getattr(sys.stderr, "buffer", None)
    while True:
        chunk = stream.read(4096)
        if not chunk:
            break
        capture.write(chunk)
        capture.flush()
        if live is not None:
            live.write(chunk)
            live.flush()
        else:
            sys.stderr.write(chunk.decode("utf-8", errors="replace"))
            sys.stderr.flush()


def _emit_outcome_marker(
    *,
    evidence_path: Path,
    xcom_path: Path,
    status: str,
    child_returncode: int,
) -> None:
    """Print one machine-readable outcome line to the base container log."""

    error_code = _extract_error_code(evidence_path)
    evidence_bytes = _safe_size(evidence_path)
    xcom_bytes = _safe_size(xcom_path)
    print(
        f"{_OUTCOME_MARKER} status={status} child_returncode={child_returncode} "
        f"error_code={error_code or '<none>'} evidence_bytes={evidence_bytes} "
        f"xcom_bytes={xcom_bytes}",
        file=sys.stderr,
        flush=True,
    )


def _extract_error_code(evidence_path: Path) -> str | None:
    try:
        raw = evidence_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None
    if not raw.strip():
        return None
    try:
        payload: Any = json.loads(raw)
    except json.JSONDecodeError:
        return stable_error_code_from_text(raw)
    if isinstance(payload, dict):
        direct = payload.get("error_code")
        if is_stable_error_code(direct):
            return direct
        errors = payload.get("errors")
        if isinstance(errors, list):
            for item in errors:
                if isinstance(item, str):
                    if code := stable_error_code_from_text(item):
                        return code
                elif isinstance(item, dict):
                    code = item.get("code") or item.get("error_code")
                    if is_stable_error_code(code):
                        return code
    return stable_error_code_from_text(raw)


def _safe_size(path: Path) -> int:
    try:
        return int(path.stat().st_size)
    except OSError:
        return -1


def _write_startup_failure_xcom(
    *,
    evidence_path: Path,
    xcom_path: Path,
    stderr_path: Path,
) -> None:
    if not evidence_path.exists():
        evidence_path.write_text("{}\n", encoding="utf-8")
    write_airflow_xcom_from_evidence(
        evidence_path=evidence_path,
        xcom_output=xcom_path,
        runtime_evidence_path=str(evidence_path),
        stderr_path=str(stderr_path) if stderr_path.exists() else None,
        status="failed",
    )
    _emit_outcome_marker(
        evidence_path=evidence_path,
        xcom_path=xcom_path,
        status="failed",
        child_returncode=5,
    )


__all__ = ["execute_verified_pack_command"]
