"""Execute a verified init-fetch command with explicit summary publication."""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
from contextlib import ExitStack, suppress
from pathlib import Path
from typing import IO, Any, BinaryIO

from dpone.contracts.stable_error_codes import (
    is_stable_error_code,
    stable_error_code_from_text,
)
from dpone.gitops.airflow_pod_contract import AIRFLOW_XCOM_RETURN_PATH
from dpone.runtime.composition_verified_dispatch import (
    CompositionDispatchRejection,
    CompositionRunVolume,
    CompositionVerifiedDispatcher,
    composition_dispatch_required,
    report_composition_rejection,
    run_composition_dispatch,
)
from dpone.runtime.verified_pack_diagnostics import (
    VerifiedPackServiceError,
    report_pack_os_error,
    service_file_stage,
)
from dpone.runtime.verified_pack_hook_policy import HOOK_SKIP_ENV_NAME
from dpone.runtime.verified_pack_launcher import VerifiedPackCommand
from dpone.runtime.verified_pack_service_files import VerifiedPackServiceFiles

_DEFAULT_RUN_DIR = Path("/var/lib/dpone/run")
_START_MARKER = "===DPONE_RUNTIME_PACK_EXEC_START==="
_OUTCOME_MARKER = "===DPONE_RUNTIME_PACK_EXEC_OUTCOME==="


def execute_verified_pack_command(
    command: VerifiedPackCommand,
    *,
    logger: logging.Logger | None = None,
    run_output_dir: Path | None = None,
    xcom_return_path: Path | None = None,
    composition_dispatcher: CompositionVerifiedDispatcher | None = None,
) -> int:
    """Run one verified argv and retain its summary on the writable run volume.

    Runtime workloads cannot ``execvpe`` directly: the base worktree is
    read-only and the KPO xcom sidecar reads ``/airflow/xcom/return.json``
    after the base container exits. Capture evidence under the writable run
    volume and, when enabled, publish XCom before applying the exit policy.
    Separate hooks disable publication and never access the XCom path.
    Normal dpone packs retain the compatibility XCom-gate policy; dbt packs
    propagate the real dbt exit code so Airflow retry and failure semantics
    remain truthful.

    Child stderr is teed live into the base container log so Airflow KPO
    ``get_logs=True`` can stream progress during long runs. Child stdout stays
    file-only (structured evidence JSON must not spam the task log).

    A workload whose authenticated command environment marks immutable
    composition runs only through ``composition_dispatcher``; it never reaches the
    generic child path. Admission is read from ``command.env`` alone, so the
    ambient pod environment can neither forge, erase nor substitute composition
    authority; the merged environment is handed onward only for the admitted
    child. Rejected admission returns ``5`` with a run-volume diagnostic and
    invalidated summary, so a missing dispatcher cannot degrade into native-v2 or
    shell execution. Admitted dispatch keeps the existing evidence, summary,
    XCom-gate and child exit-code policies unchanged.
    """

    environment = dict(os.environ)
    environment.pop(HOOK_SKIP_ENV_NAME, None)
    environment.update(command.env)
    # Keep the caller cwd intact for the subprocess path (pytest-xdist / library
    # hosts). The child receives an explicit cwd= below.
    files: VerifiedPackServiceFiles | None = None
    child_started = False
    returncode: int | None = None
    try:
        with service_file_stage("prepare_run_directory", "run_directory"):
            output_dir = (run_output_dir or _DEFAULT_RUN_DIR).absolute()
        xcom_path = None
        if command.publish_xcom:
            with service_file_stage("prepare_xcom_directory", "xcom_directory"):
                xcom_path = (xcom_return_path or Path(AIRFLOW_XCOM_RETURN_PATH)).absolute()
        files = VerifiedPackServiceFiles(output_dir, xcom_path=xcom_path)
        files.prepare()
        # Classify admission before any launch, from authenticated command
        # authority only: an unknown marker must never reach a generic child,
        # and a stale summary is already invalidated at this point.
        composition = composition_dispatch_required(command.env)
        print(
            f"{_START_MARKER} publish_xcom={str(command.publish_xcom).lower()}",
            file=sys.stderr,
            flush=True,
        )
        child_started = True
        with service_file_stage("child_execution", "workload_command"):
            returncode = (
                run_composition_dispatch(
                    composition_dispatcher,
                    argv=command.argv,
                    working_directory=command.working_directory,
                    authority=command.env,
                    environment=environment,
                    run_volume=CompositionRunVolume(
                        evidence_path=files.evidence_path,
                        stderr_path=files.stderr_path,
                    ),
                )
                if composition
                else _run_pack_subprocess(
                    list(command.argv),
                    env=environment,
                    cwd=command.working_directory,
                    stdout_path=files.evidence_path,
                    stderr_path=files.stderr_path,
                )
            )
        status = "passed" if returncode == 0 else "failed"
        files.write_summary(status=status)
        _emit_outcome_marker(
            evidence_path=files.evidence_path,
            xcom_path=files.xcom_path,
            status=status,
            child_returncode=returncode,
        )
    except CompositionDispatchRejection as exc:
        diagnostic = report_composition_rejection(exc, logger=logger)
        if files is not None:
            files.record_failure(diagnostic, child_started=exc.dispatch_started)
            _emit_outcome_marker(
                evidence_path=files.evidence_path,
                xcom_path=files.xcom_path,
                status="failed",
                child_returncode=None,
            )
        return 5
    except VerifiedPackServiceError as exc:
        diagnostic = report_pack_os_error(exc, logger=logger)
        if files is not None:
            files.record_failure(diagnostic, child_started=child_started)
            _emit_outcome_marker(
                evidence_path=files.evidence_path,
                xcom_path=files.xcom_path,
                status="failed",
                child_returncode=returncode,
            )
        return 5
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

    with ExitStack() as stack:
        with service_file_stage("open_stdout", "runtime_evidence"):
            stdout_handle = stack.enter_context(stdout_path.open("wb"))
        with service_file_stage("open_stderr", "runtime_stderr"):
            stderr_handle = stack.enter_context(stderr_path.open("wb"))
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
        with service_file_stage("spawn_child", "workload_command"):
            completed = run(
                argv,
                env=env,
                cwd=cwd,
                stdout=stdout_handle,
                stderr=stderr_handle,
                check=False,
            )
        return int(getattr(completed, "returncode", 1))

    with service_file_stage("spawn_child", "workload_command"):
        process = subprocess.Popen(
            argv,
            env=env,
            cwd=cwd,
            stdout=stdout_handle,
            stderr=subprocess.PIPE,
        )
    assert process.stderr is not None
    try:
        with service_file_stage("capture_stderr", "runtime_stderr"):
            _tee_stderr_stream(process.stderr, stderr_handle)
        with service_file_stage("wait_child", "workload_command"):
            return int(process.wait())
    except VerifiedPackServiceError:
        # A full output volume must not leave the child running after failure.
        with suppress(OSError):
            process.kill()
        with suppress(OSError):
            process.wait()
        raise
    finally:
        with suppress(OSError):
            process.stderr.close()


def _tee_stderr_stream(stream: IO[bytes], capture: BinaryIO) -> None:
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
    xcom_path: Path | None,
    status: str,
    child_returncode: int | None,
) -> None:
    """Print one machine-readable outcome line to the base container log."""

    error_code = _extract_error_code(evidence_path)
    evidence_bytes = _safe_size(evidence_path)
    xcom_bytes = _safe_size(xcom_path) if xcom_path is not None else 0
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


__all__ = ["execute_verified_pack_command", "report_pack_os_error"]
