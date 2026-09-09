"""One receipt-authenticated child attempt for Python import health."""

from __future__ import annotations

import math
import subprocess
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Literal

from dpone.readiness.python_import_probe_protocol import (
    ProbePayload,
    ProbePayloadError,
    bind_probe_receipt,
    expected_probe_receipt,
)
from dpone.readiness.python_import_probe_runner import (
    ContainedProcessResult,
    ProbeProcessCleanupError,
)
from dpone.readiness.python_import_probe_workspace import (
    cleanup_probe_workspace,
    create_probe_workspace,
    receipt_matches,
)

AttemptFailure = Literal["timeout", "start", "workspace", "receipt", "cleanup", "policy"]
ProcessRunner = Callable[..., subprocess.CompletedProcess[bytes]]
EnvironmentFactory = Callable[[str], Mapping[str, str]]
StartupCwdFactory = Callable[[str], str]


@dataclass(frozen=True, slots=True)
class ProbeAttempt:
    """One bounded child outcome or a stable infrastructure failure."""

    outcome: int | None
    failure: AttemptFailure | None
    communication_elapsed_seconds: float = 0.0


def execute_probe_attempt(
    command: Sequence[str],
    *,
    payload: ProbePayload,
    environment_factory: EnvironmentFactory,
    startup_cwd_factory: StartupCwdFactory,
    process_runner: ProcessRunner,
    timeout: float,
) -> ProbeAttempt:
    """Execute one child and accept only an exact receipt from its workspace."""

    if timeout <= 0:
        return ProbeAttempt(None, "timeout")
    workspace = create_probe_workspace()
    if workspace is None:
        return ProbeAttempt(None, "workspace")
    result: subprocess.CompletedProcess[bytes] | None = None
    failure: AttemptFailure | None = None
    receipt_confirmed = False
    communication_elapsed = 0.0
    try:
        try:
            attempt_input = bind_probe_receipt(payload, workspace.receipt_path)
            runner_started = time.monotonic()
            result = process_runner(
                tuple(command),
                check=False,
                cwd=startup_cwd_factory(workspace.directory),
                env=environment_factory(workspace.directory),
                input=attempt_input,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=timeout,
            )
            runner_elapsed = max(0.0, time.monotonic() - runner_started)
            communication_elapsed = _communication_elapsed(result, runner_elapsed)
        except ProbePayloadError as exc:
            failure = "policy" if exc.kind == "policy" else "workspace"
        except ProbeProcessCleanupError:
            failure = "cleanup"
        except subprocess.TimeoutExpired:
            failure = "timeout"
        except OSError:
            failure = "start"
        else:
            expected = expected_probe_receipt(payload.receipt_token, result.returncode)
            receipt_confirmed = expected is not None and receipt_matches(workspace, expected)
    finally:
        cleanup_confirmed = cleanup_probe_workspace(workspace)
    if not cleanup_confirmed:
        return ProbeAttempt(None, "cleanup")
    if failure is not None:
        return ProbeAttempt(None, failure)
    if result is None or not receipt_confirmed:
        return ProbeAttempt(None, "receipt")
    return ProbeAttempt(result.returncode, None, communication_elapsed)


def _communication_elapsed(
    result: subprocess.CompletedProcess[bytes],
    fallback: float,
) -> float:
    if type(result) is ContainedProcessResult:
        measured = result.communication_elapsed_seconds
        if type(measured) is float and math.isfinite(measured) and measured >= 0.0:
            return measured
    return fallback if math.isfinite(fallback) and fallback >= 0.0 else 0.0


__all__ = ["ProbeAttempt", "execute_probe_attempt"]
