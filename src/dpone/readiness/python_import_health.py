"""Bounded, redacted health probes for optional Python runtime modules."""

from __future__ import annotations

import os
import re
import subprocess
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass

from dpone.readiness.python_import_environment import validate_child_environment
from dpone.readiness.python_import_interpreter_options import (
    _interpreter_probe_options,
    _startup_warning_environment,
    _startup_xoptions_replayable,
)
from dpone.readiness.python_import_probe_child import (
    CONFINED_STARTUP_ENVIRONMENT,
    EFFECTIVE_STARTUP_ENVIRONMENT,
    IMPORT_COMMAND,
    IMPORT_FAILED_EXIT,
    IMPORT_NOT_INSTALLED_EXIT,
    IMPORT_PATH_INVALID_EXIT,
    IMPORT_POLICY_INVALID_EXIT,
    IMPORT_STARTUP_UNSUPPORTED_EXIT,
    MAX_WARN_OPTION_COUNT,
    RECEIPT_TOKEN_BYTES,
)
from dpone.readiness.python_import_probe_execution import ProbeAttempt, execute_probe_attempt
from dpone.readiness.python_import_probe_protocol import (
    ProbePayloadError,
    build_probe_payload,
    capture_probe_environment,
    normalize_xoptions,
)
from dpone.readiness.python_import_probe_runner import run_contained_import_process
from dpone.readiness.python_import_probe_snapshots import capture_probe_path, capture_warning_options
from dpone.readiness.python_import_startup_surface import startup_import_surface_replayable

_MODULE_NAME = re.compile(r"^[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*$")
_MAX_MODULE_NAME_CHARACTERS = 1_024
_PROBE_TIMEOUT_SECONDS = 5.0

ProcessRunner = Callable[..., subprocess.CompletedProcess[bytes]]


@dataclass(frozen=True, slots=True)
class PythonImportHealth:
    passed: bool
    reason_code: str | None
    summary: str


def _confined_child_environment(
    child_environment: Mapping[str, str],
    workspace_directory: str,
) -> dict[str, str]:
    """Return hermetic startup controls while preserving target restoration."""

    child = dict(child_environment)
    for name in CONFINED_STARTUP_ENVIRONMENT:
        child[name] = workspace_directory
    validated = validate_child_environment(child)
    if validated is None:
        raise ProbePayloadError("policy")
    return validated


def _effective_child_environment(
    environment: Mapping[str, str],
    target_startup: Mapping[str, str],
    startup_warnings: str | None,
    workspace_directory: str,
) -> dict[str, str]:
    """Replay bounded startup inputs whose hooks form the effective proof."""

    child = _confined_child_environment(environment, workspace_directory)
    child.update({key: value for key, value in target_startup.items() if key in EFFECTIVE_STARTUP_ENVIRONMENT})
    if startup_warnings is not None:
        child["PYTHONWARNINGS"] = startup_warnings
    validated = validate_child_environment(child)
    if validated is None:
        raise ProbePayloadError("policy")
    return validated


def probe_python_import(
    module_name: str,
    *,
    process_runner: ProcessRunner = run_contained_import_process,
    command: Sequence[str] | None = None,
) -> PythonImportHealth:
    """Prove a real import with bounded policy, containment and positive receipt."""

    if type(module_name) is not str or len(module_name) > _MAX_MODULE_NAME_CHARACTERS:
        return PythonImportHealth(False, "python_import_name_invalid", "module name is invalid")
    name = module_name.strip()
    if _MODULE_NAME.fullmatch(name) is None:
        return PythonImportHealth(False, "python_import_name_invalid", "module name is invalid")
    if not startup_import_surface_replayable():
        return _startup_unsupported_health()

    environment = capture_probe_environment()
    xoptions = normalize_xoptions()
    warnoptions = capture_warning_options(MAX_WARN_OPTION_COUNT)
    if warnoptions is False:
        return _startup_unsupported_health()
    if warnoptions is None:
        return _policy_invalid_health()
    pycache_prefix = sys.pycache_prefix
    if environment is None or xoptions is None:
        return _policy_invalid_health()
    effective_options = _interpreter_probe_options(
        warnoptions=warnoptions,
        xoptions=xoptions,
        startup_state=environment.runtime_state,
    )
    warnings_replayable, startup_warnings = _startup_warning_environment(warnoptions)
    if effective_options is None:
        return _policy_invalid_health()
    if (
        not environment.startup_state_replayable
        or not _startup_xoptions_replayable(xoptions, environment.runtime_state)
        or not warnings_replayable
    ):
        return _startup_unsupported_health()
    try:
        hermetic_token = os.urandom(RECEIPT_TOKEN_BYTES)
        effective_token = os.urandom(RECEIPT_TOKEN_BYTES)
    except OSError:
        return _probe_unavailable_health("module import probe could not initialize")
    try:
        path = capture_probe_path()
        hermetic_payload = build_probe_payload(
            hermetic_token,
            environment=environment,
            warnoptions=warnoptions,
            xoptions=xoptions,
            startup_warnings_replayed=False,
            path=path,
            pycache_prefix=pycache_prefix,
            hermetic=True,
        )
        effective_payload = build_probe_payload(
            effective_token,
            environment=environment,
            warnoptions=warnoptions,
            xoptions=xoptions,
            startup_warnings_replayed=startup_warnings is not None,
            path=path,
            pycache_prefix=pycache_prefix,
            hermetic=False,
        )
    except ProbePayloadError as exc:
        return _path_invalid_health() if exc.kind == "path" else _policy_invalid_health()

    hermetic_command = tuple(
        command
        or (
            sys.executable,
            "-I",
            "-S",
            *effective_options,
            "-c",
            IMPORT_COMMAND,
            name,
        )
    )
    effective_command = tuple(command or (sys.executable, *effective_options, "-c", IMPORT_COMMAND, name))
    communication_budget = _PROBE_TIMEOUT_SECONDS
    hermetic = execute_probe_attempt(
        hermetic_command,
        payload=hermetic_payload,
        environment_factory=lambda workspace: _confined_child_environment(environment.child, workspace),
        startup_cwd_factory=lambda workspace: workspace,
        process_runner=process_runner,
        timeout=communication_budget,
    )
    if hermetic.failure is not None:
        return _attempt_failure_health(hermetic)
    communication_budget = max(
        0.0,
        communication_budget - hermetic.communication_elapsed_seconds,
    )
    effective = execute_probe_attempt(
        effective_command,
        payload=effective_payload,
        environment_factory=lambda workspace: _effective_child_environment(
            environment.child,
            environment.target_startup,
            startup_warnings,
            workspace,
        ),
        startup_cwd_factory=lambda workspace: workspace,
        process_runner=process_runner,
        timeout=communication_budget,
    )
    if effective.failure is not None:
        return _attempt_failure_health(effective)
    if hermetic.outcome != effective.outcome:
        return _startup_unsupported_health()
    if hermetic.outcome == IMPORT_PATH_INVALID_EXIT:
        return _path_invalid_health()
    if hermetic.outcome == IMPORT_POLICY_INVALID_EXIT:
        return _policy_invalid_health()
    if hermetic.outcome == IMPORT_STARTUP_UNSUPPORTED_EXIT:
        return _startup_unsupported_health()
    if hermetic.outcome == IMPORT_NOT_INSTALLED_EXIT:
        return PythonImportHealth(False, "python_import_not_installed", "module is not installed")
    if hermetic.outcome == IMPORT_FAILED_EXIT:
        return PythonImportHealth(
            False,
            "python_import_failed",
            "module is installed but cannot be loaded",
        )
    if hermetic.outcome == 0:
        return PythonImportHealth(True, None, "module import succeeded")
    return _probe_unavailable_health("module import probe returned an unsupported outcome")


def _attempt_failure_health(attempt: ProbeAttempt) -> PythonImportHealth:
    if attempt.failure == "policy":
        return _policy_invalid_health()
    if attempt.failure == "timeout":
        return PythonImportHealth(
            False,
            "python_import_timed_out",
            "module import exceeded the health-check deadline",
        )
    summaries = {
        "start": "module import probe could not start",
        "workspace": "module import probe workspace could not initialize",
        "receipt": "module import probe did not confirm completion",
        "cleanup": "module import probe cleanup could not be confirmed",
    }
    if attempt.failure is None:
        return _probe_unavailable_health("module import probe failed closed")
    return _probe_unavailable_health(summaries[attempt.failure])


def _path_invalid_health() -> PythonImportHealth:
    return PythonImportHealth(
        False,
        "python_import_path_invalid",
        "effective Python import path is invalid or exceeds the health-check limit",
    )


def _policy_invalid_health() -> PythonImportHealth:
    return PythonImportHealth(
        False,
        "python_import_policy_invalid",
        "effective Python startup policy is invalid or exceeds the health-check limit",
    )


def _startup_unsupported_health() -> PythonImportHealth:
    return PythonImportHealth(
        False,
        "python_import_startup_unsupported",
        "effective Python startup environment cannot be reproduced safely",
    )


def _probe_unavailable_health(summary: str) -> PythonImportHealth:
    return PythonImportHealth(False, "python_import_probe_unavailable", summary)


__all__ = ["PythonImportHealth", "probe_python_import"]
