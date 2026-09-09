"""CLI result selection for fail-closed safe sample runs.

The sample-run facade already builds policy, deployment, and runtime evidence.
This module keeps the user-facing error selection deterministic so the CLI
surfaces the most actionable blocker instead of hiding it behind a generic
placeholder.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol


class SafeSampleExecutionPlanView(Protocol):
    """Structural view needed by CLI result selection."""

    @property
    def blockers(self) -> Iterable[Mapping[str, Any]]:
        """Return structured blockers produced by the execution-plan builder."""
        ...


@dataclass(frozen=True, slots=True)
class SafeSampleCliResult:
    """Final machine-readable status for the beginner safe sample facade."""

    errors: tuple[dict[str, Any], ...]
    exit_code: int

    @property
    def passed(self) -> bool:
        return not self.errors

    @property
    def status(self) -> str:
        return "success" if self.passed else "error"


def select_safe_sample_cli_result(
    *,
    policy_errors: Iterable[Mapping[str, Any]],
    target_plan_errors: Iterable[Mapping[str, Any]],
    execution_plan: SafeSampleExecutionPlanView,
    pre_runtime_errors: Iterable[Mapping[str, Any]] = (),
    runtime_run: Mapping[str, Any] | None,
    fallback_error: Mapping[str, Any],
) -> SafeSampleCliResult:
    """Choose the most useful top-level CLI error and exit code.

    Policy and target-plan errors are safety/configuration gates and keep exit
    code 4. Execution-plan blockers and runtime errors are run-readiness
    failures, so they keep exit code 3. The generic fallback is only emitted if
    no structured blocker was produced, which should be rare and defensive.
    """

    safety_errors = [*_copy_errors(policy_errors), *_copy_errors(target_plan_errors)]
    if safety_errors:
        return SafeSampleCliResult(errors=tuple(safety_errors), exit_code=4)

    plan_blockers = _copy_errors(execution_plan.blockers)
    if plan_blockers:
        return SafeSampleCliResult(errors=tuple(plan_blockers), exit_code=3)

    live_input_errors = _copy_errors(pre_runtime_errors)
    if live_input_errors:
        return SafeSampleCliResult(
            errors=tuple(live_input_errors),
            exit_code=_runtime_failure_exit_code(live_input_errors),
        )

    runtime_errors = _runtime_errors(runtime_run)
    if runtime_errors:
        return SafeSampleCliResult(
            errors=tuple(runtime_errors),
            exit_code=_runtime_failure_exit_code(runtime_errors),
        )

    if runtime_run is not None and str(runtime_run.get("execution_status") or "") == "succeeded":
        return SafeSampleCliResult(errors=(), exit_code=0)

    return SafeSampleCliResult(errors=(dict(fallback_error),), exit_code=3)


def _runtime_errors(runtime_run: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    if runtime_run is None:
        return []
    return _copy_errors(runtime_run.get("errors", ()))


def _copy_errors(errors: object) -> list[dict[str, Any]]:
    if not isinstance(errors, Iterable) or isinstance(errors, (str, bytes, Mapping)):
        return []
    return [dict(error) for error in errors if isinstance(error, Mapping)]


def _runtime_failure_exit_code(errors: Iterable[Mapping[str, Any]]) -> int:
    codes = [error.get("code") for error in errors]
    if any(str(code or "").startswith("DPONE_INTERNAL_") for code in codes):
        return 5
    return 4 if any(_is_security_failure(code) for code in codes) else 3


def _is_security_failure(code: object) -> bool:
    value = str(code or "")
    return (
        value.startswith(("DPONE_SECURITY_", "DPONE_ROUTE_ATTESTATION_"))
        or value.endswith("_FINGERPRINT_MISMATCH")
        or value
        in {
            "DPONE_AUTHORING_SOURCE_CHANGED_DURING_BUILD",
            "DPONE_CACHE_CHECKSUM_MISMATCH",
            "DPONE_SAFE_SAMPLE_LIVE_INPUT_PATH_INVALID",
            "DPONE_SAFE_SAMPLE_LIVE_POLICY_INVALID",
            "DPONE_SAFE_SAMPLE_LIVE_POLICY_NOT_AUTHORIZED",
            "DPONE_SAFE_SAMPLE_PIPELINE_SOURCE_FINGERPRINT_MISMATCH",
            "DPONE_SAFE_SAMPLE_PIPELINE_SOURCE_PIN_MISSING",
            "DPONE_SAFE_SAMPLE_TEMPORARY_TARGET_CONNECTION_MISMATCH",
        }
    )


__all__ = ["SafeSampleCliResult", "select_safe_sample_cli_result"]
