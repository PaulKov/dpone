from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from dpone.services.safe_sample_cli_result import select_safe_sample_cli_result


@dataclass(frozen=True, slots=True)
class _Plan:
    blockers: tuple[dict[str, Any], ...]


def _plan(*blockers: dict[str, Any]) -> _Plan:
    return _Plan(blockers=blockers)


def _error(code: str, stage: str = "unit") -> dict[str, object]:
    return {
        "schema": "dpone.error.v1",
        "code": code,
        "stage": stage,
        "severity": "error",
        "message": code,
        "fixes": [],
    }


def test_safe_sample_cli_result_prefers_safety_errors() -> None:
    result = select_safe_sample_cli_result(
        policy_errors=[_error("DPONE_SECURITY_SAMPLE_PUSHDOWN_REQUIRED")],
        target_plan_errors=[],
        execution_plan=_plan(_error("DPONE_DEPLOYMENT_NOT_RUNNABLE")),
        runtime_run={"errors": [_error("DPONE_SAFE_SAMPLE_DATA_COPY_NOT_IMPLEMENTED")]},
        fallback_error=_error("DPONE_SAFE_SAMPLE_RUN_NOT_IMPLEMENTED"),
    )

    assert result.exit_code == 4
    assert result.passed is False
    assert result.status == "error"
    assert [error["code"] for error in result.errors] == ["DPONE_SECURITY_SAMPLE_PUSHDOWN_REQUIRED"]


def test_safe_sample_cli_result_surfaces_execution_plan_blockers_before_runtime_errors() -> None:
    result = select_safe_sample_cli_result(
        policy_errors=[],
        target_plan_errors=[],
        execution_plan=_plan(_error("DPONE_DEPLOYMENT_NOT_RUNNABLE", stage="safe_sample_execution_plan")),
        runtime_run={"errors": [_error("DPONE_SAFE_SAMPLE_DATA_COPY_NOT_IMPLEMENTED")]},
        fallback_error=_error("DPONE_SAFE_SAMPLE_RUN_NOT_IMPLEMENTED"),
    )

    assert result.exit_code == 3
    assert result.errors[0]["code"] == "DPONE_DEPLOYMENT_NOT_RUNNABLE"
    assert result.errors[0]["stage"] == "safe_sample_execution_plan"


def test_safe_sample_cli_result_surfaces_runtime_errors_when_plan_is_runnable() -> None:
    result = select_safe_sample_cli_result(
        policy_errors=[],
        target_plan_errors=[],
        execution_plan=_plan(),
        runtime_run={"errors": [_error("DPONE_SAFE_SAMPLE_DATA_COPY_NOT_IMPLEMENTED")]},
        fallback_error=_error("DPONE_SAFE_SAMPLE_RUN_NOT_IMPLEMENTED"),
    )

    assert result.exit_code == 3
    assert [error["code"] for error in result.errors] == ["DPONE_SAFE_SAMPLE_DATA_COPY_NOT_IMPLEMENTED"]


def test_safe_sample_cli_result_surfaces_pre_runtime_live_input_error_before_fallback() -> None:
    result = select_safe_sample_cli_result(
        policy_errors=[],
        target_plan_errors=[],
        execution_plan=_plan(),
        pre_runtime_errors=[_error("DPONE_SAFE_SAMPLE_LIVE_INPUTS_INCOMPLETE")],
        runtime_run=None,
        fallback_error=_error("DPONE_SAFE_SAMPLE_RUN_NOT_IMPLEMENTED"),
    )

    assert result.exit_code == 3
    assert [error["code"] for error in result.errors] == ["DPONE_SAFE_SAMPLE_LIVE_INPUTS_INCOMPLETE"]


def test_safe_sample_cli_result_classifies_route_attestation_failure_as_security_violation() -> None:
    result = select_safe_sample_cli_result(
        policy_errors=[],
        target_plan_errors=[],
        execution_plan=_plan(),
        pre_runtime_errors=[_error("DPONE_ROUTE_ATTESTATION_SIGNATURE_INVALID")],
        runtime_run=None,
        fallback_error=_error("DPONE_SAFE_SAMPLE_RUN_NOT_IMPLEMENTED"),
    )

    assert result.exit_code == 4
    assert [error["code"] for error in result.errors] == ["DPONE_ROUTE_ATTESTATION_SIGNATURE_INVALID"]


def test_safe_sample_cli_result_classifies_environment_fingerprint_mismatch_as_security_violation() -> None:
    result = select_safe_sample_cli_result(
        policy_errors=[],
        target_plan_errors=[],
        execution_plan=_plan(),
        pre_runtime_errors=[_error("DPONE_SAFE_SAMPLE_BINDING_SET_FINGERPRINT_MISMATCH")],
        runtime_run=None,
        fallback_error=_error("DPONE_SAFE_SAMPLE_RUN_NOT_IMPLEMENTED"),
    )

    assert result.exit_code == 4
    assert [error["code"] for error in result.errors] == ["DPONE_SAFE_SAMPLE_BINDING_SET_FINGERPRINT_MISMATCH"]


def test_safe_sample_cli_result_classifies_unexpected_assembly_failure_as_internal() -> None:
    result = select_safe_sample_cli_result(
        policy_errors=[],
        target_plan_errors=[],
        execution_plan=_plan(),
        pre_runtime_errors=[_error("DPONE_INTERNAL_SAFE_SAMPLE_LIVE_ASSEMBLY_FAILED")],
        runtime_run=None,
        fallback_error=_error("DPONE_SAFE_SAMPLE_RUN_NOT_IMPLEMENTED"),
    )

    assert result.exit_code == 5
    assert [error["code"] for error in result.errors] == ["DPONE_INTERNAL_SAFE_SAMPLE_LIVE_ASSEMBLY_FAILED"]


def test_safe_sample_cli_result_only_uses_fallback_without_structured_blocker() -> None:
    result = select_safe_sample_cli_result(
        policy_errors=[],
        target_plan_errors=[],
        execution_plan=_plan(),
        runtime_run={"execution_status": "failed", "errors": []},
        fallback_error=_error("DPONE_SAFE_SAMPLE_RUN_NOT_IMPLEMENTED"),
    )

    assert result.exit_code == 3
    assert [error["code"] for error in result.errors] == ["DPONE_SAFE_SAMPLE_RUN_NOT_IMPLEMENTED"]


def test_safe_sample_cli_result_can_represent_successful_runtime_run() -> None:
    result = select_safe_sample_cli_result(
        policy_errors=[],
        target_plan_errors=[],
        execution_plan=_plan(),
        runtime_run={"execution_status": "succeeded", "errors": []},
        fallback_error=_error("DPONE_SAFE_SAMPLE_RUN_NOT_IMPLEMENTED"),
    )

    assert result.exit_code == 0
    assert result.passed is True
    assert result.status == "success"
    assert result.errors == ()
