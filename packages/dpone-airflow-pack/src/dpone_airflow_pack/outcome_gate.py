from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from dpone_airflow_pack.deployment_identity import (
    deployment_identity_error as _deployment_identity_error,
)
from dpone_airflow_pack.outcome_identity import is_sha256 as _is_sha256
from dpone_airflow_pack.outcome_identity import run_identity_error as _run_identity_error

AIRFLOW_OUTCOME_STRICT_FAIL = "strict_fail"
AIRFLOW_OUTCOME_XCOM_THEN_GATE = "xcom_then_gate"
AIRFLOW_OUTCOME_MODES = (AIRFLOW_OUTCOME_STRICT_FAIL, AIRFLOW_OUTCOME_XCOM_THEN_GATE)
_OUTCOME_SOURCE = "dpone airflow-pack outcome-gate"
_SUMMARY_REQUIRED_FIELDS = frozenset(
    {"kind", "schema_version", "producer", "status", "run_spec_path", "runtime_evidence_path"}
)
_SUMMARY_FIELDS = _SUMMARY_REQUIRED_FIELDS | frozenset(
    "runtime_profile_path runtime_evidence_sha256 runtime_evidence failed_step step_counts "
    "artifact_paths warnings interval backfill recovery blockers run_identity "
    "deployment_identity dbt_execution_evidence_ref launch_pin_ref".split()
)


@dataclass(frozen=True, slots=True)
class AirflowPackIssue:
    code: str
    message: str = ""
    path: str = ""
    source: str = _OUTCOME_SOURCE

    def to_jsonable(self) -> dict[str, str]:
        return {
            "code": self.code,
            "message": self.message,
            "path": self.path,
            "source": self.source,
        }


@dataclass(frozen=True, slots=True)
class GitOpsAirflowOutcomeGateReport:
    xcom_summary_path: str
    required_status: str
    status: str
    passed: bool
    run_spec_path: str = ""
    runtime_evidence_path: str = ""
    runtime_evidence_sha256: str | None = None
    failed_step: str | None = None
    step_counts: dict[str, int] | None = None
    warnings: tuple[AirflowPackIssue, ...] = ()
    blockers: tuple[AirflowPackIssue, ...] = ()
    kind: str = "gitops.airflow_outcome_gate"
    schema_version: str = "1"
    producer: str = _OUTCOME_SOURCE

    def to_jsonable(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "kind": self.kind,
            "schema_version": self.schema_version,
            "producer": self.producer,
            "xcom_summary_path": self.xcom_summary_path,
            "required_status": self.required_status,
            "status": self.status,
            "passed": self.passed,
            "run_spec_path": self.run_spec_path,
            "runtime_evidence_path": self.runtime_evidence_path,
            "failed_step": self.failed_step,
            "warnings": [warning.to_jsonable() for warning in self.warnings],
            "blockers": [blocker.to_jsonable() for blocker in self.blockers],
        }
        if self.runtime_evidence_sha256 is not None:
            payload["runtime_evidence_sha256"] = self.runtime_evidence_sha256
        if self.step_counts is not None:
            payload["step_counts"] = dict(self.step_counts)
        return payload

    def to_json(self) -> str:
        return json.dumps(self.to_jsonable(), ensure_ascii=False, indent=2) + "\n"


class GitOpsAirflowOutcomeGateEvaluator:
    """Evaluate a final Airflow XCom summary without importing full dpone."""

    def evaluate(
        self,
        *,
        xcom_summary_path: str,
        xcom_summary: Mapping[str, Any],
        required_status: str = "passed",
        expected_run_identity: Mapping[str, Any] | None = None,
        expected_deployment_identity: Mapping[str, Any] | None = None,
        expected_runtime_evidence_sha256: str | None = None,
    ) -> GitOpsAirflowOutcomeGateReport:
        normalized_required = _text(required_status) or "passed"
        status = _text(xcom_summary.get("status")) or "unknown"
        blockers = (
            *_summary_contract_blockers(path=xcom_summary_path, summary=xcom_summary),
            *_kind_blockers(path=xcom_summary_path, summary=xcom_summary),
            *_status_blockers(
                path=xcom_summary_path,
                status=status,
                required_status=normalized_required,
            ),
            *_passed_identity_blockers(
                path=xcom_summary_path,
                status=status,
                summary=xcom_summary,
                expected_run_identity=expected_run_identity,
            ),
            *_passed_deployment_identity_blockers(
                path=xcom_summary_path,
                status=status,
                summary=xcom_summary,
                expected_deployment_identity=expected_deployment_identity,
            ),
            *_passed_evidence_digest_blockers(
                path=xcom_summary_path,
                status=status,
                summary=xcom_summary,
                expected_runtime_evidence_sha256=expected_runtime_evidence_sha256,
            ),
            *_issues(xcom_summary.get("blockers")),
        )
        return GitOpsAirflowOutcomeGateReport(
            xcom_summary_path=xcom_summary_path,
            required_status=normalized_required,
            status=status,
            passed=not blockers,
            run_spec_path=_text(xcom_summary.get("run_spec_path")),
            runtime_evidence_path=_text(xcom_summary.get("runtime_evidence_path")),
            runtime_evidence_sha256=_optional_text(xcom_summary.get("runtime_evidence_sha256")),
            failed_step=_optional_text(xcom_summary.get("failed_step")),
            step_counts=_step_counts(xcom_summary.get("step_counts")),
            warnings=_issues(xcom_summary.get("warnings")),
            blockers=blockers,
        )


def normalize_airflow_outcome_mode(raw_mode: object) -> str:
    mode = _text(raw_mode) or AIRFLOW_OUTCOME_STRICT_FAIL
    return mode if mode in AIRFLOW_OUTCOME_MODES else AIRFLOW_OUTCOME_STRICT_FAIL


def airflow_outcome_mode_names() -> tuple[str, ...]:
    return AIRFLOW_OUTCOME_MODES


def _kind_blockers(*, path: str, summary: Mapping[str, Any]) -> tuple[AirflowPackIssue, ...]:
    if summary.get("kind") == "gitops.airflow_xcom_summary":
        return ()
    return (
        AirflowPackIssue(
            code="airflow_outcome_kind_invalid",
            message="XCom summary kind must be gitops.airflow_xcom_summary",
            path=path,
        ),
    )


def _status_blockers(*, path: str, status: str, required_status: str) -> tuple[AirflowPackIssue, ...]:
    if status == required_status:
        return ()
    return (
        AirflowPackIssue(
            code="airflow_outcome_failed",
            message=f"Airflow XCom outcome status is `{status}`, expected `{required_status}`",
            path=path,
        ),
    )


def _summary_contract_blockers(*, path: str, summary: Mapping[str, Any]) -> tuple[AirflowPackIssue, ...]:
    unknown = sorted(str(key) for key in summary if key not in _SUMMARY_FIELDS)
    missing = sorted(_SUMMARY_REQUIRED_FIELDS.difference(summary))
    invalid = [field for field in _SUMMARY_REQUIRED_FIELDS if field in summary and not isinstance(summary[field], str)]
    if summary.get("schema_version") not in {None, "1"}:
        invalid.append("schema_version")
    invalid.extend(_invalid_optional_summary_fields(summary))
    problems = tuple(
        filter(
            None,
            (
                f"unsupported fields: {', '.join(unknown)}" if unknown else "",
                f"missing fields: {', '.join(missing)}" if missing else "",
                f"invalid fields: {', '.join(sorted(set(invalid)))}" if invalid else "",
            ),
        )
    )
    return tuple(
        _blocker(
            code="airflow_outcome_contract_invalid",
            message=f"XCom summary contract is invalid ({problem})",
            path=path,
        )
        for problem in problems
    )


def _invalid_optional_summary_fields(summary: Mapping[str, Any]) -> list[str]:
    validators = {
        "runtime_profile_path": lambda value: isinstance(value, str),
        "runtime_evidence_sha256": lambda value: isinstance(value, str),
        "runtime_evidence": lambda value: isinstance(value, Mapping),
        "failed_step": lambda value: value is None or isinstance(value, str),
        "step_counts": _is_integer_mapping,
        "artifact_paths": _is_string_mapping,
        "warnings": _is_issue_list,
        "interval": lambda value: isinstance(value, Mapping),
        "backfill": lambda value: isinstance(value, Mapping),
        "recovery": lambda value: value is None or isinstance(value, Mapping),
        "blockers": _is_issue_list,
        "run_identity": lambda value: isinstance(value, Mapping),
        "deployment_identity": lambda value: not _deployment_identity_error(value),
        "dbt_execution_evidence_ref": _is_dbt_evidence_ref,
        "launch_pin_ref": lambda value: isinstance(value, Mapping),
    }
    return [field for field, validator in validators.items() if field in summary and not validator(summary[field])]


def _is_dbt_evidence_ref(value: object) -> bool:
    if not isinstance(value, Mapping) or set(value) != {
        "schema",
        "workflow_id",
        "sha256",
        "bytes",
        "storage_scope",
    }:
        return False
    size = value.get("bytes")
    return (
        value.get("schema") == "dpone.dbt-execution-evidence-ref.v1"
        and isinstance(value.get("workflow_id"), str)
        and bool(str(value["workflow_id"]).strip())
        and _is_sha256(value.get("sha256"))
        and not isinstance(size, bool)
        and isinstance(size, int)
        and 0 < size <= 16 * 1024 * 1024
        and value.get("storage_scope") == "dbt_spool"
    )


def _passed_identity_blockers(
    *,
    path: str,
    status: str,
    summary: Mapping[str, Any],
    expected_run_identity: Mapping[str, Any] | None,
) -> tuple[AirflowPackIssue, ...]:
    if status != "passed":
        return ()
    observed = summary.get("run_identity")
    if observed is None and expected_run_identity is None:
        return ()
    observed_error = _run_identity_error(observed)
    expected_error = "" if expected_run_identity is None else _run_identity_error(expected_run_identity)
    if observed_error or expected_error:
        detail = observed_error or f"provider expectation {expected_error}"
        return (
            _blocker(
                code="airflow_outcome_identity_invalid",
                message=f"Passed XCom run identity is incomplete or invalid: {detail}",
                path=path,
            ),
        )
    assert isinstance(observed, Mapping)
    if expected_run_identity is not None and dict(observed) != dict(expected_run_identity):
        return (
            _blocker(
                code="airflow_outcome_identity_mismatch",
                message="Passed XCom run identity does not match the provider deployment context",
                path=path,
            ),
        )
    return ()


def _passed_deployment_identity_blockers(
    *,
    path: str,
    status: str,
    summary: Mapping[str, Any],
    expected_deployment_identity: Mapping[str, Any] | None,
) -> tuple[AirflowPackIssue, ...]:
    if status != "passed":
        return ()
    observed = summary.get("deployment_identity")
    if observed is None and expected_deployment_identity is None:
        return ()
    observed_error = _deployment_identity_error(observed)
    expected_error = (
        "" if expected_deployment_identity is None else _deployment_identity_error(expected_deployment_identity)
    )
    if observed_error or expected_error:
        detail = observed_error or f"provider expectation {expected_error}"
        return (
            _blocker(
                code="airflow_outcome_deployment_identity_invalid",
                message=f"Passed XCom deployment identity is incomplete or invalid: {detail}",
                path=path,
            ),
        )
    assert isinstance(observed, Mapping)
    if expected_deployment_identity is not None and dict(observed) != dict(expected_deployment_identity):
        return (
            _blocker(
                code="airflow_outcome_deployment_identity_mismatch",
                message="Passed XCom deployment identity does not match the provider activation context",
                path=path,
            ),
        )
    return ()


def _passed_evidence_digest_blockers(
    *,
    path: str,
    status: str,
    summary: Mapping[str, Any],
    expected_runtime_evidence_sha256: str | None,
) -> tuple[AirflowPackIssue, ...]:
    if status != "passed":
        return ()
    observed = summary.get("runtime_evidence_sha256")
    if not _is_sha256(observed):
        return (
            _blocker(
                code="airflow_outcome_evidence_digest_invalid",
                message="Passed XCom summary requires a canonical runtime evidence digest",
                path=path,
            ),
        )
    if expected_runtime_evidence_sha256 is None:
        return ()
    if not _is_sha256(expected_runtime_evidence_sha256) or observed != expected_runtime_evidence_sha256:
        return (
            _blocker(
                code="airflow_outcome_evidence_digest_mismatch",
                message="Passed XCom runtime evidence digest does not match the provider expectation",
                path=path,
            ),
        )
    return ()


def _is_integer_mapping(value: object) -> bool:
    return isinstance(value, Mapping) and all(
        isinstance(key, str) and isinstance(item, int) and not isinstance(item, bool) for key, item in value.items()
    )


def _is_string_mapping(value: object) -> bool:
    return isinstance(value, Mapping) and all(
        isinstance(key, str) and isinstance(item, str) for key, item in value.items()
    )


def _is_issue_list(value: object) -> bool:
    return isinstance(value, list) and all(
        isinstance(item, Mapping)
        and all(isinstance(item.get(field), str) for field in ("code", "message", "path", "source"))
        for item in value
    )


def _blocker(*, code: str, message: str, path: str) -> AirflowPackIssue:
    return AirflowPackIssue(code=code, message=message, path=path)


def _issues(raw_items: object) -> tuple[AirflowPackIssue, ...]:
    if not isinstance(raw_items, list):
        return ()
    issues: list[AirflowPackIssue] = []
    for raw_item in raw_items:
        if not isinstance(raw_item, Mapping):
            continue
        code = _text(raw_item.get("code"))
        if code:
            issues.append(
                AirflowPackIssue(
                    code=code,
                    message=_text(raw_item.get("message")),
                    path=_text(raw_item.get("path")),
                    source=_text(raw_item.get("source")) or "dpone airflow-pack runtime",
                )
            )
    return tuple(issues)


def _step_counts(value: object) -> dict[str, int] | None:
    if not isinstance(value, Mapping):
        return None
    counts: dict[str, int] = {}
    for key, raw_count in value.items():
        if isinstance(raw_count, bool) or not isinstance(raw_count, int):
            continue
        counts[str(key)] = raw_count
    return counts


def _optional_text(value: object) -> str | None:
    text = _text(value)
    return text or None


def _text(value: object) -> str:
    return str(value or "").strip()


__all__ = [
    "AIRFLOW_OUTCOME_MODES",
    "AIRFLOW_OUTCOME_STRICT_FAIL",
    "AIRFLOW_OUTCOME_XCOM_THEN_GATE",
    "AirflowPackIssue",
    "GitOpsAirflowOutcomeGateEvaluator",
    "GitOpsAirflowOutcomeGateReport",
    "airflow_outcome_mode_names",
    "normalize_airflow_outcome_mode",
]
