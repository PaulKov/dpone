from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from dpone.gitops.models import GitOpsIssue

AIRFLOW_OUTCOME_STRICT_FAIL = "strict_fail"
AIRFLOW_OUTCOME_XCOM_THEN_GATE = "xcom_then_gate"
AIRFLOW_OUTCOME_MODES = (AIRFLOW_OUTCOME_STRICT_FAIL, AIRFLOW_OUTCOME_XCOM_THEN_GATE)


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
    warnings: tuple[GitOpsIssue, ...] = ()
    blockers: tuple[GitOpsIssue, ...] = ()
    kind: str = "gitops.airflow_outcome_gate"
    schema_version: str = "1"
    producer: str = "dpone gitops airflow outcome-gate"

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
    """Evaluate a final Airflow XCom summary as a downstream task gate."""

    def evaluate(
        self,
        *,
        xcom_summary_path: str,
        xcom_summary: Mapping[str, Any],
        required_status: str = "passed",
    ) -> GitOpsAirflowOutcomeGateReport:
        normalized_required = _text(required_status) or "passed"
        status = _text(xcom_summary.get("status")) or "unknown"
        upstream_blockers = _issues(xcom_summary.get("blockers"))
        blockers = (
            *_kind_blockers(path=xcom_summary_path, summary=xcom_summary),
            *_status_blockers(
                path=xcom_summary_path,
                status=status,
                required_status=normalized_required,
            ),
            *upstream_blockers,
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
            blockers=blockers,
        )


def normalize_airflow_outcome_mode(raw_mode: object) -> str:
    mode = _text(raw_mode) or AIRFLOW_OUTCOME_STRICT_FAIL
    if mode in AIRFLOW_OUTCOME_MODES:
        return mode
    return AIRFLOW_OUTCOME_STRICT_FAIL


def airflow_outcome_mode_names() -> tuple[str, ...]:
    return AIRFLOW_OUTCOME_MODES


def _kind_blockers(*, path: str, summary: Mapping[str, Any]) -> tuple[GitOpsIssue, ...]:
    if summary.get("kind") == "gitops.airflow_xcom_summary":
        return ()
    return (
        GitOpsIssue(
            code="airflow_outcome_kind_invalid",
            message="XCom summary kind must be gitops.airflow_xcom_summary",
            path=path,
            source="dpone gitops airflow outcome-gate",
        ),
    )


def _status_blockers(*, path: str, status: str, required_status: str) -> tuple[GitOpsIssue, ...]:
    if status == required_status:
        return ()
    return (
        GitOpsIssue(
            code="airflow_outcome_failed",
            message=f"Airflow XCom outcome status is `{status}`, expected `{required_status}`",
            path=path,
            source="dpone gitops airflow outcome-gate",
        ),
    )


def _issues(raw_items: object) -> tuple[GitOpsIssue, ...]:
    if not isinstance(raw_items, list):
        return ()
    issues: list[GitOpsIssue] = []
    for raw_item in raw_items:
        if not isinstance(raw_item, Mapping):
            continue
        code = _text(raw_item.get("code"))
        message = _text(raw_item.get("message"))
        path = _text(raw_item.get("path"))
        source = _text(raw_item.get("source")) or "dpone gitops airflow run-spec-exec"
        if code:
            issues.append(GitOpsIssue(code=code, message=message, path=path, source=source))
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
    "GitOpsAirflowOutcomeGateEvaluator",
    "GitOpsAirflowOutcomeGateReport",
    "airflow_outcome_mode_names",
    "normalize_airflow_outcome_mode",
]
