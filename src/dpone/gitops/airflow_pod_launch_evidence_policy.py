from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.gitops.airflow_pod_launch_evidence_checks import (
    AIRFLOW_POD_LAUNCH_EVIDENCE_SOURCE,
    GitOpsAirflowPodLaunchEvidenceCheck,
)
from dpone.gitops.airflow_pod_launch_observed_policy import append_container_and_event_checks
from dpone.gitops.models import GitOpsIssue

_SOURCE = AIRFLOW_POD_LAUNCH_EVIDENCE_SOURCE


def append_artifact_checks(
    checks: list[GitOpsAirflowPodLaunchEvidenceCheck],
    blockers: list[GitOpsIssue],
    *,
    runtime_profile_path: str,
    runtime_profile: Mapping[str, Any],
    pod_contract_path: str,
    pod_contract: Mapping[str, Any],
    image_contract_path: str | None,
    image_contract: Mapping[str, Any] | None,
    runtime_evidence_path: str | None,
    runtime_evidence: Mapping[str, Any] | None,
    xcom_summary_path: str | None,
    xcom_summary: Mapping[str, Any] | None,
    runner_policy: str,
) -> None:
    _append_check(
        checks,
        blockers,
        name="airflow_pod_runtime_profile_kind",
        passed=runtime_profile.get("kind") == "gitops.airflow_runtime_profile",
        path=runtime_profile_path,
        message="Runtime profile artifact has the expected kind",
    )
    _append_check(
        checks,
        blockers,
        name="airflow_pod_contract_kind",
        passed=pod_contract.get("kind") == "gitops.airflow_pod_contract",
        path=pod_contract_path,
        message="Pod contract artifact has the expected kind",
    )
    if image_contract is not None:
        _append_check(
            checks,
            blockers,
            name="airflow_pod_image_contract_kind",
            passed=image_contract.get("kind") == "gitops.airflow_image_contract.v1",
            path=image_contract_path or "image_contract",
            message="Image contract artifact has the expected kind",
            code="airflow_pod_image_contract_invalid",
        )
    _append_runtime_evidence_check(
        checks,
        blockers,
        runtime_evidence_path=runtime_evidence_path,
        runtime_evidence=runtime_evidence,
        runner_policy=runner_policy,
    )
    _append_xcom_check(
        checks,
        blockers,
        xcom_summary_path=xcom_summary_path,
        xcom_summary=xcom_summary,
        runner_policy=runner_policy,
    )


def append_profile_contract_checks(
    checks: list[GitOpsAirflowPodLaunchEvidenceCheck],
    blockers: list[GitOpsIssue],
    *,
    image: str,
    image_digest: str | None,
    namespace: str,
    service_account: str,
    runtime_profile_path: str,
    pod_contract: Mapping[str, Any],
    image_contract: Mapping[str, Any] | None,
    image_contract_path: str | None,
    runner_policy: str,
) -> None:
    _append_check(
        checks,
        blockers,
        name="airflow_pod_namespace",
        passed=bool(namespace) and namespace == _text(pod_contract.get("namespace")),
        path=runtime_profile_path,
        message="Runtime profile namespace matches the pod contract",
        code="airflow_pod_namespace_mismatch",
    )
    _append_check(
        checks,
        blockers,
        name="airflow_pod_service_account",
        passed=bool(service_account) and service_account == _text(pod_contract.get("service_account")),
        path=runtime_profile_path,
        message="Runtime profile service account matches the pod contract",
        code="airflow_pod_service_account_mismatch",
    )
    _append_check(
        checks,
        blockers,
        name="airflow_pod_image_matches_contract",
        passed=bool(image) and image == _text(pod_contract.get("image")),
        path="pod_contract.image",
        message="Runtime image matches the pod contract image",
        code="airflow_pod_image_mismatch",
    )
    if image_contract is not None:
        _append_check(
            checks,
            blockers,
            name="airflow_pod_image_contract_matches",
            passed=not _text(image_contract.get("image")) or image == _text(image_contract.get("image")),
            path=image_contract_path or "image_contract",
            message="Image contract image matches the runtime image",
            code="airflow_pod_image_contract_mismatch",
        )
    if runner_policy == "release":
        _append_check(
            checks,
            blockers,
            name="airflow_pod_image_digest",
            passed=bool(image_digest),
            path=image_contract_path or "image_digest",
            message="Release pod watch uses an immutable image digest",
            code="airflow_pod_image_digest_required",
        )


def append_observed_checks(
    checks: list[GitOpsAirflowPodLaunchEvidenceCheck],
    blockers: list[GitOpsIssue],
    *,
    report: Any,
    observed_pod: Any,
) -> None:
    _append_check(
        checks,
        blockers,
        name="airflow_pod_name_observed",
        passed=observed_pod.pod_name == report.pod_name,
        path="observed_pod.pod_name",
        message="Observed pod name matches the planned pod",
        code="airflow_pod_name_mismatch",
    )
    _append_check(
        checks,
        blockers,
        name="airflow_pod_namespace_observed",
        passed=observed_pod.namespace == report.namespace,
        path="observed_pod.namespace",
        message="Observed namespace matches the runtime profile",
        code="airflow_pod_namespace_mismatch",
    )
    _append_check(
        checks,
        blockers,
        name="airflow_pod_service_account_observed",
        passed=observed_pod.service_account == report.service_account,
        path="observed_pod.service_account",
        message="Observed service account matches the runtime profile",
        code="airflow_pod_service_account_mismatch",
    )
    if report.expected_phase != "Any":
        _append_check(
            checks,
            blockers,
            name="airflow_pod_phase",
            passed=observed_pod.phase == report.expected_phase,
            path="observed_pod.phase",
            message=f"Observed pod phase is {report.expected_phase}",
            code="airflow_pod_phase_mismatch",
        )
    append_container_and_event_checks(
        checks,
        blockers,
        report=report,
        observed_pod=observed_pod,
        issue_factory=pod_evidence_issue,
    )


def pod_command_blockers(
    *,
    commands: tuple[Any, ...],
    results: tuple[Any, ...],
) -> list[GitOpsIssue]:
    required_names = {command.name for command in commands if command.required}
    return [
        pod_evidence_issue(
            code="airflow_pod_watch_command_failed",
            message=f"Pod watch command {result.name} failed with exit code {result.exit_code}",
            path=result.name,
        )
        for result in results
        if result.exit_code != 0 and result.name in required_names
    ]


def pod_evidence_issue(*, code: str, message: str, path: str) -> GitOpsIssue:
    return GitOpsIssue(code=code, message=message, path=path, source=_SOURCE)


def _append_runtime_evidence_check(
    checks: list[GitOpsAirflowPodLaunchEvidenceCheck],
    blockers: list[GitOpsIssue],
    *,
    runtime_evidence_path: str | None,
    runtime_evidence: Mapping[str, Any] | None,
    runner_policy: str,
) -> None:
    if runtime_evidence is None:
        if runner_policy == "release":
            blockers.append(
                pod_evidence_issue(
                    code="airflow_pod_runtime_evidence_required",
                    message="Release pod watch requires runtime evidence from run-spec-exec",
                    path=runtime_evidence_path or "runtime_evidence",
                )
            )
        return
    _append_check(
        checks,
        blockers,
        name="airflow_pod_runtime_evidence_status",
        passed=runtime_evidence.get("kind") == "gitops.airflow_runtime_evidence"
        and runtime_evidence.get("status") == "passed",
        path=runtime_evidence_path or "runtime_evidence",
        message="Runtime evidence has passed status",
        code="airflow_pod_runtime_evidence_failed",
    )


def _append_xcom_check(
    checks: list[GitOpsAirflowPodLaunchEvidenceCheck],
    blockers: list[GitOpsIssue],
    *,
    xcom_summary_path: str | None,
    xcom_summary: Mapping[str, Any] | None,
    runner_policy: str,
) -> None:
    if xcom_summary is None:
        if runner_policy == "release":
            blockers.append(
                pod_evidence_issue(
                    code="airflow_pod_xcom_summary_required",
                    message="Release pod watch requires final XCom summary evidence",
                    path=xcom_summary_path or "xcom_summary",
                )
            )
        return
    _append_check(
        checks,
        blockers,
        name="airflow_pod_xcom_summary_status",
        passed=xcom_summary.get("kind") == "gitops.airflow_xcom_summary" and xcom_summary.get("status") == "passed",
        path=xcom_summary_path or "xcom_summary",
        message="Final XCom summary has passed status",
        code="airflow_pod_xcom_failed",
    )


def _append_check(
    checks: list[GitOpsAirflowPodLaunchEvidenceCheck],
    blockers: list[GitOpsIssue],
    *,
    name: str,
    passed: bool,
    path: str,
    message: str,
    severity: str = "blocker",
    code: str | None = None,
) -> None:
    checks.append(
        GitOpsAirflowPodLaunchEvidenceCheck(
            name=name,
            passed=passed,
            severity=severity,
            message=message,
            path=path,
        )
    )
    if not passed and severity == "blocker":
        blockers.append(pod_evidence_issue(code=code or name, message=message, path=path))


def _text(value: object) -> str:
    return str(value or "").strip()


__all__ = [
    "append_artifact_checks",
    "append_observed_checks",
    "append_profile_contract_checks",
    "pod_command_blockers",
    "pod_evidence_issue",
]
