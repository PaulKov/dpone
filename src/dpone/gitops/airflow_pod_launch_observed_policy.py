from __future__ import annotations

from typing import Any

from dpone.gitops.airflow_pod_launch_evidence_checks import GitOpsAirflowPodLaunchEvidenceCheck
from dpone.gitops.models import GitOpsIssue

_BAD_EVENT_REASONS = {
    "BackOff",
    "CrashLoopBackOff",
    "ErrImagePull",
    "Failed",
    "FailedScheduling",
    "ImagePullBackOff",
    "OOMKilled",
}
_BAD_CONTAINER_REASONS = {"CrashLoopBackOff", "Error", "OOMKilled"}


def append_container_and_event_checks(
    checks: list[GitOpsAirflowPodLaunchEvidenceCheck],
    blockers: list[GitOpsIssue],
    *,
    report: Any,
    observed_pod: Any,
    issue_factory: Any,
) -> None:
    base = observed_pod.container("base") or (observed_pod.containers[0] if observed_pod.containers else None)
    if base is None:
        blockers.append(
            issue_factory(
                code="airflow_pod_base_container_missing",
                message="Observed pod does not include a base container status",
                path="observed_pod.containers",
            )
        )
        return
    _append_check(
        checks,
        blockers,
        name="airflow_pod_base_image",
        passed=base.image == report.image,
        path="observed_pod.containers.base.image",
        message="Observed base container image matches the runtime image",
        code="airflow_pod_image_mismatch",
        issue_factory=issue_factory,
    )
    if report.runner_policy == "release":
        _append_release_container_checks(blockers, report=report, base=base, issue_factory=issue_factory)
    _append_event_checks(blockers, events=observed_pod.events, issue_factory=issue_factory)


def _append_release_container_checks(
    blockers: list[GitOpsIssue],
    *,
    report: Any,
    base: Any,
    issue_factory: Any,
) -> None:
    if report.image_digest and report.image_digest not in base.image_id:
        blockers.append(
            issue_factory(
                code="airflow_pod_image_digest_mismatch",
                message="Observed base container imageID does not contain the expected immutable digest",
                path="observed_pod.containers.base.image_id",
            )
        )
    if base.restart_count > 0:
        blockers.append(
            issue_factory(
                code="airflow_pod_restart_detected",
                message="Observed base container restarted during the pod run",
                path="observed_pod.containers.base.restart_count",
            )
        )
    if base.exit_code not in (None, 0) or (base.reason or "") in _BAD_CONTAINER_REASONS:
        blockers.append(
            issue_factory(
                code="airflow_pod_container_failed",
                message="Observed base container did not terminate cleanly",
                path="observed_pod.containers.base.exit_code",
            )
        )


def _append_event_checks(
    blockers: list[GitOpsIssue],
    *,
    events: tuple[Any, ...],
    issue_factory: Any,
) -> None:
    for event in events:
        if event.reason in _BAD_EVENT_REASONS or event.type == "Warning":
            blockers.append(
                issue_factory(
                    code="airflow_pod_event_blocker",
                    message=f"Kubernetes reported pod event {event.reason}: {event.message}",
                    path=f"observed_pod.events.{event.reason}",
                )
            )


def _append_check(
    checks: list[GitOpsAirflowPodLaunchEvidenceCheck],
    blockers: list[GitOpsIssue],
    *,
    name: str,
    passed: bool,
    path: str,
    message: str,
    code: str,
    issue_factory: Any,
) -> None:
    checks.append(
        GitOpsAirflowPodLaunchEvidenceCheck(
            name=name,
            passed=passed,
            severity="blocker",
            message=message,
            path=path,
        )
    )
    if not passed:
        blockers.append(issue_factory(code=code, message=message, path=path))
