from __future__ import annotations

from dpone.gitops.airflow_git_sync_capabilities import evaluate_git_sync_filter_request
from dpone.gitops.airflow_runtime_profile_models import GitOpsAirflowRuntimeProfile, GitOpsAirflowRuntimeResources
from dpone.gitops.models import GitOpsIssue


class GitOpsAirflowRuntimeProfilePolicy:
    """Applies runner profile checks without CLI, filesystem, or scheduler dependencies."""

    def evaluate(self, profile: GitOpsAirflowRuntimeProfile) -> tuple[tuple[GitOpsIssue, ...], tuple[GitOpsIssue, ...]]:
        warnings, blockers = _git_sync_filter_policy(profile)
        issues = _release_readiness_issues(profile)
        if profile.runner_policy == "release":
            return warnings, (*blockers, *issues)
        return (*warnings, *issues), blockers


def _git_sync_filter_policy(
    profile: GitOpsAirflowRuntimeProfile,
) -> tuple[tuple[GitOpsIssue, ...], tuple[GitOpsIssue, ...]]:
    if profile.git_sync is None:
        return (), ()
    return evaluate_git_sync_filter_request(
        image=profile.git_sync.image,
        filter_value=profile.git_sync.clone.filter,
        runner_policy=profile.runner_policy,
        path="--git-sync-filter",
        source="dpone gitops airflow runtime-profile",
    )


def _release_readiness_issues(profile: GitOpsAirflowRuntimeProfile) -> tuple[GitOpsIssue, ...]:
    issues: list[GitOpsIssue] = []
    if not profile.image:
        issues.append(
            _issue(
                code="runtime_profile_image_required",
                message="Airflow runtime profile requires a custom dpone image",
                path="--image",
            )
        )
    if not profile.image_digest:
        issues.append(
            _issue(
                code="runtime_profile_image_digest_required",
                message="Release Airflow runtime profile requires an immutable image digest",
                path="--image-digest",
            )
        )
    if not profile.service_account or profile.service_account == "default":
        issues.append(
            _issue(
                code="runtime_profile_service_account_required",
                message="Release Airflow runtime profile requires a non-default Kubernetes service account",
                path="--service-account",
            )
        )
    if not _has_resources(profile.resources):
        issues.append(
            _issue(
                code="runtime_profile_resources_required",
                message="Release Airflow runtime profile requires cpu and memory requests and limits",
                path="--cpu-request/--memory-request/--cpu-limit/--memory-limit",
            )
        )
    if not profile.artifact_sink.path:
        issues.append(
            _issue(
                code="runtime_profile_artifact_sink_required",
                message="Release Airflow runtime profile requires an artifact sink path for run evidence handoff",
                path="--artifact-sink-path",
            )
        )
    return tuple(issues)


def _has_resources(resources: GitOpsAirflowRuntimeResources) -> bool:
    return all(
        (
            resources.requests.get("cpu"),
            resources.requests.get("memory"),
            resources.limits.get("cpu"),
            resources.limits.get("memory"),
        )
    )


def _issue(*, code: str, message: str, path: str) -> GitOpsIssue:
    return GitOpsIssue(code=code, message=message, path=path, source="dpone gitops airflow runtime-profile")


__all__ = ["GitOpsAirflowRuntimeProfilePolicy"]
