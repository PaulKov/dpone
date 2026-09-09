from __future__ import annotations

from pathlib import Path

from dpone.gitops.models import GitOpsIssue
from dpone.gitops.paths import GitOpsPathValidationError, safe_relative_path


class GitOpsAirflowPodContractPaths:
    def __init__(
        self,
        *,
        bundle_path: Path,
        bundle_label: str,
        run_spec_path: Path,
        run_spec_label: str,
        runtime_profile_path: Path,
        runtime_profile_label: str,
        output_path: Path,
        output_label: str,
        pod_spec_path: Path,
        pod_spec_label: str,
        kpo_kwargs_path: Path,
        kpo_kwargs_label: str,
    ) -> None:
        self.bundle_path = bundle_path
        self.bundle_label = bundle_label
        self.run_spec_path = run_spec_path
        self.run_spec_label = run_spec_label
        self.runtime_profile_path = runtime_profile_path
        self.runtime_profile_label = runtime_profile_label
        self.output_path = output_path
        self.output_label = output_label
        self.pod_spec_path = pod_spec_path
        self.pod_spec_label = pod_spec_label
        self.kpo_kwargs_path = kpo_kwargs_path
        self.kpo_kwargs_label = kpo_kwargs_label


def resolve_pod_contract_paths(args: object) -> tuple[GitOpsAirflowPodContractPaths, tuple[GitOpsIssue, ...]]:
    bundle_path, bundle_label, bundle_issue = _safe_path(getattr(args, "bundle_path", None), source="BUNDLE")
    run_spec_path, run_spec_label, run_spec_issue = _safe_path(
        getattr(args, "run_spec_path", ".dpone/gitops/airflow/run-spec.json"),
        source="--run-spec-path",
    )
    runtime_profile_path, runtime_profile_label, profile_issue = _safe_path(
        getattr(args, "runtime_profile_path", ".dpone/gitops/airflow/runtime-profile.json"),
        source="--runtime-profile-path",
    )
    output_path, output_label, output_issue = _safe_path(
        getattr(args, "output_path", ".dpone/gitops/airflow/pod-contract.json"),
        source="--output-path",
    )
    pod_spec_path, pod_spec_label, pod_issue = _safe_path(
        getattr(args, "pod_spec_path", ".dpone/gitops/airflow/pod-spec.yaml"),
        source="--pod-spec-path",
    )
    kpo_kwargs_path, kpo_kwargs_label, kpo_issue = _safe_path(
        getattr(args, "kpo_kwargs_path", ".dpone/gitops/airflow/kpo-kwargs.json"),
        source="--kpo-kwargs-path",
    )
    paths = GitOpsAirflowPodContractPaths(
        bundle_path=bundle_path,
        bundle_label=bundle_label,
        run_spec_path=run_spec_path,
        run_spec_label=run_spec_label,
        runtime_profile_path=runtime_profile_path,
        runtime_profile_label=runtime_profile_label,
        output_path=output_path,
        output_label=output_label,
        pod_spec_path=pod_spec_path,
        pod_spec_label=pod_spec_label,
        kpo_kwargs_path=kpo_kwargs_path,
        kpo_kwargs_label=kpo_kwargs_label,
    )
    issues = (bundle_issue, run_spec_issue, profile_issue, output_issue, pod_issue, kpo_issue)
    return paths, tuple(issue for issue in issues if issue)


def _safe_path(raw_path: object, *, source: str) -> tuple[Path, str, GitOpsIssue | None]:
    try:
        path = safe_relative_path(raw_path, source=source)
    except GitOpsPathValidationError as exc:
        label = str(raw_path or "")
        return Path("."), label, GitOpsIssue(code="invalid_path", message=str(exc), path=label, source=source)
    return path, "." if path.as_posix() == "." else path.as_posix(), None


__all__ = [
    "GitOpsAirflowPodContractPaths",
    "resolve_pod_contract_paths",
]
