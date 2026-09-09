from __future__ import annotations

from importlib import import_module
from pathlib import Path
from typing import Any, Protocol

from dpone.ports.filesystem import FileSystem
from dpone.services.gitops.views import GitOpsView, build_gitops_meta

_SOURCE = "dpone gitops airflow admission-check"


class _GitOpsSettings(Protocol):
    repo_root: Path


class GitOpsAirflowAdmissionCheckContext(Protocol):
    settings: _GitOpsSettings
    fs: FileSystem


class GitOpsAirflowAdmissionCheckService:
    """Build and optionally execute Kubernetes admission dry-run checks."""

    def __init__(
        self,
        *,
        ctx: GitOpsAirflowAdmissionCheckContext,
        runner: Any | None = None,
    ) -> None:
        self._ctx = ctx
        self._runner = runner or _runner_domain().SubprocessAirflowAdmissionCheckRunner()
        self._planner = _planner_domain().GitOpsAirflowAdmissionPlanner()

    def build_view(self, args: object) -> GitOpsView:
        repo_root = self._ctx.settings.repo_root.resolve(strict=False)
        paths, path_blockers = _resolve_paths(args)
        file_blockers = _file_blockers(self._ctx, repo_root, paths)
        blockers = (*path_blockers, *file_blockers)
        report = self._planner.plan(
            mode=str(getattr(args, "mode", "plan")),
            runner_policy=str(getattr(args, "runner_policy", "advisory")),
            artifact_dir=paths.artifact_dir_label,
            manifest_path=paths.manifest_label,
            pod_spec_path=paths.pod_spec_label,
            timeout_seconds=int(getattr(args, "timeout_seconds", 120)),
            kubectl=str(getattr(args, "kubectl", "kubectl")),
        )
        if blockers:
            report = report.with_results(commands=report.commands, results=(), blockers=blockers)
            return _view(report)
        if getattr(args, "mode", "plan") == "live":
            results = tuple(self._runner.run(command=command, cwd=repo_root) for command in report.commands)
            report = self._planner.with_results(report, results=results)
        return _view(report)


class _AdmissionPaths:
    def __init__(
        self,
        *,
        artifact_dir_path: Path,
        artifact_dir_label: str,
        manifest_path: Path,
        manifest_label: str,
        pod_spec_path: Path,
        pod_spec_label: str,
    ) -> None:
        self.artifact_dir_path = artifact_dir_path
        self.artifact_dir_label = artifact_dir_label
        self.manifest_path = manifest_path
        self.manifest_label = manifest_label
        self.pod_spec_path = pod_spec_path
        self.pod_spec_label = pod_spec_label


def _resolve_paths(args: object) -> tuple[_AdmissionPaths, tuple[Any, ...]]:
    artifact_dir, artifact_label, artifact_issue = _safe_path(
        getattr(args, "artifact_dir", ".dpone/gitops/airflow") or ".dpone/gitops/airflow",
        "--artifact-dir",
    )
    manifest_path, manifest_label, manifest_issue = _safe_path(
        getattr(args, "manifest_path", ".dpone/gitops/airflow/airflow-k8s-manifests.yaml"),
        "--manifest-path",
    )
    pod_spec_path, pod_spec_label, pod_issue = _safe_path(
        getattr(args, "pod_spec_path", ".dpone/gitops/airflow/pod-spec.yaml"),
        "--pod-spec-path",
    )
    return (
        _AdmissionPaths(
            artifact_dir_path=artifact_dir,
            artifact_dir_label=artifact_label,
            manifest_path=manifest_path,
            manifest_label=manifest_label,
            pod_spec_path=pod_spec_path,
            pod_spec_label=pod_spec_label,
        ),
        tuple(issue for issue in (artifact_issue, manifest_issue, pod_issue) if issue),
    )


def _file_blockers(
    ctx: GitOpsAirflowAdmissionCheckContext,
    repo_root: Path,
    paths: _AdmissionPaths,
) -> tuple[Any, ...]:
    issues: list[Any] = []
    if not ctx.fs.exists(repo_root / paths.manifest_path):
        issues.append(
            _issue("airflow_admission_manifest_missing", "Kubernetes manifest YAML is missing", paths.manifest_label)
        )
    if not ctx.fs.exists(repo_root / paths.pod_spec_path):
        issues.append(_issue("airflow_admission_pod_spec_missing", "Pod spec YAML is missing", paths.pod_spec_label))
    return tuple(issues)


def _view(report: Any) -> GitOpsView:
    return GitOpsView(
        meta=build_gitops_meta(
            "gitops.airflow_admission_check",
            path=report.manifest_path,
            options={"mode": report.mode},
        ),
        report=report,
    )


def _safe_path(raw_path: object, source: str) -> tuple[Path, str, Any | None]:
    paths = _paths_domain()
    try:
        path = paths.safe_relative_path(raw_path, source=source)
    except paths.GitOpsPathValidationError as exc:
        label = str(raw_path or "")
        return Path("."), label, _issue("invalid_path", str(exc), label)
    return path, "." if path.as_posix() == "." else path.as_posix(), None


def _issue(code: str, message: str, path: str) -> Any:
    return _gitops_models().GitOpsIssue(code=code, message=message, path=path, source=_SOURCE)


def _planner_domain() -> Any:
    return import_module("dpone.gitops.airflow_admission_check")


def _runner_domain() -> Any:
    return import_module("dpone.gitops.airflow_admission_check_runner")


def _paths_domain() -> Any:
    return import_module("dpone.gitops.paths")


def _gitops_models() -> Any:
    return import_module("dpone.gitops.models")


__all__ = ["GitOpsAirflowAdmissionCheckContext", "GitOpsAirflowAdmissionCheckService"]
