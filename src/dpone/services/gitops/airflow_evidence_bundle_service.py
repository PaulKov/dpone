from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from dpone.gitops.airflow_evidence_bundle import (
    GitOpsAirflowEvidenceArtifactInput,
    GitOpsAirflowEvidenceBundleCollector,
)
from dpone.gitops.airflow_evidence_bundle_models import (
    AIRFLOW_EVIDENCE_BUNDLE_SOURCE,
    GitOpsAirflowEvidenceBundleReport,
    GitOpsIssue,
)
from dpone.gitops.paths import GitOpsPathValidationError, safe_relative_path
from dpone.ports.filesystem import FileSystem
from dpone.services.gitops.views import GitOpsView, build_gitops_meta


class _GitOpsSettings(Protocol):
    repo_root: Path


class GitOpsAirflowEvidenceBundleContext(Protocol):
    settings: _GitOpsSettings
    fs: FileSystem


@dataclass(frozen=True, slots=True)
class _ArtifactSpec:
    name: str
    arg_name: str
    expected_kind: str
    required: bool
    require_flag: str | None = None


_ARTIFACT_SPECS = (
    _ArtifactSpec("bundle", "bundle_path", "gitops.bundle", True),
    _ArtifactSpec("run_spec", "run_spec_path", "gitops.airflow_run_spec", True),
    _ArtifactSpec("runtime_profile", "runtime_profile_path", "gitops.airflow_runtime_profile", True),
    _ArtifactSpec("pod_contract", "pod_contract_path", "gitops.airflow_pod_contract", True),
    _ArtifactSpec("runtime_evidence", "runtime_evidence_path", "gitops.airflow_runtime_evidence", True),
    _ArtifactSpec("xcom_summary", "xcom_summary_path", "gitops.airflow_xcom_summary", True),
    _ArtifactSpec(
        "k8s_smoke",
        "k8s_smoke_path",
        "gitops.airflow_k8s_smoke",
        False,
        require_flag="require_k8s_smoke",
    ),
    _ArtifactSpec(
        "pod_launch_evidence",
        "pod_launch_evidence_path",
        "gitops.airflow_pod_launch_evidence",
        False,
        require_flag="require_pod_launch_evidence",
    ),
)


class GitOpsAirflowEvidenceBundleService:
    """Collects one Airflow attempt evidence bundle from repo-relative artifacts."""

    def __init__(self, *, ctx: GitOpsAirflowEvidenceBundleContext) -> None:
        self._ctx = ctx
        self._collector = GitOpsAirflowEvidenceBundleCollector()

    def build_view(self, args: object) -> GitOpsView:
        repo_root = self._ctx.settings.repo_root.resolve(strict=False)
        artifacts: list[GitOpsAirflowEvidenceArtifactInput] = []
        blockers: list[GitOpsIssue] = []
        for spec in _ARTIFACT_SPECS:
            resolved, issue = _artifact_input(ctx=self._ctx, repo_root=repo_root, args=args, spec=spec)
            if resolved is not None:
                artifacts.append(resolved)
            if issue is not None:
                blockers.append(issue)

        report = self._collector.collect(
            dag_id=str(getattr(args, "dag_id", "")),
            task_id=str(getattr(args, "task_id", "")),
            run_id=str(getattr(args, "run_id", "")),
            try_number=int(getattr(args, "try_number", 1)),
            map_index=int(getattr(args, "map_index", -1)),
            runner_policy=str(getattr(args, "runner_policy", "advisory")),
            artifacts=tuple(artifacts),
            pod_name=_optional_string(getattr(args, "pod_name", None)),
            pod_uid=_optional_string(getattr(args, "pod_uid", None)),
            blockers=tuple(blockers),
        )
        return _view(args=args, report=report)


def _artifact_input(
    *,
    ctx: GitOpsAirflowEvidenceBundleContext,
    repo_root: Path,
    args: object,
    spec: _ArtifactSpec,
) -> tuple[GitOpsAirflowEvidenceArtifactInput | None, GitOpsIssue | None]:
    raw_path = getattr(args, spec.arg_name, None)
    required = spec.required or bool(getattr(args, spec.require_flag, False)) if spec.require_flag else spec.required
    if raw_path in (None, ""):
        if required:
            return None, _issue(
                code="airflow_evidence_artifact_path_required",
                message="Required evidence artifact path was not provided",
                path=spec.arg_name,
            )
        return None, None
    try:
        rel_path = safe_relative_path(raw_path, source=f"--{spec.arg_name.replace('_', '-')}")
    except GitOpsPathValidationError as exc:
        return None, _issue(code="invalid_path", message=str(exc), path=str(raw_path))
    full_path = repo_root / rel_path
    content = ctx.fs.read_text(full_path, encoding="utf-8") if ctx.fs.exists(full_path) else None
    return (
        GitOpsAirflowEvidenceArtifactInput(
            name=spec.name,
            path=rel_path.as_posix(),
            expected_kind=spec.expected_kind,
            required=required,
            content=content,
        ),
        None,
    )


def _view(*, args: object, report: GitOpsAirflowEvidenceBundleReport) -> GitOpsView:
    return GitOpsView(
        meta=build_gitops_meta(
            "gitops.airflow_evidence_bundle",
            path=report.artifacts[0].path if report.artifacts else None,
            options={
                "format": getattr(args, "format", "json"),
                "runner_policy": report.runner_policy,
                "require_k8s_smoke": bool(getattr(args, "require_k8s_smoke", False)),
                "require_pod_launch_evidence": bool(getattr(args, "require_pod_launch_evidence", False)),
            },
        ),
        report=report,
    )


def _optional_string(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _issue(*, code: str, message: str, path: str) -> GitOpsIssue:
    return GitOpsIssue(code=code, message=message, path=path, source=AIRFLOW_EVIDENCE_BUNDLE_SOURCE)


__all__ = ["GitOpsAirflowEvidenceBundleContext", "GitOpsAirflowEvidenceBundleService"]
