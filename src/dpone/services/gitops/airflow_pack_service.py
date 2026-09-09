from __future__ import annotations

from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import Any, Protocol

from dpone.ports.filesystem import FileSystem
from dpone.services.gitops.views import GitOpsView, build_gitops_meta


class _GitOpsSettings(Protocol):
    repo_root: Path


class GitOpsAirflowPackContext(Protocol):
    settings: _GitOpsSettings
    fs: FileSystem


@dataclass(frozen=True, slots=True)
class AirflowPackBuildResult:
    report: Any
    path_blockers: tuple[Any, ...] = ()


class GitOpsAirflowPackService:
    """Build and persist the Airflow runtime pack golden-path report."""

    def __init__(self, *, ctx: GitOpsAirflowPackContext) -> None:
        self._ctx = ctx

    def build_view(self, args: object) -> GitOpsView:
        result = build_airflow_pack(ctx=self._ctx, args=args)
        report = result.report
        if not result.path_blockers:
            repo_root = self._ctx.settings.repo_root.resolve(strict=False)
            output_path = repo_root / _safe_relative_path(report.output_path, source="--output-path")
            self._ctx.fs.write_text(output_path, report.to_json())
        return GitOpsView(
            meta=build_gitops_meta(
                "gitops.airflow_pack",
                path=report.output_path,
                options={"mode": report.mode, "runner_policy": report.runner_policy},
            ),
            report=report,
        )


def build_airflow_pack(*, ctx: GitOpsAirflowPackContext, args: object) -> AirflowPackBuildResult:
    repo_root = ctx.settings.repo_root.resolve(strict=False)
    artifact_dir, artifact_dir_label, dir_issue = _safe_path(
        getattr(args, "artifact_dir", ".dpone/gitops/airflow") or ".dpone/gitops/airflow",
        source="--artifact-dir",
    )
    output_path, output_label, output_issue = _safe_path(
        getattr(args, "output_path", None) or artifact_dir / "airflow-runtime-pack.json",
        source="--output-path",
    )
    _, bundle_label, bundle_issue = _safe_path(
        getattr(args, "bundle_path", ".dpone/gitops/bundle/bundle.json") or ".dpone/gitops/bundle/bundle.json",
        source="--bundle-path",
    )
    path_blockers = tuple(issue for issue in (dir_issue, output_issue, bundle_issue) if issue is not None)
    artifact_index = _artifact_index_domain()
    artifacts = tuple(
        artifact_index.GitOpsAirflowArtifactContent(
            spec=spec,
            content=_read_optional(ctx=ctx, repo_root=repo_root, path=artifact_dir / spec.filename),
        )
        for spec in artifact_index.AIRFLOW_ARTIFACT_SPECS
    )
    report = (
        _pack_domain()
        .GitOpsAirflowPackPlanner()
        .plan(
            artifact_dir=artifact_dir_label,
            output_path=output_label,
            bundle_path=bundle_label,
            image=_optional_text(getattr(args, "image", None)),
            image_digest=_optional_text(getattr(args, "image_digest", None)),
            mode=str(getattr(args, "mode", "plan")),
            runner_policy=str(getattr(args, "runner_policy", "advisory")),
            include_live_gates=bool(getattr(args, "include_live_gates", False)),
            artifacts=artifacts,
        )
    )
    if path_blockers:
        report = _replace_blockers(report, path_blockers)
    return AirflowPackBuildResult(report=report, path_blockers=path_blockers)


def _replace_blockers(report: Any, blockers: tuple[Any, ...]) -> Any:
    report_type = type(report)
    return report_type(
        artifact_dir=report.artifact_dir,
        output_path=report.output_path,
        bundle_path=report.bundle_path,
        image=report.image,
        image_digest=report.image_digest,
        mode=report.mode,
        runner_policy=report.runner_policy,
        include_live_gates=report.include_live_gates,
        artifacts=report.artifacts,
        steps=report.steps,
        next_actions=report.next_actions,
        warnings=report.warnings,
        blockers=(*blockers, *report.blockers),
    )


def _read_optional(*, ctx: GitOpsAirflowPackContext, repo_root: Path, path: Path) -> str | None:
    full_path = repo_root / path
    return ctx.fs.read_text(full_path, encoding="utf-8") if ctx.fs.exists(full_path) else None


def _safe_path(raw_path: object, *, source: str) -> tuple[Path, str, Any | None]:
    try:
        path = _safe_relative_path(raw_path, source=source)
    except _path_validation_error() as exc:
        label = str(raw_path or "")
        return Path("."), label, _gitops_issue(code="invalid_path", message=str(exc), path=label, source=source)
    return path, "." if path.as_posix() == "." else path.as_posix(), None


def _safe_relative_path(raw_path: object, *, source: str) -> Path:
    return _paths_domain().safe_relative_path(raw_path, source=source)


def _path_validation_error() -> type[Exception]:
    return _paths_domain().GitOpsPathValidationError


def _gitops_issue(*, code: str, message: str, path: str, source: str) -> Any:
    return _models_domain().GitOpsIssue(code=code, message=message, path=path, source=source)


def _optional_text(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _artifact_index_domain() -> Any:
    return import_module("dpone.gitops.airflow_artifact_index")


def _pack_domain() -> Any:
    return import_module("dpone.gitops.airflow_pack")


def _paths_domain() -> Any:
    return import_module("dpone.gitops.paths")


def _models_domain() -> Any:
    return import_module("dpone.gitops.models")


__all__ = ["AirflowPackBuildResult", "GitOpsAirflowPackContext", "GitOpsAirflowPackService", "build_airflow_pack"]
