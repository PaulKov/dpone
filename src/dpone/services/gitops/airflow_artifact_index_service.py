from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from importlib import import_module
from pathlib import Path
from typing import Any, Protocol

from dpone.ports.filesystem import FileSystem
from dpone.ports.yaml_codec import YamlCodec
from dpone.services.gitops.views import GitOpsView, build_gitops_meta


class _GitOpsSettings(Protocol):
    repo_root: Path


class GitOpsAirflowArtifactIndexContext(Protocol):
    settings: _GitOpsSettings
    fs: FileSystem
    yaml: YamlCodec


@dataclass(frozen=True, slots=True)
class AirflowArtifactIndexBuildResult:
    report: Any
    path_blockers: tuple[Any, ...] = ()


class GitOpsAirflowArtifactIndexService:
    """Build and persist the Airflow artifact inventory."""

    def __init__(self, *, ctx: GitOpsAirflowArtifactIndexContext) -> None:
        self._ctx = ctx

    def build_view(self, args: object) -> GitOpsView:
        result = build_airflow_artifact_index(ctx=self._ctx, args=args)
        report = result.report
        if not result.path_blockers:
            repo_root = self._ctx.settings.repo_root.resolve(strict=False)
            output_path = _safe_relative_path(report.output_path, source="--output-path")
            self._ctx.fs.write_text(repo_root / output_path, report.to_json())
        return GitOpsView(
            meta=build_gitops_meta(
                "gitops.airflow_artifact_index",
                path=report.output_path,
                options={
                    "artifact_dir": report.artifact_dir,
                    "format": getattr(args, "format", "json"),
                },
            ),
            report=report,
        )


def build_airflow_artifact_index(
    *,
    ctx: GitOpsAirflowArtifactIndexContext,
    args: object,
) -> AirflowArtifactIndexBuildResult:
    domain = _artifact_index_domain()
    repo_root = ctx.settings.repo_root.resolve(strict=False)
    artifact_dir, artifact_dir_label, dir_issue = _safe_path(
        getattr(args, "artifact_dir", ".dpone/gitops/airflow") or ".dpone/gitops/airflow",
        source="--artifact-dir",
    )
    output_path, output_label, output_issue = _safe_path(
        getattr(args, "output_path", None) or artifact_dir / "artifact-index.json",
        source="--output-path",
    )
    path_blockers = tuple(issue for issue in (dir_issue, output_issue) if issue is not None)
    artifacts = tuple(
        domain.GitOpsAirflowArtifactContent(
            spec=spec,
            content=_read_optional(ctx=ctx, repo_root=repo_root, path=artifact_dir / spec.filename),
        )
        for spec in domain.AIRFLOW_ARTIFACT_SPECS
    )
    report = domain.GitOpsAirflowArtifactIndexBuilder().build(
        artifact_dir=artifact_dir_label,
        output_path=output_label,
        created_at=_now(),
        artifacts=artifacts,
        yaml_loader=ctx.yaml.load,
    )
    if path_blockers:
        report = domain.GitOpsAirflowArtifactIndexReport(
            artifact_dir=report.artifact_dir,
            output_path=report.output_path,
            created_at=report.created_at,
            entries=report.entries,
            warnings=report.warnings,
            blockers=(*path_blockers, *report.blockers),
        )
    return AirflowArtifactIndexBuildResult(report=report, path_blockers=path_blockers)


def _read_optional(
    *,
    ctx: GitOpsAirflowArtifactIndexContext,
    repo_root: Path,
    path: Path,
) -> str | None:
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


def _artifact_index_domain() -> Any:
    return import_module("dpone.gitops.airflow_artifact_index")


def _paths_domain() -> Any:
    return import_module("dpone.gitops.paths")


def _models_domain() -> Any:
    return import_module("dpone.gitops.models")


def _now() -> str:
    return datetime.now(tz=timezone.utc).isoformat().replace("+00:00", "Z")  # noqa: UP017


__all__ = [
    "AirflowArtifactIndexBuildResult",
    "GitOpsAirflowArtifactIndexContext",
    "GitOpsAirflowArtifactIndexService",
    "build_airflow_artifact_index",
]
