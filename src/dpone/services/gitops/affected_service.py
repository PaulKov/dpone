from __future__ import annotations

from pathlib import Path
from typing import Protocol

from dpone.gitops.affected import GitOpsImpactAnalyzer, with_emitted_plans
from dpone.gitops.changed_files import GitChangedFilesResolver
from dpone.gitops.lock import GitOpsLockBuilder
from dpone.gitops.models import GitOpsAffectedReport, GitOpsIssue
from dpone.gitops.paths import GitOpsPathValidationError, safe_relative_path
from dpone.gitops.plan import GitOpsPlanBuilder
from dpone.manifest.sparse_paths_discovery import ManifestSparsePathReader
from dpone.manifest.sparse_paths_models import SparsePathReport
from dpone.manifest.sparse_paths_planner import ManifestSparsePathPlanner
from dpone.ports.filesystem import FileSystem
from dpone.ports.yaml_codec import YamlCodec
from dpone.services.gitops.views import GitOpsView, build_gitops_meta


class _GitOpsSettings(Protocol):
    repo_root: Path


class GitOpsAffectedContext(Protocol):
    settings: _GitOpsSettings
    fs: FileSystem
    yaml: YamlCodec


class _YamlSparsePathReader:
    def __init__(self, *, fs: FileSystem, yaml: YamlCodec) -> None:
        self._fs = fs
        self._yaml = yaml

    def read(self, path: Path) -> object:
        return self._yaml.load(self._fs.read_text(path, encoding="utf-8")) or {}


class GitOpsAffectedService:
    """Builds GitOps impact reports from changed repo-relative files."""

    def __init__(self, *, ctx: GitOpsAffectedContext) -> None:
        self._ctx = ctx

    def build_view(self, args: object) -> GitOpsView:
        repo_root = self._ctx.settings.repo_root.resolve(strict=False)
        workload_root, workload_label, root_blocker = _resolve_workload_root(
            repo_root=repo_root,
            raw_workload_root=getattr(args, "workload_root", None),
        )
        if root_blocker is not None:
            report = GitOpsAffectedReport(
                changed_files=tuple(str(item) for item in (getattr(args, "changed_files", []) or ())),
                impacted_manifests=(),
                blockers=(root_blocker,),
            )
        else:
            report = self._build_report(
                args=args,
                repo_root=repo_root,
                workload_root=workload_root,
                workload_label=workload_label,
            )
        return GitOpsView(
            meta=build_gitops_meta(
                "gitops.affected",
                options={
                    "format": getattr(args, "format", "json"),
                    "workload_root": workload_label,
                    "manifest_glob": getattr(args, "manifest_glob", "manifests/**/*.yaml"),
                    "changed_files_file": getattr(args, "changed_files_file", None),
                    "from_ref": getattr(args, "from_ref", None),
                    "to_ref": getattr(args, "to_ref", None),
                    "runner": getattr(args, "runner", "generic"),
                    "emit_plans": bool(getattr(args, "emit_plans", False)),
                    "repo_root": repo_root.relative_to(repo_root).as_posix(),
                },
            ),
            report=report,
        )

    def _build_report(
        self,
        *,
        args: object,
        repo_root: Path,
        workload_root: Path,
        workload_label: str,
    ) -> GitOpsAffectedReport:
        changed_files, changed_warnings, changed_blockers = GitChangedFilesResolver(fs=self._ctx.fs).resolve(
            args=args,
            repo_root=repo_root,
        )
        if changed_blockers:
            return GitOpsAffectedReport(
                changed_files=changed_files,
                impacted_manifests=(),
                warnings=changed_warnings,
                blockers=changed_blockers,
            )
        manifest_paths, warnings, blockers = _discover_manifest_paths(
            fs=self._ctx.fs,
            workload_root=workload_root,
            raw_manifest_glob=getattr(args, "manifest_glob", "manifests/**/*.yaml"),
        )
        if blockers:
            return GitOpsAffectedReport(
                changed_files=changed_files,
                impacted_manifests=(),
                warnings=(*changed_warnings, *tuple(warnings)),
                blockers=tuple(blockers),
            )
        sparse_reports = tuple(
            _build_sparse_report(
                args=args,
                reader=_YamlSparsePathReader(fs=self._ctx.fs, yaml=self._ctx.yaml),
                repo_root=repo_root,
                workload_label=workload_label,
                manifest_path=path,
            )
            for path in manifest_paths
        )
        entrypoint_reports = _entrypoint_sparse_reports(sparse_reports)
        report = GitOpsImpactAnalyzer().analyze(
            changed_files=changed_files,
            sparse_reports=entrypoint_reports,
            runner=str(getattr(args, "runner", "generic") or "generic"),
        )
        if changed_warnings or warnings:
            report = GitOpsAffectedReport(
                changed_files=report.changed_files,
                impacted_manifests=report.impacted_manifests,
                warnings=(*changed_warnings, *tuple(warnings), *report.warnings),
                blockers=report.blockers,
            )
        if bool(getattr(args, "emit_plans", False)) and report.passed:
            emitted = self._emit_plans(args=args, repo_root=repo_root, sparse_reports=entrypoint_reports, report=report)
            report = with_emitted_plans(report, emitted)
        return report

    def _emit_plans(
        self,
        *,
        args: object,
        repo_root: Path,
        sparse_reports: tuple[SparsePathReport, ...],
        report: GitOpsAffectedReport,
    ) -> dict[str, str]:
        output_dir = safe_relative_path(getattr(args, "output_dir", ".dpone/gitops/affected"), source="--output-dir")
        reports_by_manifest = {item.manifest: item for item in sparse_reports}
        emitted: dict[str, str] = {}
        for impacted in report.impacted_manifests:
            sparse_report = reports_by_manifest[impacted.manifest]
            plan_report = GitOpsPlanBuilder().build(
                sparse_report=sparse_report,
                runner=str(getattr(args, "runner", "generic") or "generic"),
                run_command=None,
                lock=GitOpsLockBuilder().build(repo_root=repo_root, entries=sparse_report.entries),
            )
            rel_output = output_dir / _manifest_slug(impacted.manifest) / "gitops_plan.json"
            self._ctx.fs.write_text(repo_root / rel_output, plan_report.to_json(), encoding="utf-8")
            emitted[impacted.manifest] = rel_output.as_posix()
        return emitted


def _resolve_workload_root(
    *,
    repo_root: Path,
    raw_workload_root: object,
) -> tuple[Path, str, GitOpsIssue | None]:
    if raw_workload_root:
        try:
            rel_path = safe_relative_path(raw_workload_root, source="--workload-root")
        except GitOpsPathValidationError as exc:
            return (
                repo_root,
                str(raw_workload_root),
                GitOpsIssue(
                    code="invalid_path", message=str(exc), path=str(raw_workload_root), source="--workload-root"
                ),
            )
        label = "." if rel_path.as_posix() == "." else rel_path.as_posix()
        return (repo_root / rel_path).resolve(strict=False), label, None
    default = repo_root / "dpone_workloads"
    if default.exists():
        return default.resolve(strict=False), "dpone_workloads", None
    return repo_root, ".", None


def _discover_manifest_paths(
    *,
    fs: FileSystem,
    workload_root: Path,
    raw_manifest_glob: object,
) -> tuple[tuple[Path, ...], list[GitOpsIssue], list[GitOpsIssue]]:
    try:
        manifest_glob = safe_relative_path(raw_manifest_glob, source="--manifest-glob").as_posix()
    except GitOpsPathValidationError as exc:
        return (
            (),
            [],
            [GitOpsIssue(code="invalid_path", message=str(exc), path=str(raw_manifest_glob), source="--manifest-glob")],
        )
    paths = tuple(sorted(path for path in fs.glob(workload_root, manifest_glob) if path.is_file()))
    warnings: list[GitOpsIssue] = []
    if not paths:
        warnings.append(
            GitOpsIssue(
                code="no_manifests",
                message="No manifests matched the manifest glob",
                path=manifest_glob,
                source="--manifest-glob",
            )
        )
    return paths, warnings, []


def _build_sparse_report(
    *,
    args: object,
    reader: ManifestSparsePathReader,
    repo_root: Path,
    workload_label: str,
    manifest_path: Path,
) -> SparsePathReport:
    raw_manifest = manifest_path.resolve(strict=False).relative_to(repo_root).as_posix()
    planner = ManifestSparsePathPlanner.from_inputs(
        reader=reader,
        repo_root=repo_root,
        raw_manifest=raw_manifest,
        raw_workload_root=workload_label,
    )
    return planner.build_report(
        raw_manifest=raw_manifest,
        include_global_overrides=bool(getattr(args, "include_global_overrides", False)),
        include_env_overrides=tuple(getattr(args, "include_env_overrides", []) or ()),
        include_registry=bool(getattr(args, "include_registry", False)),
        registry_paths=tuple(getattr(args, "registry", []) or ()),
        support_paths=tuple(getattr(args, "support_path", []) or ()),
    )


def _entrypoint_sparse_reports(sparse_reports: tuple[SparsePathReport, ...]) -> tuple[SparsePathReport, ...]:
    dependency_paths = {
        entry.path
        for report in sparse_reports
        for entry in report.entries
        if entry.kind != "manifest" and not entry.is_dir
    }
    return tuple(report for report in sparse_reports if report.manifest not in dependency_paths)


def _manifest_slug(manifest: str) -> str:
    return manifest.replace("/", "__").replace("\\", "__").replace(":", "_")


__all__ = ["GitOpsAffectedContext", "GitOpsAffectedService"]
