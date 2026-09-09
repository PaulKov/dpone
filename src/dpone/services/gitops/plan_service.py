from __future__ import annotations

from pathlib import Path
from typing import Protocol

from dpone.gitops.lock import GitOpsLockBuilder
from dpone.gitops.plan import GitOpsPlanBuilder
from dpone.manifest.sparse_paths_discovery import ManifestSparsePathReader
from dpone.manifest.sparse_paths_models import SparsePathReport
from dpone.manifest.sparse_paths_planner import ManifestSparsePathPlanner
from dpone.ports.filesystem import FileSystem
from dpone.ports.yaml_codec import YamlCodec
from dpone.services.gitops.views import GitOpsView, build_gitops_meta


class _GitOpsSettings(Protocol):
    repo_root: Path


class GitOpsPlanContext(Protocol):
    settings: _GitOpsSettings
    fs: FileSystem
    yaml: YamlCodec


class _YamlSparsePathReader:
    def __init__(self, *, fs: FileSystem, yaml: YamlCodec) -> None:
        self._fs = fs
        self._yaml = yaml

    def read(self, path: Path) -> object:
        return self._yaml.load(self._fs.read_text(path, encoding="utf-8")) or {}


class GitOpsPlanService:
    """Builds scheduler-neutral GitOps runner plans."""

    def __init__(self, *, ctx: GitOpsPlanContext) -> None:
        self._ctx = ctx

    def build_view(self, args: object) -> GitOpsView:
        repo_root = self._ctx.settings.repo_root.resolve(strict=False)
        raw_manifest = str(getattr(args, "path", "") or "")
        sparse_report = _build_sparse_report(
            args=args,
            reader=_YamlSparsePathReader(fs=self._ctx.fs, yaml=self._ctx.yaml),
            repo_root=repo_root,
            raw_manifest=raw_manifest,
        )
        report = GitOpsPlanBuilder().build(
            sparse_report=sparse_report,
            runner=str(getattr(args, "runner", "generic") or "generic"),
            run_command=getattr(args, "run_command", None),
            lock=GitOpsLockBuilder().build(repo_root=repo_root, entries=sparse_report.entries),
        )
        return GitOpsView(
            meta=build_gitops_meta(
                "gitops.plan",
                path=raw_manifest,
                options={
                    "format": getattr(args, "format", "json"),
                    "runner": getattr(args, "runner", "generic"),
                    "workload_root": report.workload_root,
                    "include_global_overrides": bool(getattr(args, "include_global_overrides", False)),
                    "include_env_overrides": tuple(getattr(args, "include_env_overrides", []) or ()),
                    "include_registry": bool(getattr(args, "include_registry", False)),
                    "support_path": tuple(getattr(args, "support_path", []) or ()),
                    "repo_root": repo_root.relative_to(repo_root).as_posix(),
                },
            ),
            report=report,
        )


def _build_sparse_report(
    *,
    args: object,
    reader: ManifestSparsePathReader,
    repo_root: Path,
    raw_manifest: str,
) -> SparsePathReport:
    planner = ManifestSparsePathPlanner.from_inputs(
        reader=reader,
        repo_root=repo_root,
        raw_manifest=raw_manifest,
        raw_workload_root=getattr(args, "workload_root", None),
    )
    return planner.build_report(
        raw_manifest=raw_manifest,
        include_global_overrides=bool(getattr(args, "include_global_overrides", False)),
        include_env_overrides=tuple(getattr(args, "include_env_overrides", []) or ()),
        include_registry=bool(getattr(args, "include_registry", False)),
        registry_paths=tuple(getattr(args, "registry", []) or ()),
        support_paths=tuple(getattr(args, "support_path", []) or ()),
    )


__all__ = ["GitOpsPlanContext", "GitOpsPlanService"]
