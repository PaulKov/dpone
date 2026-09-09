from __future__ import annotations

from pathlib import Path
from typing import Protocol

from dpone.manifest.sparse_paths_planner import ManifestSparsePathPlanner
from dpone.ports.filesystem import FileSystem
from dpone.ports.yaml_codec import YamlCodec
from dpone.services.manifest.views.common import build_meta
from dpone.services.manifest.views.sparse_paths import ManifestSparsePathsView


class _ManifestSparsePathsSettings(Protocol):
    repo_root: Path


class ManifestSparsePathsContext(Protocol):
    settings: _ManifestSparsePathsSettings
    fs: FileSystem
    yaml: YamlCodec


class _SparsePathReportLike(Protocol):
    @property
    def workload_root(self) -> str: ...


class _YamlSparsePathReader:
    def __init__(self, *, fs: FileSystem, yaml: YamlCodec) -> None:
        self._fs = fs
        self._yaml = yaml

    def read(self, path: Path) -> object:
        return self._yaml.load(self._fs.read_text(path, encoding="utf-8")) or {}


class ManifestSparsePathsService:
    """Application service for Git sparse-checkout allowlist planning."""

    def __init__(self, *, ctx: ManifestSparsePathsContext) -> None:
        self._ctx = ctx

    def build_view(self, args: object) -> ManifestSparsePathsView:
        repo_root = self._ctx.settings.repo_root.resolve(strict=False)
        raw_manifest = str(getattr(args, "path", "") or "")
        planner = ManifestSparsePathPlanner.from_inputs(
            reader=_YamlSparsePathReader(fs=self._ctx.fs, yaml=self._ctx.yaml),
            repo_root=repo_root,
            raw_manifest=raw_manifest,
            raw_workload_root=getattr(args, "workload_root", None),
        )
        report = planner.build_report(
            raw_manifest=raw_manifest,
            include_global_overrides=bool(getattr(args, "include_global_overrides", False)),
            include_env_overrides=tuple(getattr(args, "include_env_overrides", []) or ()),
            include_registry=bool(getattr(args, "include_registry", False)),
            registry_paths=tuple(getattr(args, "registry", []) or ()),
            support_paths=tuple(getattr(args, "support_path", []) or ()),
        )
        return self._view(args, repo_root=repo_root, report=report)

    def _view(self, args: object, *, repo_root: Path, report: _SparsePathReportLike) -> ManifestSparsePathsView:
        return ManifestSparsePathsView(
            meta=build_meta(
                "manifest.sparse_paths",
                path=getattr(args, "path", None),
                registry_paths=tuple(str(p) for p in (getattr(args, "registry", []) or ())),
                options={
                    "format": getattr(args, "format", "sparse"),
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


__all__ = ["ManifestSparsePathsContext", "ManifestSparsePathsService"]
