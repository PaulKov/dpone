"""Resolve workload-local file dependencies for compact runner packs."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from dpone.manifest.authoring_folder import (
    BoundedYamlFolderFragmentLoader,
    FolderAuthoringError,
    FolderFragmentLoader,
    iter_sql_file_paths,
)
from dpone.manifest.bounded_yaml import BoundedYamlError, BoundedYamlLimits, load_bounded_yaml
from dpone.manifest.confined_files import (
    ConfinedFileError,
    project_relative_path,
    read_confined_file,
    sha256_confined_file,
)
from dpone.manifest.recipe_models import RecipeContext, RecipeResolutionError, RecipeSourceResolver
from dpone.manifest.recipe_resolver import BoundedLocalRecipeSourceResolver

_MANIFEST_LIMITS = BoundedYamlLimits(max_bytes=8 * 1024 * 1024, max_tokens=100_000, max_nodes=50_000)
_KIND_ORDER = {
    "manifest": 0,
    "authoring_fragment": 1,
    "recipe": 2,
    "profile": 3,
    "component": 4,
    "sql_file": 5,
}


class WorkloadDependencyError(ValueError):
    """A declared workload dependency cannot be safely materialized."""


@dataclass(frozen=True, slots=True)
class WorkloadFileDependency:
    """A source file that must be embedded into scheduler-static workload packs."""

    kind: str
    path: str
    sha256: str

    def to_jsonable(self) -> dict[str, str]:
        return {"kind": self.kind, "path": self.path, "sha256": self.sha256}


class WorkloadDependencyResolver:
    """Find manifest-referenced files and fingerprint their contents."""

    def __init__(
        self,
        *,
        folder_loader: FolderFragmentLoader | None = None,
        recipe_resolver: RecipeSourceResolver | None = None,
    ) -> None:
        self._folder_loader = folder_loader or BoundedYamlFolderFragmentLoader()
        self._recipe_resolver = recipe_resolver or BoundedLocalRecipeSourceResolver()

    def resolve(self, *, repo_root: str | Path, manifest: str) -> tuple[WorkloadFileDependency, ...]:
        root = Path(repo_root).resolve(strict=True)
        try:
            manifest_ref = project_relative_path(root, Path(manifest))
            manifest_bytes = read_confined_file(root, manifest_ref, max_bytes=_MANIFEST_LIMITS.max_bytes)
            payload = load_bounded_yaml(manifest_bytes, limits=_MANIFEST_LIMITS) or {}
        except (BoundedYamlError, ConfinedFileError, UnicodeDecodeError) as exc:
            raise WorkloadDependencyError("manifest_read_failed") from exc
        if not isinstance(payload, Mapping):
            raise WorkloadDependencyError("manifest_invalid")
        manifest_path = root.joinpath(*Path(manifest_ref).parts)
        dependencies: list[WorkloadFileDependency] = [
            self._dependency(root=root, manifest_dir=manifest_path.parent, raw_path=raw_path)
            for raw_path in iter_sql_file_paths(payload)
        ]
        if _is_recipe_source(payload):
            recipe_block = payload.get("recipe")
            metadata = payload.get("metadata")
            if not isinstance(recipe_block, Mapping) or not isinstance(metadata, Mapping):
                raise WorkloadDependencyError("recipe_source_invalid")
            try:
                resolved = self._recipe_resolver.resolve(
                    recipe_block,
                    source_path=manifest_path,
                    project_root=root,
                    context=RecipeContext(
                        pipeline_id=str(metadata.get("id") or ""),
                        domain=str(metadata.get("domain") or ""),
                    ),
                )
            except RecipeResolutionError as exc:
                raise WorkloadDependencyError(exc.code.removeprefix("DPONE_").lower()) from exc
            dependencies.extend(
                WorkloadFileDependency(
                    kind=item.kind,
                    path=item.path,
                    sha256=item.sha256.removeprefix("sha256:"),
                )
                for item in resolved.dependencies
            )
            dependencies.extend(
                self._dependency(root=root, manifest_dir=manifest_path.parent, raw_path=raw_path)
                for process in resolved.processes
                for raw_path in iter_sql_file_paths(process)
            )
        if _is_folder_source(payload):
            if "processes" in payload:
                raise WorkloadDependencyError("folder_processes_must_be_fragments")
            authoring = payload.get("authoring")
            fragments = payload.get("fragments")
            project_source = str(authoring.get("source") or "") if isinstance(authoring, Mapping) else ""
            if not isinstance(fragments, list):
                raise WorkloadDependencyError("folder_fragments_invalid")
            if project_source != manifest_path.relative_to(root).as_posix():
                raise WorkloadDependencyError("folder_primary_source_mismatch")
            try:
                folder = self._folder_loader.load(
                    source_path=manifest_path,
                    project_source=project_source,
                    fragment_refs=fragments,
                    project_root=root,
                )
            except FolderAuthoringError as exc:
                raise WorkloadDependencyError(exc.code.removeprefix("DPONE_").lower()) from exc
            for document in folder.source_documents:
                dependencies.append(
                    WorkloadFileDependency(
                        kind=document.dependency.kind,
                        path=document.dependency.path,
                        sha256=document.dependency.sha256.removeprefix("sha256:"),
                    )
                )
                dependencies.extend(
                    self._dependency(root=root, manifest_dir=document.path.parent, raw_path=raw_path)
                    for raw_path in iter_sql_file_paths(document.payload)
                )
        dependencies.append(
            WorkloadFileDependency(
                kind="manifest",
                path=manifest_path.relative_to(root).as_posix(),
                sha256=hashlib.sha256(manifest_bytes).hexdigest(),
            )
        )
        return tuple(
            sorted(
                set(dependencies),
                key=lambda item: (_KIND_ORDER.get(item.kind, len(_KIND_ORDER)), item.kind, item.path, item.sha256),
            )
        )

    @staticmethod
    def _dependency(*, root: Path, manifest_dir: Path, raw_path: str) -> WorkloadFileDependency:
        path = (manifest_dir / raw_path).resolve()
        if not _is_relative_to(path, root):
            raise WorkloadDependencyError("sql_file_outside_repo")
        relative = path.relative_to(root).as_posix()
        try:
            sha256 = sha256_confined_file(root, relative, follow_in_root_symlinks=True).removeprefix("sha256:")
        except ConfinedFileError as exc:
            raise WorkloadDependencyError("sql_file_read_failed") from exc
        return WorkloadFileDependency(kind="sql_file", path=relative, sha256=sha256)


def _is_folder_source(payload: object) -> bool:
    if not isinstance(payload, Mapping):
        return False
    authoring = payload.get("authoring")
    return isinstance(authoring, Mapping) and authoring.get("mode") == "folder"


def _is_recipe_source(payload: object) -> bool:
    if not isinstance(payload, Mapping):
        return False
    authoring = payload.get("authoring")
    return (
        isinstance(authoring, Mapping)
        and authoring.get("mode") == "flow"
        and isinstance(payload.get("recipe"), Mapping)
    )


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


__all__ = ["WorkloadDependencyError", "WorkloadDependencyResolver", "WorkloadFileDependency"]
