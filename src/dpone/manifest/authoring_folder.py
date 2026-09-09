"""Bounded filesystem adapter for explicit folder-authoring fragments."""

from __future__ import annotations

import copy
import hashlib
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Protocol

from dpone.contracts.project_discovery import MAX_PROJECT_DISCOVERY_BYTES
from dpone.manifest.bounded_yaml import BoundedYamlError, BoundedYamlLimits, load_bounded_yaml
from dpone.manifest.confined_files import (
    ConfinedFileError,
    project_relative_path,
    read_confined_file,
    sha256_confined_file,
)

MAX_FRAGMENT_COUNT = 100
MAX_FRAGMENT_BYTES = 1024 * 1024
MAX_TOTAL_BYTES = 8 * 1024 * 1024
MAX_YAML_TOKENS = 20_000
MAX_YAML_DEPTH = 32
MAX_YAML_NODES = 10_000
MAX_SQL_DEPENDENCY_BYTES = MAX_PROJECT_DISCOVERY_BYTES


class FolderAuthoringError(ValueError):
    """Stable failure raised by a folder-authoring source adapter."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True, slots=True)
class AuthoringSourceDependency:
    """One exact editable source consumed by authoring compilation."""

    kind: str
    path: str
    sha256: str

    def to_jsonable(self) -> dict[str, str]:
        return {"kind": self.kind, "path": self.path, "sha256": self.sha256}


@dataclass(frozen=True, slots=True)
class FolderFragmentDocument:
    """One validated fragment and its project-relative identity."""

    path: Path
    payload: Mapping[str, Any]
    dependency: AuthoringSourceDependency


@dataclass(frozen=True, slots=True)
class FolderLoadResult:
    """Ordered fragment content returned to the build-plane compiler."""

    processes: tuple[Mapping[str, Any], ...]
    dependencies: tuple[AuthoringSourceDependency, ...]
    source_documents: tuple[FolderFragmentDocument, ...]


class FolderFragmentLoader(Protocol):
    """Port used by the canonical authoring compiler for folder sources."""

    def load(
        self,
        *,
        source_path: Path,
        project_source: str,
        fragment_refs: Sequence[str],
        project_root: Path | None,
    ) -> FolderLoadResult: ...


class BoundedYamlFolderFragmentLoader:
    """Read only explicitly listed, project-confined YAML fragments."""

    def load(
        self,
        *,
        source_path: Path,
        project_source: str,
        fragment_refs: Sequence[str],
        project_root: Path | None,
    ) -> FolderLoadResult:
        if not fragment_refs:
            raise FolderAuthoringError(
                "DPONE_AUTHORING_FOLDER_FRAGMENTS_INVALID",
                "Folder authoring requires a non-empty fragments list.",
            )
        if len(fragment_refs) > MAX_FRAGMENT_COUNT:
            raise FolderAuthoringError(
                "DPONE_AUTHORING_FOLDER_FRAGMENT_LIMIT_EXCEEDED",
                f"Folder authoring supports at most {MAX_FRAGMENT_COUNT} fragments.",
            )

        root = _project_root(project_source, source_path=source_path, configured=project_root)
        documents: list[FolderFragmentDocument] = []
        seen: set[str] = set()
        total_bytes = 0
        for raw_ref in fragment_refs:
            ref = _validate_fragment_ref(raw_ref)
            if ref in seen:
                raise FolderAuthoringError(
                    "DPONE_AUTHORING_FOLDER_FRAGMENT_DUPLICATE",
                    f"Fragment is listed more than once: {ref}",
                )
            seen.add(ref)
            path, content = _read_fragment_bytes(project_source=project_source, ref=ref, root=root)
            payload = _parse_fragment(path, content)
            total_bytes += len(content)
            if total_bytes > MAX_TOTAL_BYTES:
                raise FolderAuthoringError(
                    "DPONE_AUTHORING_FOLDER_TOTAL_SIZE_EXCEEDED",
                    f"Folder fragments exceed the {MAX_TOTAL_BYTES}-byte total limit.",
                )
            relative = path.relative_to(root).as_posix()
            dependency = AuthoringSourceDependency(
                kind="authoring_fragment",
                path=relative,
                sha256="sha256:" + hashlib.sha256(content).hexdigest(),
            )
            documents.append(FolderFragmentDocument(path=path, payload=payload, dependency=dependency))

        processes = tuple(
            process
            for document in documents
            for process in _normalized_fragment_processes(document, source_dir=source_path.parent)
        )
        return FolderLoadResult(
            processes=processes,
            dependencies=tuple(document.dependency for document in documents),
            source_documents=tuple(documents),
        )


def collect_sql_file_dependencies(
    payload: Mapping[str, Any],
    *,
    source_path: Path,
    project_root: Path,
) -> tuple[AuthoringSourceDependency, ...]:
    """Pin every SQL file referenced by one normalized authoring source graph."""

    root = project_root.resolve(strict=True)
    source_dir = source_path.absolute().parent
    dependencies: set[AuthoringSourceDependency] = set()
    for raw_path in iter_sql_file_paths(payload):
        relative = project_relative_path(root, source_dir / raw_path)
        dependencies.add(
            AuthoringSourceDependency(
                kind="sql_file",
                path=relative,
                sha256=sha256_confined_file(
                    root,
                    relative,
                    max_bytes=MAX_SQL_DEPENDENCY_BYTES,
                ),
            )
        )
    return tuple(sorted(dependencies, key=lambda item: (item.kind, item.path)))


def iter_sql_file_paths(value: Any) -> Iterable[str]:
    """Yield declared SQL file references from a nested authoring mapping."""

    if isinstance(value, Mapping):
        raw_sql_file = value.get("sql_file")
        if raw_sql_file:
            yield str(raw_sql_file)
        for nested in value.values():
            yield from iter_sql_file_paths(nested)
    elif isinstance(value, list):
        for item in value:
            yield from iter_sql_file_paths(item)


def _project_root(project_source: str, *, source_path: Path, configured: Path | None) -> Path:
    if configured is not None:
        return configured.resolve(strict=False)
    parts = PurePosixPath(project_source).parts
    if not parts or len(source_path.resolve(strict=False).parts) < len(parts):
        raise FolderAuthoringError(
            "DPONE_AUTHORING_SOURCE_PATH_INVALID",
            "Folder authoring could not infer the project root from authoring.source.",
        )
    return source_path.resolve(strict=False).parents[len(parts) - 1]


def _validate_fragment_ref(value: object) -> str:
    if not isinstance(value, str) or not value.strip() or "\\" in value:
        raise _invalid_path()
    ref = value.strip()
    path = PurePosixPath(ref)
    if path.is_absolute() or ".." in path.parts or "." in path.parts or path.suffix not in {".yaml", ".yml"}:
        raise _invalid_path()
    return path.as_posix()


def _read_fragment_bytes(*, project_source: str, ref: str, root: Path) -> tuple[Path, bytes]:
    try:
        root_path = root.resolve(strict=True)
        relative = (PurePosixPath(project_source).parent / PurePosixPath(ref)).as_posix()
        content = read_confined_file(root_path, relative, max_bytes=MAX_FRAGMENT_BYTES)
        return root_path.joinpath(*PurePosixPath(relative).parts), content
    except ConfinedFileError as exc:
        if exc.code == "file_too_large":
            raise FolderAuthoringError(
                "DPONE_AUTHORING_FOLDER_FRAGMENT_TOO_LARGE",
                f"Folder fragment exceeds the {MAX_FRAGMENT_BYTES}-byte file limit: {PurePosixPath(ref).name}",
            ) from exc
        if exc.code == "symlink_forbidden":
            raise FolderAuthoringError(
                "DPONE_AUTHORING_FOLDER_SYMLINK_FORBIDDEN",
                f"Folder fragment paths cannot contain symlinks: {ref}",
            ) from exc
        raise FolderAuthoringError(
            "DPONE_AUTHORING_FOLDER_FRAGMENT_NOT_FOUND",
            f"Folder fragment is missing or outside the project root: {ref}",
        ) from exc
    except (OSError, ValueError) as exc:
        raise FolderAuthoringError(
            "DPONE_AUTHORING_FOLDER_FRAGMENT_NOT_FOUND",
            f"Folder fragment is missing or outside the project root: {ref}",
        ) from exc


def _parse_fragment(path: Path, content: bytes) -> Mapping[str, Any]:
    try:
        payload = load_bounded_yaml(
            content,
            limits=BoundedYamlLimits(
                max_bytes=MAX_FRAGMENT_BYTES,
                max_tokens=MAX_YAML_TOKENS,
                max_depth=MAX_YAML_DEPTH,
                max_nodes=MAX_YAML_NODES,
            ),
        )
    except BoundedYamlError as exc:
        if exc.code == "alias_forbidden":
            raise FolderAuthoringError(
                "DPONE_AUTHORING_FOLDER_YAML_ALIAS_FORBIDDEN",
                "YAML anchors and aliases are forbidden in folder fragments.",
            ) from exc
        if exc.code in {"token_limit", "depth_limit", "node_limit"}:
            raise FolderAuthoringError(
                "DPONE_AUTHORING_FOLDER_YAML_BUDGET_EXCEEDED",
                "Folder fragment exceeds the YAML parser budget.",
            ) from exc
        raise FolderAuthoringError(
            "DPONE_AUTHORING_FOLDER_FRAGMENT_INVALID",
            f"Folder fragment is not bounded UTF-8 YAML: {path.name}",
        ) from exc
    if not isinstance(payload, Mapping):
        raise FolderAuthoringError(
            "DPONE_AUTHORING_FOLDER_FRAGMENT_INVALID",
            f"Folder fragment must be a YAML object: {path.name}",
        )
    if set(payload) != {"kind", "processes"} or payload.get("kind") != "dpone.flow-fragment.v1":
        raise FolderAuthoringError(
            "DPONE_AUTHORING_FOLDER_FRAGMENT_INVALID",
            "Folder fragment requires only kind: dpone.flow-fragment.v1 and processes.",
        )
    processes = payload.get("processes")
    if not isinstance(processes, list) or not processes:
        raise FolderAuthoringError(
            "DPONE_AUTHORING_FOLDER_FRAGMENT_INVALID",
            f"Folder fragment processes must be a non-empty list: {path.name}",
        )
    return payload


def _normalized_fragment_processes(
    document: FolderFragmentDocument,
    *,
    source_dir: Path,
) -> tuple[Mapping[str, Any], ...]:
    fragment_dir = PurePosixPath(document.path.relative_to(source_dir).as_posix()).parent
    raw_processes = document.payload["processes"]
    return tuple(_rewrite_fragment_file_paths(copy.deepcopy(process), fragment_dir) for process in raw_processes)


def _rewrite_fragment_file_paths(value: Any, fragment_dir: PurePosixPath) -> Any:
    if isinstance(value, Mapping):
        normalized = dict(value)
        raw_sql_file = normalized.get("sql_file")
        if raw_sql_file is not None:
            normalized["sql_file"] = _root_relative_fragment_file(raw_sql_file, fragment_dir)
        return {key: _rewrite_fragment_file_paths(item, fragment_dir) for key, item in normalized.items()}
    if isinstance(value, list):
        return [_rewrite_fragment_file_paths(item, fragment_dir) for item in value]
    return value


def _root_relative_fragment_file(value: object, fragment_dir: PurePosixPath) -> str:
    if not isinstance(value, str) or not value.strip() or "\\" in value:
        raise _invalid_path()
    raw = PurePosixPath(value.strip())
    if raw.is_absolute() or ".." in raw.parts:
        raise _invalid_path()
    return (fragment_dir / raw).as_posix()


def _invalid_path() -> FolderAuthoringError:
    return FolderAuthoringError(
        "DPONE_AUTHORING_FOLDER_PATH_INVALID",
        "Fragment paths must be relative POSIX YAML paths without traversal.",
    )


__all__ = [
    "AuthoringSourceDependency",
    "BoundedYamlFolderFragmentLoader",
    "collect_sql_file_dependencies",
    "FolderAuthoringError",
    "FolderFragmentDocument",
    "FolderFragmentLoader",
    "FolderLoadResult",
    "iter_sql_file_paths",
]
