"""Confined project discovery and input loading for hermetic tests."""

from __future__ import annotations

import os
import posixpath
import stat
from collections.abc import Iterable, Mapping
from pathlib import Path, PurePosixPath
from typing import Any

from dpone.manifest.hermetic_test_io import (
    HERMETIC_YAML_MAX_BYTES,
    HermeticInputReadError,
    parse_hermetic_project_yaml,
    read_hermetic_project_file,
)
from dpone.manifest.pipeline_source_reference import (
    PipelineSourceReferenceError,
    resolve_pipeline_reference,
)
from dpone.manifest.project_config import ProjectConfigError, ProjectLayout, resolve_project_layout
from dpone.manifest.project_discovery import ProjectDiscoveryService
from dpone.manifest.project_layout_authority import conflicting_authoring_layout

_MAX_TEST_FILES = 100


class HermeticProjectError(ValueError):
    """Safe project-I/O failure normalized without raw OS details."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        exit_code: int = 2,
        stage: str = "test_input",
        path: str | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.exit_code = exit_code
        self.stage = stage
        self.path = path


class HermeticTestProject:
    """A bounded, cache-on-first-read view of one project test invocation."""

    def __init__(self, root: Path) -> None:
        self._root = root.resolve(strict=True)
        self._cache: dict[tuple[str, int], bytes] = {}
        self._input_paths: set[str] = set()

    @property
    def root(self) -> Path:
        return self._root

    @property
    def input_paths(self) -> tuple[str, ...]:
        """Every project-relative input attempted by this invocation."""

        return tuple(sorted(self._input_paths))

    def discover(self, target: str | Path) -> tuple[str, ...]:
        layout = self._resolve_layout()
        self._require_compatible_authority(layout)
        if _is_project_target(target, self._root):
            return self._discover_suite(layout)
        relative = _target_relative(self._root, target)
        if _is_test_path(relative):
            return (relative,)
        pipeline_path, requested_id = self._find_pipeline_path(relative, layout)
        payload = self.read_yaml(pipeline_path)
        metadata = payload.get("metadata")
        pipeline_id = str(metadata.get("id") or "").strip() if isinstance(metadata, Mapping) else ""
        if not pipeline_id or "/" in pipeline_id or "\\" in pipeline_id:
            raise HermeticProjectError(
                "DPONE_TEST_PIPELINE_INVALID",
                "Pipeline metadata.id is required to resolve its test file.",
                stage="test_discovery",
                path=pipeline_path,
            )
        if requested_id is not None and pipeline_id != requested_id:
            raise HermeticProjectError(
                "DPONE_PIPELINE_ID_MISMATCH",
                "Requested pipeline id does not match metadata.id in the primary source.",
                stage="test_discovery",
                path=pipeline_path,
            )
        return (self._pipeline_test_path(pipeline_path, pipeline_id, layout),)

    def read_yaml(self, relative_path: str) -> dict[str, Any]:
        content = self._read(relative_path, max_bytes=HERMETIC_YAML_MAX_BYTES)
        try:
            return parse_hermetic_project_yaml(content, path=relative_path)
        except HermeticInputReadError as exc:
            raise HermeticProjectError(
                exc.code,
                str(exc),
                exit_code=exc.exit_code,
                stage="test_manifest",
                path=exc.path,
            ) from exc

    def read_fixture(self, relative_path: str, *, max_bytes: int) -> bytes:
        return self._read(relative_path, max_bytes=max_bytes, fixture=True)

    def track_inputs(self, relative_paths: Iterable[str]) -> None:
        """Protect compiler-consumed authoring dependencies from report output."""

        for relative_path in relative_paths:
            _validate_project_relative(relative_path)
            self._input_paths.add(relative_path)

    def track_declared_authoring_inputs(self, pipeline_path: str, payload: Mapping[str, Any]) -> None:
        """Protect dependencies visible in the primary source before compilation."""

        authoring = payload.get("authoring")
        mode = str(authoring.get("mode") or "") if isinstance(authoring, Mapping) else ""
        if mode == "folder":
            fragments = payload.get("fragments")
            if isinstance(fragments, list):
                for raw_ref in fragments:
                    if isinstance(raw_ref, str):
                        self.track_inputs((self.resolve_reference(pipeline_path, raw_ref),))
        recipe = payload.get("recipe")
        artifact_ref = recipe.get("artifact_ref") if isinstance(recipe, Mapping) else None
        if isinstance(artifact_ref, str):
            self.track_inputs((artifact_ref,))

    def resolve_reference(self, base_path: str, raw_reference: str) -> str:
        if "\\" in raw_reference or PurePosixPath(raw_reference).is_absolute():
            raise _unsafe_reference()
        combined = posixpath.normpath(f"{PurePosixPath(base_path).parent.as_posix()}/{raw_reference}")
        _validate_project_relative(combined)
        return combined

    def _read(self, relative_path: str, *, max_bytes: int, fixture: bool = False) -> bytes:
        self._input_paths.add(relative_path)
        key = (relative_path, max_bytes)
        if key in self._cache:
            return self._cache[key]
        try:
            content = read_hermetic_project_file(
                self._root,
                relative_path,
                max_bytes=max_bytes,
                fixture=fixture,
            )
        except HermeticInputReadError as exc:
            raise HermeticProjectError(
                exc.code,
                str(exc),
                exit_code=exc.exit_code,
                stage="test_input",
                path=exc.path,
            ) from exc
        self._cache[key] = content
        return content

    def _discover_suite(self, layout: ProjectLayout) -> tuple[str, ...]:
        if layout.is_domain_first:
            snapshot = ProjectDiscoveryService(self._root).discover(layout=layout)
            if snapshot.issues:
                issue = snapshot.issues[0]
                raise HermeticProjectError(
                    issue.code,
                    issue.message,
                    exit_code=_discovery_issue_exit_code(issue.code),
                    stage="test_discovery",
                    path=issue.path,
                )
            paths = tuple(
                self._pipeline_test_path(
                    workload.checked_source.source_label,
                    workload.pipeline_id,
                    layout,
                )
                for workload in snapshot.workloads
            )
            if not paths:
                raise HermeticProjectError(
                    "DPONE_TEST_SUITE_EMPTY",
                    "No domain-first pipeline tests were discovered.",
                    stage="test_discovery",
                )
            if len(paths) > _MAX_TEST_FILES:
                raise HermeticProjectError(
                    "DPONE_TEST_SUITE_LIMIT_EXCEEDED",
                    "Project test suite exceeds the 100-file discovery limit.",
                    exit_code=4,
                    stage="test_discovery",
                )
            return paths
        directory = self._root / "tests"
        try:
            metadata = directory.lstat()
        except OSError as exc:
            raise HermeticProjectError(
                "DPONE_TEST_SUITE_NOT_FOUND",
                "Project tests directory was not found.",
                stage="test_discovery",
            ) from exc
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
            raise HermeticProjectError(
                "DPONE_TEST_PATH_UNSAFE",
                "Project tests path must be a non-symlink directory.",
                exit_code=4,
                stage="test_discovery",
                path="tests",
            )
        try:
            entries = tuple(directory.iterdir())
        except OSError as exc:
            raise HermeticProjectError(
                "DPONE_TEST_SUITE_UNAVAILABLE",
                "Project tests directory could not be enumerated safely.",
                exit_code=4,
                stage="test_discovery",
            ) from exc
        paths = tuple(
            f"tests/{path.name}" for path in sorted(entries, key=lambda item: item.name) if _is_test_path(path.name)
        )
        if not paths:
            raise HermeticProjectError(
                "DPONE_TEST_SUITE_EMPTY",
                "No direct tests/*.test.yaml files were found.",
                stage="test_discovery",
            )
        if len(paths) > _MAX_TEST_FILES:
            raise HermeticProjectError(
                "DPONE_TEST_SUITE_LIMIT_EXCEEDED",
                "Project test suite exceeds the 100-file discovery limit.",
                exit_code=4,
                stage="test_discovery",
            )
        return paths

    def _find_pipeline_path(
        self,
        relative: str,
        layout: ProjectLayout,
    ) -> tuple[str, str | None]:
        try:
            reference = resolve_pipeline_reference(
                self._root,
                relative,
                layout_snapshot=layout,
            )
        except PipelineSourceReferenceError as exc:
            raise HermeticProjectError(
                exc.code,
                str(exc),
                exit_code=exc.exit_code,
                stage="test_discovery",
                path=exc.path,
            ) from exc
        return reference.relative_path, str(reference.requested_id) if reference.requested_id is not None else None

    def _pipeline_test_path(
        self,
        pipeline_path: str,
        pipeline_id: str,
        layout: ProjectLayout,
    ) -> str:
        if layout.is_domain_first:
            parent = PurePosixPath(pipeline_path).parent
            return f"{parent.as_posix()}/tests/pipeline.test.yaml"
        return f"tests/{pipeline_id}.test.yaml"

    def _resolve_layout(self) -> ProjectLayout:
        try:
            return resolve_project_layout(self._root)
        except ProjectConfigError as exc:
            raise HermeticProjectError(
                "DPONE_PROJECT_CONFIG_INVALID",
                "Project configuration is invalid.",
                stage="test_discovery",
                path="dpone.yaml",
            ) from exc

    def _require_compatible_authority(self, layout: ProjectLayout) -> None:
        conflict = conflicting_authoring_layout(
            self._root,
            expected_mode=layout.mode,
            domain_first_root=layout.root,
        )
        if conflict is not None:
            raise HermeticProjectError(
                "DPONE_PROJECT_LAYOUT_MIGRATION_REQUIRED",
                "Existing authoring authority does not match the configured project layout.",
                exit_code=1,
                stage="test_discovery",
                path=layout.root,
            )


def default_test_name(path: str) -> str:
    name = PurePosixPath(path).name
    for suffix in (".test.yaml", ".test.yml"):
        if name.endswith(suffix):
            return name[: -len(suffix)]
    return name or "test"


def display_target(target: str | Path) -> str:
    text = str(target).replace("\\", "/")
    return PurePosixPath(text).name or "project"


def _target_relative(root: Path, target: str | Path) -> str:
    raw = Path(target)
    if raw.is_absolute():
        try:
            return Path(os.path.abspath(raw)).relative_to(root).as_posix()
        except ValueError as exc:
            raise _unsafe_reference() from exc
    text = str(target)
    if "\\" in text:
        raise _unsafe_reference()
    normalized = posixpath.normpath(text)
    _validate_project_relative(normalized)
    return normalized


def _validate_project_relative(value: str) -> None:
    path = PurePosixPath(value)
    if not value or path.is_absolute() or value == ".." or value.startswith("../"):
        raise _unsafe_reference()


def _unsafe_reference() -> HermeticProjectError:
    return HermeticProjectError(
        "DPONE_TEST_PATH_UNSAFE",
        "Test reference must stay inside the configured project root.",
        exit_code=4,
        stage="test_input",
    )


def _discovery_issue_exit_code(code: str) -> int:
    if code == "DPONE_PROJECT_LAYOUT_MIGRATION_REQUIRED":
        return 1
    if code in {"DPONE_DISCOVERY_PATH_INVALID", "DPONE_LAYOUT_ROOT_INVALID"}:
        return 4
    return 2


def _is_project_target(target: str | Path, root: Path) -> bool:
    text = str(target)
    if text in {"", "."}:
        return True
    try:
        return Path(os.path.abspath(Path(target))) == root if Path(target).is_absolute() else False
    except OSError:
        return False


def _is_test_path(value: str) -> bool:
    return value.endswith((".test.yaml", ".test.yml"))


__all__ = [
    "HermeticProjectError",
    "HermeticTestProject",
    "default_test_name",
    "display_target",
]
