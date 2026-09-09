"""Canonical bounded reader for ``dpone.project.v1`` configuration."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Literal

from dpone.manifest.bounded_yaml import BoundedYamlError, BoundedYamlLimits, load_bounded_yaml
from dpone.manifest.confined_files import ConfinedFileError, read_confined_file

PROJECT_CONFIG_PATH = "dpone.yaml"
DEFAULT_DOMAIN_FIRST_ROOT = "workloads"
_PROJECT_CONFIG_LIMITS = BoundedYamlLimits(max_bytes=64 * 1024, max_tokens=4_000, max_depth=16, max_nodes=2_000)
_LAYOUT_FIELDS = frozenset({"mode", "root", "pipeline_id_scope", "system_root", "dual_read_legacy_catalogs"})
DEFAULT_SYSTEM_ROOT = ".dpone"
LayoutMode = Literal["flat", "domain_first"]
PipelineIdScope = Literal["project"]


class ProjectConfigError(ValueError):
    """The project configuration is missing or violates its bounded contract."""

    def __init__(self, reason: str, *, missing: bool = False) -> None:
        super().__init__("Project configuration must be a valid dpone.project.v1 object.")
        self.reason = reason
        self.missing = missing


@dataclass(frozen=True, slots=True)
class ProjectConfigSnapshot:
    """One stable project-config payload and its exact content digest."""

    payload: Mapping[str, Any]
    sha256: str


@dataclass(frozen=True, slots=True)
class ProjectAirflowDecision:
    """Effective Airflow authoring policy and its observable authority."""

    enabled: bool
    source: str


@dataclass(frozen=True, slots=True)
class ProjectLayout:
    """Canonical authoring layout selected once for the whole project."""

    mode: LayoutMode = "flat"
    root: str = DEFAULT_DOMAIN_FIRST_ROOT
    pipeline_id_scope: PipelineIdScope = "project"
    system_root: str | None = None
    dual_read_legacy_catalogs: bool = False

    @property
    def is_domain_first(self) -> bool:
        return self.mode == "domain_first"


class ProjectConfigReader:
    """Read one no-follow, unique-key project configuration snapshot."""

    def __init__(self, root: str | Path) -> None:
        root_path = Path(root)
        if root_path.is_symlink():
            raise ProjectConfigError("project_root_invalid")
        self._root = root_path.resolve(strict=True)

    def read(self, *, required: bool = True) -> ProjectConfigSnapshot | None:
        try:
            content = read_confined_file(
                self._root,
                PROJECT_CONFIG_PATH,
                max_bytes=_PROJECT_CONFIG_LIMITS.max_bytes,
            )
        except ConfinedFileError as exc:
            if exc.code == "file_not_found" and not required:
                return None
            raise ProjectConfigError(exc.code, missing=exc.code == "file_not_found") from exc
        try:
            payload = load_bounded_yaml(content, limits=_PROJECT_CONFIG_LIMITS)
        except BoundedYamlError as exc:
            raise ProjectConfigError(exc.code) from exc
        if not isinstance(payload, Mapping) or payload.get("schema") != "dpone.project.v1":
            raise ProjectConfigError("schema_invalid")
        _validate_project_payload(payload)
        return ProjectConfigSnapshot(
            payload=payload,
            sha256="sha256:" + hashlib.sha256(content).hexdigest(),
        )


def resolve_project_airflow_enabled(root: str | Path, *, explicit: bool | None) -> bool:
    """Resolve the pipeline Airflow flag without reading defaults after an override."""

    return resolve_project_airflow_decision(root, explicit=explicit).enabled


def resolve_project_layout(root: str | Path) -> ProjectLayout:
    """Read a strict layout policy while preserving the missing-layout flat default."""

    layout, _ = load_project_layout(root)
    return layout


def load_project_layout(
    root: str | Path,
) -> tuple[ProjectLayout, ProjectConfigSnapshot | None]:
    """Read layout and its exact config snapshot once for downstream pinning."""

    root_path = Path(root)
    if root_path.is_symlink():
        raise ProjectConfigError("project_root_invalid")
    if not root_path.exists():
        return ProjectLayout(), None
    if not root_path.is_dir():
        raise ProjectConfigError("project_root_invalid")
    snapshot = ProjectConfigReader(root_path).read(required=False)
    if snapshot is None:
        return ProjectLayout(), None
    return _project_layout(snapshot.payload), snapshot


def _project_layout(payload: Mapping[str, Any]) -> ProjectLayout:
    if "layout" not in payload:
        return ProjectLayout()
    raw_layout = payload["layout"]
    if not isinstance(raw_layout, Mapping):
        raise ProjectConfigError("layout_invalid")
    if set(raw_layout) - _LAYOUT_FIELDS:
        raise ProjectConfigError("layout_fields_invalid")
    raw_mode = raw_layout.get("mode")
    if not isinstance(raw_mode, str) or raw_mode not in {"flat", "domain_first"}:
        raise ProjectConfigError("layout_mode_invalid")
    mode: LayoutMode = "domain_first" if raw_mode == "domain_first" else "flat"
    raw_root = raw_layout.get("root", DEFAULT_DOMAIN_FIRST_ROOT)
    if not isinstance(raw_root, str):
        raise ProjectConfigError("layout_root_invalid")
    normalized_root = _normalized_project_directory(raw_root)
    raw_scope = raw_layout.get("pipeline_id_scope", "project")
    if raw_scope != "project":
        raise ProjectConfigError("pipeline_id_scope_invalid")
    if mode == "domain_first":
        raw_system_root = raw_layout.get("system_root", DEFAULT_SYSTEM_ROOT)
        dual_read_default = True
    else:
        raw_system_root = raw_layout.get("system_root")
        dual_read_default = False
    if raw_system_root is None:
        system_root = None
    elif not isinstance(raw_system_root, str):
        raise ProjectConfigError("layout_system_root_invalid")
    else:
        try:
            system_root = _normalized_project_directory(raw_system_root)
        except ProjectConfigError as exc:
            raise ProjectConfigError("layout_system_root_invalid") from exc
    raw_dual = raw_layout.get("dual_read_legacy_catalogs", dual_read_default)
    if not isinstance(raw_dual, bool):
        raise ProjectConfigError("layout_dual_read_legacy_catalogs_invalid")
    return ProjectLayout(
        mode=mode,
        root=normalized_root,
        pipeline_id_scope="project",
        system_root=system_root,
        dual_read_legacy_catalogs=raw_dual,
    )


def resolve_project_airflow_decision(root: str | Path, *, explicit: bool | None) -> ProjectAirflowDecision:
    """Resolve Airflow policy and retain whether CLI, project, or legacy default won."""

    if explicit is not None:
        return ProjectAirflowDecision(enabled=explicit, source="explicit")
    snapshot = ProjectConfigReader(root).read(required=False)
    return project_airflow_decision(snapshot, explicit=explicit)


def project_airflow_decision(
    snapshot: ProjectConfigSnapshot | None,
    *,
    explicit: bool | None,
) -> ProjectAirflowDecision:
    """Resolve Airflow policy from the same pinned project snapshot as layout."""

    if explicit is not None:
        return ProjectAirflowDecision(enabled=explicit, source="explicit")
    if snapshot is None:
        return ProjectAirflowDecision(enabled=False, source="legacy_default")
    if "airflow" not in snapshot.payload:
        return ProjectAirflowDecision(enabled=False, source="legacy_default")
    airflow = snapshot.payload["airflow"]
    if not isinstance(airflow, Mapping):
        raise ProjectConfigError("airflow_invalid")
    enabled = airflow.get("enabled", False)
    if not isinstance(enabled, bool):
        raise ProjectConfigError("airflow_enabled_invalid")
    return ProjectAirflowDecision(enabled=enabled, source="project")


def _normalized_project_directory(value: str) -> str:
    text = value.strip()
    parsed = PurePosixPath(text)
    if (
        not text
        or value != text
        or "\x00" in text
        or "\\" in text
        or parsed.is_absolute()
        or any(part in {"", ".", ".."} for part in parsed.parts)
    ):
        raise ProjectConfigError("layout_root_invalid")
    return parsed.as_posix()


def _validate_project_payload(payload: Mapping[str, Any]) -> None:
    if "authoring" in payload:
        authoring = payload["authoring"]
        if not isinstance(authoring, Mapping):
            raise ProjectConfigError("authoring_invalid")
        if "primary_source_policy" in authoring and not isinstance(authoring["primary_source_policy"], str):
            raise ProjectConfigError("primary_source_policy_invalid")
    if "airflow" in payload:
        airflow = payload["airflow"]
        if not isinstance(airflow, Mapping):
            raise ProjectConfigError("airflow_invalid")
        if "enabled" in airflow and not isinstance(airflow["enabled"], bool):
            raise ProjectConfigError("airflow_enabled_invalid")
        if "index_path" in airflow and not isinstance(airflow["index_path"], str):
            raise ProjectConfigError("airflow_index_path_invalid")
    _project_layout(payload)


__all__ = [
    "PROJECT_CONFIG_PATH",
    "DEFAULT_DOMAIN_FIRST_ROOT",
    "DEFAULT_SYSTEM_ROOT",
    "LayoutMode",
    "PipelineIdScope",
    "ProjectAirflowDecision",
    "ProjectConfigError",
    "ProjectConfigReader",
    "ProjectConfigSnapshot",
    "ProjectLayout",
    "load_project_layout",
    "project_airflow_decision",
    "resolve_project_airflow_decision",
    "resolve_project_airflow_enabled",
    "resolve_project_layout",
]
