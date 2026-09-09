"""Immutable, bounded source inventory for a multi-project dbt release."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, TypeVar

from dpone.contracts.airflow_deployment import canonical_fingerprint, is_canonical_sha256_digest
from dpone.contracts.dbt_contract_validation import sha256_bytes
from dpone.contracts.dbt_identifiers import dbt_workflow_id
from dpone.contracts.dbt_runtime_payloads import (
    DBT_RUNTIME_WIRE_V2,
    MAX_DBT_RUNTIME_PAYLOAD_BYTES,
    MAX_DBT_RUNTIME_PAYLOAD_TOTAL_BYTES,
    dbt_runtime_payload_reference,
    validate_dbt_runtime_payload_trio,
)
from dpone.contracts.dbt_workspace_paths import (
    dbt_workspace_directories,
    validate_dbt_project_layout,
    validate_dbt_project_name,
    validate_dbt_project_path,
)
from dpone.contracts.strict_json import strict_json_object

DBT_SOURCE_INVENTORY_SCHEMA = "dpone.dbt-source-snapshot.v2"
MAX_DBT_SOURCE_INVENTORY_BYTES = 1024 * 1024
_MAX_ITEMS = 64
_DAG_ID = re.compile(r"[A-Za-z0-9_.-]{1,250}")
_WORKFLOW_KEYS = frozenset({"workflow_id", "dag_id", "workload_id", "runtime_payload_ids", "selection_lock_sha256"})
_PROJECT_KEYS = frozenset(
    {"project_path", "project_name", "project_bundle_sha256", "manifest_sha256", "toolchain_sha256", "workflows"}
)
_SNAPSHOT_KEYS = frozenset({"schema", "projects", "snapshot_sha256"})
_T = TypeVar("_T")


@dataclass(frozen=True, slots=True)
class DbtWorkflowSource:
    """A workflow's exact source trio and stable execution/DAG ownership."""

    workflow_id: str
    dag_id: str
    workload_id: str
    runtime_payload_ids: tuple[str, ...]
    selection_lock_sha256: str

    def __post_init__(self) -> None:
        dbt_workflow_id(self.workflow_id)
        if not isinstance(self.dag_id, str) or _DAG_ID.fullmatch(self.dag_id) is None:
            raise ValueError("source inventory DAG identity is invalid")
        if self.workload_id != f"dbt__{self.workflow_id}":
            raise ValueError("source inventory workflow does not own its workload")
        ids = _items(self.runtime_payload_ids, str, "workflow payload IDs")
        validate_dbt_runtime_payload_trio(ids, wire_contract=DBT_RUNTIME_WIRE_V2)
        _digest(self.selection_lock_sha256)
        selection = dbt_runtime_payload_reference(ids[2], wire_contract=DBT_RUNTIME_WIRE_V2)
        if selection.sha256 != self.selection_lock_sha256:
            raise ValueError("source inventory selection byte identity differs from its payload ID")
        object.__setattr__(self, "runtime_payload_ids", ids)

    def to_dict(self) -> dict[str, Any]:
        return {
            "workflow_id": self.workflow_id,
            "dag_id": self.dag_id,
            "workload_id": self.workload_id,
            "runtime_payload_ids": list(self.runtime_payload_ids),
            "selection_lock_sha256": self.selection_lock_sha256,
        }

    @classmethod
    def from_mapping(cls, value: object) -> DbtWorkflowSource:
        item = _mapping(value, _WORKFLOW_KEYS, "source workflow")
        return cls(
            workflow_id=_text(item, "workflow_id"),
            dag_id=_text(item, "dag_id"),
            workload_id=_text(item, "workload_id"),
            runtime_payload_ids=_items(item["runtime_payload_ids"], str, "workflow payload IDs"),
            selection_lock_sha256=_text(item, "selection_lock_sha256"),
        )


@dataclass(frozen=True, slots=True)
class DbtProjectSource:
    """Source hashes and workflow ownership for one repository-relative project."""

    project_path: str
    project_name: str
    project_bundle_sha256: str
    manifest_sha256: str
    toolchain_sha256: str
    workflows: tuple[DbtWorkflowSource, ...]

    def __post_init__(self) -> None:
        validate_dbt_project_path(self.project_path)
        validate_dbt_project_name(self.project_name)
        for digest in (self.project_bundle_sha256, self.manifest_sha256, self.toolchain_sha256):
            _digest(digest)
        workflows = _items(self.workflows, DbtWorkflowSource, "project workflows")
        _sorted_unique(tuple(item.workflow_id for item in workflows), "project workflows")
        for workflow in workflows:
            project, manifest = (
                dbt_runtime_payload_reference(item, wire_contract=DBT_RUNTIME_WIRE_V2)
                for item in workflow.runtime_payload_ids[:2]
            )
            if project.sha256 != self.project_bundle_sha256 or manifest.sha256 != self.manifest_sha256:
                raise ValueError("source inventory workflow is bound to another project's bytes")
        object.__setattr__(self, "workflows", workflows)

    def to_dict(self) -> dict[str, Any]:
        return {
            "project_path": self.project_path,
            "project_name": self.project_name,
            "project_bundle_sha256": self.project_bundle_sha256,
            "manifest_sha256": self.manifest_sha256,
            "toolchain_sha256": self.toolchain_sha256,
            "workflows": [item.to_dict() for item in self.workflows],
        }

    @classmethod
    def from_mapping(cls, value: object) -> DbtProjectSource:
        item = _mapping(value, _PROJECT_KEYS, "source project")
        workflows = _items(item["workflows"], object, "project workflows")
        return cls(
            project_path=_text(item, "project_path"),
            project_name=_text(item, "project_name"),
            project_bundle_sha256=_text(item, "project_bundle_sha256"),
            manifest_sha256=_text(item, "manifest_sha256"),
            toolchain_sha256=_text(item, "toolchain_sha256"),
            workflows=tuple(DbtWorkflowSource.from_mapping(workflow) for workflow in workflows),
        )


@dataclass(frozen=True, slots=True)
class DbtSourceInventory:
    """Complete canonical inventory; its fingerprint excludes only itself."""

    projects: tuple[DbtProjectSource, ...]

    def __post_init__(self) -> None:
        projects = _items(self.projects, DbtProjectSource, "source projects")
        paths = tuple(item.project_path for item in projects)
        _sorted_unique(paths, "project paths")
        validate_dbt_project_layout(tuple((item.project_path, item.project_name) for item in projects))
        workflows = tuple(workflow for project in projects for workflow in project.workflows)
        if len(workflows) > _MAX_ITEMS:
            raise ValueError("source inventory workflow bound exceeded")
        for field in ("workflow_id", "dag_id", "workload_id"):
            _unique(tuple(getattr(item, field) for item in workflows), field)
        if len({payload for item in workflows for payload in item.runtime_payload_ids}) > _MAX_ITEMS:
            raise ValueError("source inventory runtime payload bound exceeded")
        object.__setattr__(self, "projects", projects)

    @property
    def project_directories(self) -> frozenset[str]:
        """Exact project roots and structural parents, without filesystem access."""

        return dbt_workspace_directories(tuple(project.project_path for project in self.projects))

    def bind_project_bundles(self, project_bundles: Mapping[str, bytes]) -> Mapping[str, bytes]:
        """Freeze complete, bounded source bytes against their declared identities.

        This checks membership/hash/size, not archive contents, signatures or
        live readiness. Extraction and full source verification remain mandatory.
        Resource accounting charges identical content-addressed bytes once.
        """

        if set(project_bundles) != {project.project_path for project in self.projects}:
            raise ValueError("workspace bundles differ from the complete source inventory")
        archives: dict[str, bytes] = {}
        unique_bytes: dict[str, int] = {}
        for project in self.projects:
            archive = project_bundles[project.project_path]
            if not isinstance(archive, bytes) or not archive or len(archive) > MAX_DBT_RUNTIME_PAYLOAD_BYTES:
                raise ValueError("workspace project archive exceeds its byte bound")
            if sha256_bytes(archive) != project.project_bundle_sha256:
                raise ValueError("workspace project archive differs from source inventory")
            archives[project.project_path] = archive
            unique_bytes[project.project_bundle_sha256] = len(archive)
        if sum(unique_bytes.values()) > MAX_DBT_RUNTIME_PAYLOAD_TOTAL_BYTES:
            raise ValueError("workspace project archives exceed the aggregate byte bound")
        return MappingProxyType(archives)

    @property
    def snapshot_sha256(self) -> str:
        return canonical_fingerprint(self._unsigned())

    def to_dict(self) -> dict[str, Any]:
        unsigned = self._unsigned()
        return {**unsigned, "snapshot_sha256": canonical_fingerprint(unsigned)}

    def _unsigned(self) -> dict[str, Any]:
        return {"schema": DBT_SOURCE_INVENTORY_SCHEMA, "projects": [item.to_dict() for item in self.projects]}

    @classmethod
    def build(cls, projects: Sequence[DbtProjectSource]) -> DbtSourceInventory:
        """Canonicalize discovery order before producing a new inventory."""

        items = _items(projects, DbtProjectSource, "source projects")
        return cls(tuple(sorted(items, key=lambda item: item.project_path)))

    @classmethod
    def from_mapping(cls, value: object) -> DbtSourceInventory:
        """Validate signed input without repairing or reordering it."""

        item = _mapping(value, _SNAPSHOT_KEYS, "source inventory")
        if item["schema"] != DBT_SOURCE_INVENTORY_SCHEMA:
            raise ValueError("source inventory version is unsupported")
        projects = _items(item["projects"], object, "source projects")
        result = cls(tuple(DbtProjectSource.from_mapping(project) for project in projects))
        if _text(item, "snapshot_sha256") != result.snapshot_sha256:
            raise ValueError("source inventory fingerprint is invalid")
        return result

    @classmethod
    def from_payload(cls, payload: bytes) -> DbtSourceInventory:
        if not isinstance(payload, bytes) or not payload or len(payload) > MAX_DBT_SOURCE_INVENTORY_BYTES:
            raise ValueError("source inventory bytes are missing or exceed the bound")
        try:
            return cls.from_mapping(strict_json_object(payload))
        except RecursionError as exc:
            raise ValueError("source inventory JSON nesting exceeds its bound") from exc


def _items(value: object, item_type: type[_T], label: str) -> tuple[_T, ...]:
    if (
        not isinstance(value, (tuple, list))
        or not value
        or len(value) > _MAX_ITEMS
        or any(not isinstance(item, item_type) for item in value)
    ):
        raise ValueError(f"{label} must be a non-empty bounded array")
    return tuple(value)


def _mapping(value: object, keys: frozenset[str], label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or set(value) != keys:
        raise ValueError(f"{label} fields are invalid")
    return value


def _text(value: Mapping[str, object], key: str) -> str:
    result = value[key]
    if not isinstance(result, str) or not result:
        raise ValueError(f"source inventory {key} is invalid")
    return result


def _digest(value: str) -> None:
    if not is_canonical_sha256_digest(value):
        raise ValueError("source inventory digest is invalid")


def _sorted_unique(values: tuple[str, ...], label: str) -> None:
    _unique(values, label)
    if values != tuple(sorted(values)):
        raise ValueError(f"source inventory {label} must be sorted")


def _unique(values: tuple[str, ...], label: str) -> None:
    if len(set(values)) != len(values):
        raise ValueError(f"source inventory {label} must be unique")


__all__ = [
    "DBT_SOURCE_INVENTORY_SCHEMA",
    "MAX_DBT_SOURCE_INVENTORY_BYTES",
    "DbtProjectSource",
    "DbtSourceInventory",
    "DbtWorkflowSource",
]
