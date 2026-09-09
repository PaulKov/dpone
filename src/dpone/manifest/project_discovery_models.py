"""Immutable domain-first discovery and workload-index projections."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from types import MappingProxyType
from typing import Any

from dpone.manifest.authoring import AuthoringCompilation
from dpone.manifest.domain_dag_models import DiscoveredDomainDag
from dpone.manifest.project_config import ProjectLayout
from dpone.manifest.project_discovery_identity import (
    project_fingerprint as compute_project_fingerprint,
)
from dpone.manifest.project_discovery_identity import (
    workload_fingerprint as compute_workload_fingerprint,
)
from dpone.manifest.project_selection_contracts import ProjectCheckedSource


class ProjectDiscoveryProjectionError(ValueError):
    """A failed or malformed discovery snapshot cannot become build evidence."""


@dataclass(frozen=True, slots=True)
class ProjectDiscoveryIssue:
    """One stable, safe discovery failure suitable for CLI/API adaptation."""

    code: str
    message: str
    path: str | None = None
    pipeline_id: str | None = None
    domain: str | None = None


@dataclass(frozen=True, slots=True)
class DiscoveredWorkload:
    """One compiled primary source and its path-owned domain metadata."""

    pipeline_id: str
    domain: str
    owner: str | None
    ownership_fingerprint: str
    airflow_enabled: bool
    checked_source: ProjectCheckedSource
    connection_refs: tuple[str, ...]

    @property
    def workload_fingerprint(self) -> str:
        return compute_workload_fingerprint(self.identity_material())

    def identity_material(self) -> dict[str, Any]:
        """Return canonical non-secret material used by selection identities."""

        dependencies = [
            {"kind": item.kind, "path": item.path, "sha256": item.sha256}
            for item in self.checked_source.compilation.dependencies
        ]
        return {
            "pipeline_id": self.pipeline_id,
            "domain": self.domain,
            "owner": self.owner,
            "ownership_fingerprint": self.ownership_fingerprint,
            "authoring_source": self.checked_source.source_label,
            "source_sha256": self.checked_source.source_sha256,
            "semantic_fingerprint": self.checked_source.compilation.semantic_fingerprint,
            "connection_refs": list(self.connection_refs),
            "dependencies": dependencies,
            "airflow": {"enabled": self.airflow_enabled, "dag_id": self.pipeline_id, "schedule": None},
        }


@dataclass(frozen=True, slots=True)
class ProjectDiscoverySnapshot:
    """Immutable discovery snapshot consumed by selection and CI projections."""

    layout: ProjectLayout
    workloads: tuple[DiscoveredWorkload, ...]
    issues: tuple[ProjectDiscoveryIssue, ...]
    consumed_files: Mapping[str, str]
    project_fingerprint: str
    domain_dags: tuple[DiscoveredDomainDag, ...] = ()
    warnings: tuple[ProjectDiscoveryIssue, ...] = ()

    @property
    def ok(self) -> bool:
        return not self.issues


def build_discovery_snapshot(
    layout: ProjectLayout,
    workloads: list[DiscoveredWorkload],
    issues: list[ProjectDiscoveryIssue],
    consumed: Mapping[str, str],
    *,
    domain_dags: list[DiscoveredDomainDag] | None = None,
    warnings: list[ProjectDiscoveryIssue] | None = None,
) -> ProjectDiscoverySnapshot:
    """Canonicalize one scan result and fingerprint its semantic projection."""

    ordered = tuple(_freeze_workload(item) for item in sorted(workloads, key=lambda item: item.pipeline_id))
    material = [_project_identity_item(item) for item in ordered]
    fingerprint = compute_project_fingerprint(
        layout_mode=layout.mode,
        layout_root=layout.root,
        pipeline_id_scope=layout.pipeline_id_scope,
        workloads=material,
    )
    ordered_dags = tuple(
        sorted(domain_dags or (), key=lambda item: (item.dag_id, item.path)),
    )
    return ProjectDiscoverySnapshot(
        layout,
        ordered,
        tuple(issues),
        MappingProxyType(dict(sorted(consumed.items()))),
        fingerprint,
        ordered_dags,
        tuple(warnings or ()),
    )


def empty_discovery_snapshot(layout: ProjectLayout) -> ProjectDiscoverySnapshot:
    return build_discovery_snapshot(layout, [], [], {})


def _project_identity_item(workload: DiscoveredWorkload) -> dict[str, Any]:
    material = workload.identity_material()
    material["workload_fingerprint"] = workload.workload_fingerprint
    return material


def _freeze_workload(workload: DiscoveredWorkload) -> DiscoveredWorkload:
    checked = workload.checked_source
    compilation = _freeze_compilation(checked.compilation)
    frozen_checked = replace(
        checked,
        payload=_freeze_mapping(checked.payload),
        compilation=compilation,
    )
    return replace(workload, checked_source=frozen_checked)


def _freeze_compilation(compilation: AuthoringCompilation) -> AuthoringCompilation:
    return replace(
        compilation,
        canonical_manifest=_freeze_mapping(compilation.canonical_manifest),
        processes=tuple(_freeze_mapping(item) for item in compilation.processes),
        recipe_provenance=(
            _freeze_mapping(compilation.recipe_provenance) if compilation.recipe_provenance is not None else None
        ),
    )


def _freeze_mapping(value: Mapping[str, Any]) -> Mapping[str, Any]:
    return MappingProxyType({str(key): _freeze_value(item) for key, item in value.items()})


def _freeze_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return _freeze_mapping(value)
    if isinstance(value, list | tuple):
        return tuple(_freeze_value(item) for item in value)
    if isinstance(value, set | frozenset):
        return frozenset(_freeze_value(item) for item in value)
    if isinstance(value, bytearray):
        return bytes(value)
    return value


__all__ = [
    "DiscoveredWorkload",
    "ProjectDiscoveryIssue",
    "ProjectDiscoveryProjectionError",
    "ProjectDiscoverySnapshot",
    "build_discovery_snapshot",
    "empty_discovery_snapshot",
]
