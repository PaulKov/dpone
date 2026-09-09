"""Canonical lexical resolution for one primary pipeline authoring source."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from types import MappingProxyType

from dpone.manifest.confined_files import ConfinedFileError, project_relative_path
from dpone.manifest.domain_identity import DomainId, DomainIdError
from dpone.manifest.pipeline_identity import PipelineId, PipelineIdError
from dpone.manifest.project_config import (
    ProjectConfigError,
    ProjectConfigSnapshot,
    ProjectLayout,
    load_project_layout,
)
from dpone.manifest.project_discovery import ProjectDiscoveryService, ProjectDiscoverySnapshot
from dpone.manifest.project_layout_authority import conflicting_authoring_layout
from dpone.manifest.project_selection_contracts import ProjectCheckedSource


class PipelineSourceReferenceError(ValueError):
    """A pipeline id or explicit source reference is unsafe or ambiguous."""

    def __init__(
        self,
        message: str,
        *,
        code: str = "DPONE_PIPELINE_SOURCE_PATH_INVALID",
        exit_code: int = 4,
        entity_id: str | None = None,
        domain: str | None = None,
        path: str | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.exit_code = exit_code
        self.entity_id = entity_id
        self.domain = domain
        self.path = path


@dataclass(frozen=True, slots=True)
class ResolvedPipelineReference:
    """Canonical source path plus the identity asserted by a bare reference."""

    relative_path: str
    requested_id: PipelineId | None
    kind: str
    checked_source: ProjectCheckedSource | None = None
    workload_fingerprint: str | None = None
    consumed_files: Mapping[str, str] = field(default_factory=lambda: MappingProxyType({}))


def resolve_pipeline_source_relative(root: Path, target: str | Path) -> str:
    """Resolve an id or explicit path without basename fallback or symlink traversal."""

    return resolve_pipeline_reference(root, target).relative_path


def resolve_pipeline_reference(
    root: Path,
    target: str | Path,
    *,
    layout_snapshot: ProjectLayout | None = None,
    config_snapshot: ProjectConfigSnapshot | None = None,
    discovery_snapshot: ProjectDiscoverySnapshot | None = None,
) -> ResolvedPipelineReference:
    """Resolve one lexical reference while retaining a bare requested identity."""

    if root.is_symlink():
        raise PipelineSourceReferenceError("Project root must not be a symbolic link.")
    root_path = root.resolve(strict=True)
    raw_text = os.fspath(target)
    raw = Path(target)
    parsed = PurePosixPath(raw.as_posix())
    if (
        not raw_text
        or raw_text == "."
        or "\x00" in raw_text
        or "\\" in raw_text
        or ".." in parsed.parts
        or "." in parsed.parts
    ):
        raise PipelineSourceReferenceError("Pipeline source path must be a normalized project path.")

    if layout_snapshot is None:
        layout, loaded_config = _layout(root_path)
        config_snapshot = loaded_config
    else:
        layout = layout_snapshot
    conflicting_layout = conflicting_authoring_layout(
        root_path,
        expected_mode=layout.mode,
        domain_first_root=layout.root,
    )
    if conflicting_layout is not None:
        raise PipelineSourceReferenceError(
            "Existing authoring authority does not match the configured project layout.",
            code="DPONE_PROJECT_LAYOUT_MIGRATION_REQUIRED",
            exit_code=1,
        )
    if discovery_snapshot is None and layout.is_domain_first:
        discovery_snapshot = ProjectDiscoveryService(root_path).discover(layout=layout)
    _require_valid_discovery(discovery_snapshot)
    candidate, requested_id, kind = _candidate(
        raw,
        parsed,
        layout=layout,
        snapshot=discovery_snapshot,
    )
    try:
        relative_path = project_relative_path(root_path, candidate)
    except ConfinedFileError as exc:
        raise PipelineSourceReferenceError(
            "Pipeline source must stay inside the project root and must not traverse parent paths."
        ) from exc
    checked_source = _checked_source(discovery_snapshot, relative_path)
    workload_fingerprint = _workload_fingerprint(discovery_snapshot, relative_path)
    consumed = _consumed_files(discovery_snapshot, config_snapshot)
    return ResolvedPipelineReference(
        relative_path=relative_path,
        requested_id=requested_id,
        kind=kind,
        checked_source=checked_source,
        workload_fingerprint=workload_fingerprint,
        consumed_files=MappingProxyType(consumed),
    )


def _candidate(
    raw: Path,
    parsed: PurePosixPath,
    *,
    layout: ProjectLayout,
    snapshot: ProjectDiscoverySnapshot | None,
) -> tuple[Path, PipelineId | None, str]:
    if raw.is_absolute():
        candidate = raw if raw.suffix in {".yaml", ".yml"} or raw.name == "pipeline.yaml" else raw / "pipeline.yaml"
        return candidate, None, "file" if candidate == raw else "directory"
    if raw.suffix in {".yaml", ".yml"} or raw.name == "pipeline.yaml":
        return raw, None, "file"
    if len(parsed.parts) == 1:
        try:
            pipeline_id = PipelineId.parse(raw.as_posix())
        except PipelineIdError as exc:
            raise PipelineSourceReferenceError(str(exc)) from exc
        return _bare_id_candidate(layout, snapshot, pipeline_id), pipeline_id, "id"
    if len(parsed.parts) == 2:
        domain_candidate = _domain_shorthand_candidate(layout, parsed.parts[0], parsed.parts[1])
        if domain_candidate is not None:
            return domain_candidate
    return raw / "pipeline.yaml", None, "directory"


def _bare_id_candidate(
    layout: ProjectLayout,
    snapshot: ProjectDiscoverySnapshot | None,
    pipeline_id: PipelineId,
) -> Path:
    flat = Path("pipelines") / str(pipeline_id) / "pipeline.yaml"
    if not layout.is_domain_first:
        return flat
    assert snapshot is not None
    matches = [
        workload.checked_source.source_path
        for workload in snapshot.workloads
        if workload.pipeline_id == str(pipeline_id)
    ]
    if matches:
        return matches[0]
    return Path(layout.root) / "_unknown" / "pipelines" / str(pipeline_id) / "pipeline.yaml"


def _domain_shorthand_candidate(
    layout: ProjectLayout,
    raw_domain: str,
    raw_pipeline_id: str,
) -> tuple[Path, PipelineId, str] | None:
    if not layout.is_domain_first:
        return None
    try:
        domain = DomainId.parse(raw_domain)
        pipeline_id = PipelineId.parse(raw_pipeline_id)
    except (DomainIdError, PipelineIdError) as exc:
        raise PipelineSourceReferenceError(str(exc)) from exc
    return (
        Path(layout.root) / str(domain) / "pipelines" / str(pipeline_id) / "pipeline.yaml",
        pipeline_id,
        "domain_id",
    )


def _layout(root: Path) -> tuple[ProjectLayout, ProjectConfigSnapshot | None]:
    try:
        return load_project_layout(root)
    except ProjectConfigError as exc:
        raise PipelineSourceReferenceError(
            "Project layout configuration is invalid.",
            code="DPONE_PROJECT_CONFIG_INVALID",
            exit_code=2,
        ) from exc


def _require_valid_discovery(snapshot: ProjectDiscoverySnapshot | None) -> None:
    if snapshot is None or not snapshot.issues:
        return
    issue = snapshot.issues[0]
    location = f" ({issue.path})" if issue.path else ""
    raise PipelineSourceReferenceError(
        f"{issue.message}{location}",
        code=issue.code,
        exit_code=4 if issue.code in {"DPONE_DISCOVERY_PATH_INVALID", "DPONE_LAYOUT_ROOT_INVALID"} else 1,
        entity_id=issue.pipeline_id or issue.domain,
        domain=issue.domain,
        path=issue.path,
    )


def _checked_source(
    snapshot: ProjectDiscoverySnapshot | None,
    relative_path: str,
) -> ProjectCheckedSource | None:
    if snapshot is None:
        return None
    matches = [
        workload.checked_source
        for workload in snapshot.workloads
        if workload.checked_source.source_label == relative_path
    ]
    if len(matches) != 1:
        raise PipelineSourceReferenceError(
            "Pipeline source is not an authoritative domain-first primary source.",
            code="DPONE_PIPELINE_SOURCE_NOT_FOUND",
            exit_code=1,
        )
    return matches[0]


def _workload_fingerprint(
    snapshot: ProjectDiscoverySnapshot | None,
    relative_path: str,
) -> str | None:
    if snapshot is None:
        return None
    matches = [
        workload.workload_fingerprint
        for workload in snapshot.workloads
        if workload.checked_source.source_label == relative_path
    ]
    if len(matches) != 1:
        raise PipelineSourceReferenceError(
            "Pipeline source is not an authoritative domain-first primary source.",
            code="DPONE_PIPELINE_SOURCE_NOT_FOUND",
            exit_code=1,
        )
    return matches[0]


def _consumed_files(
    snapshot: ProjectDiscoverySnapshot | None,
    config_snapshot: ProjectConfigSnapshot | None,
) -> dict[str, str]:
    consumed = dict(snapshot.consumed_files) if snapshot is not None else {}
    if config_snapshot is not None:
        consumed["dpone.yaml"] = config_snapshot.sha256
    return dict(sorted(consumed.items()))


__all__ = [
    "PipelineSourceReferenceError",
    "ResolvedPipelineReference",
    "resolve_pipeline_reference",
    "resolve_pipeline_source_relative",
]
