"""Pure complete-workspace release assembly from verified framework observations."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, TypeVar

from dpone.contracts.airflow_deployment import is_canonical_sha256_digest
from dpone.contracts.dbt_contract_validation import artifact_json_bytes, sha256_bytes
from dpone.contracts.dbt_relation_writes import (
    DbtRelationWrite,
    require_distinct_logical_writes,
    selected_relation_writes,
    transfer_relation_write,
)
from dpone.contracts.dbt_release import build_dbt_release_metadata, dbt_release_artifact_descriptor
from dpone.contracts.dbt_runtime_payloads import (
    DBT_RUNTIME_WIRE_V2,
    MAX_DBT_RUNTIME_PAYLOAD_TOTAL_BYTES,
    MAX_DBT_RUNTIME_PAYLOADS,
    dbt_runtime_payload_read_limit,
    validate_dbt_runtime_payload_descriptor,
    validate_dbt_runtime_payload_trio,
)
from dpone.contracts.dbt_source_inventory import DbtProjectSource, DbtSourceInventory, DbtWorkflowSource
from dpone.contracts.dbt_source_inventory_binding import DbtSourcePlan
from dpone.contracts.strict_json import strict_json_object

if TYPE_CHECKING:
    from dpone.contracts.dbt_project_artifacts import DbtProjectArtifacts
    from dpone.contracts.dbt_workspace import DbtWorkspaceCheckReport, DbtWorkspaceProjectCheck

_T = TypeVar("_T")


@dataclass(frozen=True, slots=True)
class DbtWorkspaceReleaseTree:
    """Exact release bytes; not a signature, physical binding or live proof."""

    release_id: str
    inventory: DbtSourceInventory
    files: Mapping[str, bytes]

    def __post_init__(self) -> None:
        object.__setattr__(self, "files", MappingProxyType(dict(self.files)))


@dataclass(frozen=True, slots=True)
class DbtWorkspaceProjectArtifacts:
    """One project projection bound to its exact checked authoring inputs."""

    check: DbtWorkspaceProjectCheck
    artifacts: DbtProjectArtifacts


def assemble_dbt_workspace_release(
    check: DbtWorkspaceCheckReport,
    projects: Sequence[DbtWorkspaceProjectArtifacts],
    *,
    producer_version: str,
    pack_fingerprints: Mapping[str, str],
    schema_files: Mapping[str, bytes],
    schema_descriptors: list[dict[str, Any]],
) -> DbtWorkspaceReleaseTree:
    """Build once from all projects; reject omissions, collisions and preview authority.

    Pack fingerprints must come from the service's actual framework verification.
    This boundary validates metadata and supplied bytes, not SDK pack semantics.
    It does not read or publish directories. The writer
    must still run complete source-reader and integrity verification on an owned
    stage before its single atomic publication.
    """

    ordered = require_workspace_projects(check, projects)
    packs: dict[str, bytes] = {}
    dags: dict[str, bytes] = {}
    trios: dict[str, tuple[str, ...]] = {}
    certifications: dict[str, dict[str, Any]] = {}
    payloads = _RuntimeInventory()
    fingerprints: set[str] = set()
    authorities: set[str] = set()
    writes: list[DbtRelationWrite] = []
    for item in ordered:
        project = item.artifacts
        _merge_unique(packs, project.pack_files)
        _merge_unique(dags, project.dag_files)
        _merge_unique(trios, project.runtime_payload_ids)
        payloads.add(project)
        authorities.add(project.inputs.selection_authority)
        fingerprints.update(lock.selection_sha256 for lock in project.inputs.selection_locks.values())
        for certification in project.route_certifications(item.check.report):
            key = certification["variant_id"]
            previous = certifications.get(key)
            if previous is not None and previous != certification:
                raise ValueError("workspace route certification variant conflicts")
            certifications[key] = certification
        manifest = strict_json_object(project.inputs.manifest_bytes)
        for execution in project.inputs.execution_packs.values():
            writes.extend(
                selected_relation_writes(
                    project_path=item.check.project.project_path, execution=execution, manifest=manifest
                )
            )
        for model in item.check.report.models:
            writes.append(
                transfer_relation_write(
                    project_path=item.check.project.project_path,
                    workflow_id=model.intent.workflow,
                    workload_id=model.workload_id,
                    manifest=model.manifest,
                )
            )
    if authorities != {"dbt_cli"}:
        raise ValueError("workspace release requires dbt-authoritative selection for every project")
    require_distinct_logical_writes(writes)
    inventory = DbtSourceInventory.build(tuple(_source(item) for item in ordered))
    if set(pack_fingerprints) != set(packs) or any(
        not is_canonical_sha256_digest(value) for value in pack_fingerprints.values()
    ):
        raise ValueError("workspace pack fingerprint inventory is incomplete or invalid")
    pack_descriptors = []
    for key, body in sorted(packs.items()):
        descriptor = dbt_release_artifact_descriptor(key, f"packs/{key}.airflow-pack.json", body)
        descriptor["pack_fingerprint"] = pack_fingerprints[key]
        if key in trios:
            descriptor["runtime_payload_ids"] = list(trios[key])
        pack_descriptors.append(descriptor)
    release = build_dbt_release_metadata(
        dag_specs=[
            dbt_release_artifact_descriptor(key, f"dags/{key}.dag-spec.json", body)
            for key, body in sorted(dags.items())
        ],
        workload_packs=pack_descriptors,
        runtime_descriptors=[dict(payloads.descriptors[key]) for key in sorted(payloads.descriptors)],
        canonical_schema_descriptors=schema_descriptors,
        source_snapshot_sha256=inventory.snapshot_sha256,
        selection_authority="dbt_cli",
        route_certifications=[certifications[key] for key in sorted(certifications)],
        selection_fingerprints=sorted(fingerprints),
        producer_version=producer_version,
        wire_contract=DBT_RUNTIME_WIRE_V2,
    )
    files = {
        "release-set.json": artifact_json_bytes(release),
        "_dbt/dbt-source-snapshot.json": artifact_json_bytes(inventory.to_dict()),
    }
    _merge_unique(files, {f"packs/{key}.airflow-pack.json": body for key, body in packs.items()})
    _merge_unique(files, {f"dags/{key}.dag-spec.json": body for key, body in dags.items()})
    _merge_unique(files, schema_files)
    _merge_unique(files, payloads.files)
    plan = DbtSourcePlan.from_release(release, inventory, expected_release_id=release["release_id"])
    if set(files) != set(plan.artifacts.by_path) | {"release-set.json", "_dbt/dbt-source-snapshot.json"}:
        raise ValueError("workspace release file inventory contains missing or orphan bytes")
    for path in plan.artifacts.by_path:
        plan.artifacts.require_bytes(path, files[path])
    return DbtWorkspaceReleaseTree(release["release_id"], inventory, files)


def require_workspace_projects(
    check: DbtWorkspaceCheckReport, projects: Sequence[DbtWorkspaceProjectArtifacts]
) -> tuple[DbtWorkspaceProjectArtifacts, ...]:
    """Require complete checked membership before any external pack verification."""

    if not check.passed or not 0 < len(projects) <= 64:
        raise ValueError("workspace assembly requires a complete nonempty successful check")
    expected = {row.project.project_path: row for row in check.projects}
    discovered = {row.project_path: row for row in check.discovery.projects if row.publishing}
    paths = [item.check.project.project_path for item in projects]
    if (
        len(expected) != len(check.projects)
        or len(paths) != len(set(paths))
        or len(discovered) != sum(row.publishing for row in check.discovery.projects)
    ):
        raise ValueError("workspace project identities must be unique")
    if set(paths) != set(expected) or set(expected) != set(discovered):
        raise ValueError("workspace projections must cover the complete publishing inventory")
    for item in projects:
        path = item.check.project.project_path
        if item.check != expected[path] or item.check.project != discovered[path]:
            raise ValueError("workspace projection differs from its checked project")
    seen_packs: set[str] = set()
    seen_dags: set[str] = set()
    for item in projects:
        _require_project_membership(item)
        if seen_packs & item.artifacts.pack_files.keys() or seen_dags & item.artifacts.dag_files.keys():
            raise ValueError("workspace artifact identity collision")
        seen_packs.update(item.artifacts.pack_files)
        seen_dags.update(item.artifacts.dag_files)
    return tuple(sorted(projects, key=lambda item: item.check.project.project_path))


def _source(item: DbtWorkspaceProjectArtifacts) -> DbtProjectSource:
    project, inputs = item.check.project, item.artifacts.inputs
    workflows = []
    for workflow in sorted(item.check.report.workflows, key=lambda row: row.workflow):
        ids = item.artifacts.payloads.workflow_ids[workflow.workflow]
        descriptor = next(row for row in item.artifacts.payloads.descriptors if row["id"] == ids[2])
        workflows.append(
            DbtWorkflowSource(
                workflow.workflow, workflow.dag_id, f"dbt__{workflow.workflow}", ids, str(descriptor["sha256"])
            )
        )
    if project.project_name is None:
        raise ValueError("workspace project name is missing")
    return DbtProjectSource(
        project.project_path,
        project.project_name,
        inputs.project_sha256,
        sha256_bytes(inputs.manifest_bytes),
        inputs.toolchain_sha256,
        tuple(workflows),
    )


def _require_project_membership(item: DbtWorkspaceProjectArtifacts) -> None:
    report, artifacts = item.check.report, item.artifacts
    workflows = {row.workflow for row in report.workflows}
    dbt_packs = {f"dbt__{workflow}" for workflow in workflows}
    if (
        not report.passed
        or not report.models
        or any(model.profile.semantic_refresh is not None for model in report.models)
        or set(artifacts.pack_files) != dbt_packs | {model.workload_id for model in report.models}
        or set(artifacts.dag_files) != {workflow.dag_id for workflow in report.workflows}
        or set(artifacts.runtime_payload_ids) != dbt_packs
        or set(artifacts.payloads.workflow_ids) != workflows
        or set(artifacts.inputs.selection_locks) != workflows
        or set(artifacts.inputs.execution_packs) != workflows
        or sha256_bytes(artifacts.inputs.manifest_bytes) != report.manifest_sha256
    ):
        raise ValueError("workspace projection membership differs from checked project")
    for workflow in workflows:
        validate_dbt_runtime_payload_trio(artifacts.payloads.workflow_ids[workflow], wire_contract=DBT_RUNTIME_WIRE_V2)
        if artifacts.runtime_payload_ids[f"dbt__{workflow}"] != artifacts.payloads.workflow_ids[workflow]:
            raise ValueError("workspace project workflow payload membership differs")


class _RuntimeInventory:
    """Accumulate only canonical bytes; charge resource limits after exact deduplication."""

    def __init__(self) -> None:
        self.files: dict[str, bytes] = {}
        self.descriptors: dict[str, Mapping[str, object]] = {}
        self.total_bytes = 0

    def add(self, project: DbtProjectArtifacts) -> None:
        seen: set[str] = set()
        paths: set[str] = set()
        for descriptor in project.payloads.descriptors:
            reference = validate_dbt_runtime_payload_descriptor(descriptor, wire_contract=DBT_RUNTIME_WIRE_V2)
            if reference.id in seen:
                raise ValueError("project runtime inventory contains duplicate identities")
            seen.add(reference.id)
            paths.add(reference.path)
            body = project.runtime_files.get(reference.path)
            if body is None or reference.descriptor(body) != descriptor:
                raise ValueError("project runtime inventory differs from its bytes")
            if len(body) > dbt_runtime_payload_read_limit(reference.kind):
                raise ValueError("project runtime object exceeds its reader bound")
            previous = self.descriptors.get(reference.id)
            if previous is not None:
                if previous != descriptor or self.files[reference.path] != body:
                    raise ValueError("workspace runtime identity maps to conflicting content")
                continue
            if (
                len(self.descriptors) >= MAX_DBT_RUNTIME_PAYLOADS
                or self.total_bytes + len(body) > MAX_DBT_RUNTIME_PAYLOAD_TOTAL_BYTES
            ):
                raise ValueError("workspace runtime aggregate bound exceeded")
            self.descriptors[reference.id] = descriptor
            self.files[reference.path] = body
            self.total_bytes += len(body)
        if paths != set(project.runtime_files) or seen != {
            key for ids in project.payloads.workflow_ids.values() for key in ids
        }:
            raise ValueError("project runtime inventory membership is incomplete or contains orphans")


def _merge_unique(target: dict[str, _T], incoming: Mapping[str, _T]) -> None:
    if target.keys() & incoming.keys():
        raise ValueError("workspace artifact identity collision")
    target.update(incoming)


__all__ = [
    "DbtWorkspaceProjectArtifacts",
    "DbtWorkspaceReleaseTree",
    "assemble_dbt_workspace_release",
    "require_workspace_projects",
]
