"""Plan the complete producer-verified parent without reading credentials or SQL."""

from __future__ import annotations

from dpone.contracts.composition_control import (
    CompositionAdmissionError,
    CompositionExecutionPlan,
    CompositionSourceSnapshot,
    CompositionWorkloadAdmission,
    composition_generated_transfer_cell,
    composition_transfer_cell,
    dbt_relation_write_subject,
)
from dpone.manifest.bounded_yaml import load_bounded_yaml


def plan_composition_execution(sources: CompositionSourceSnapshot) -> CompositionExecutionPlan:
    """Preserve original native workflow ownership and every generated transfer."""
    sources.__post_init__()
    native_workflows = {row.source.workload_id: row for row in sources.native.workflows}
    native_ids = {workload for workload, _ in sources.native.required_workloads}
    transfers = dict(sources.transfer_manifests)
    planned = []
    for workload_id, pack_sha256 in sources.workload_pins:
        workflow = native_workflows.get(workload_id)
        if workflow is not None:
            if workflow.execution.profile.adapter_type not in {"mssql", "sqlserver"}:
                raise CompositionAdmissionError("native_execution_capability")
            writes = tuple(
                write
                for write in sources.native.relation_writes
                if write.kind != "transfer"
                and write.workflow_id == workflow.source.workflow_id
                and write.project_path == workflow.project.project_path
            )
            cell = "sqlserver_dbt_v1"
        else:
            body = transfers.get(workload_id)
            if body is None:
                raise CompositionAdmissionError("transfer_source_missing")
            manifest = load_bounded_yaml(body)
            if not isinstance(manifest, dict) or manifest.get("name") != workload_id:
                raise CompositionAdmissionError("transfer_source_identity")
            cell = (
                composition_generated_transfer_cell(manifest)
                if workload_id in native_ids
                else composition_transfer_cell(manifest)
            )
            writes = tuple(
                write
                for write in sources.relation_writes
                if write.kind == "transfer" and write.resource_id == workload_id
            )
        planned.append(
            CompositionWorkloadAdmission(
                workload_id=workload_id,
                constituent_id="native" if workload_id in native_ids else "standalone",
                pack_sha256=pack_sha256,
                execution_cell=cell,
                write_subjects=tuple(sorted(dbt_relation_write_subject(write) for write in writes)),
            )
        )
    return CompositionExecutionPlan(sources, tuple(planned), sources.relation_writes)
