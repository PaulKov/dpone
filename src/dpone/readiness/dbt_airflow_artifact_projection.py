"""Pure Airflow pack and DAG policy projections for compiled dbt workflows."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from dpone.contracts.dbt_contract_validation import (
    DbtPublishingError,
    canonical_fingerprint,
)
from dpone.contracts.dbt_semantic_refresh_plan_contracts import (
    SemanticRefreshPreReleaseProofBundle,
)
from dpone.contracts.dbt_semantic_refresh_project_overlay import (
    semantic_refresh_project_overlay,
)
from dpone.gitops.airflow_dag_spec import compute_spec_fingerprint
from dpone.gitops.workload_catalog_models import GitOpsWorkloadDefinition

if TYPE_CHECKING:
    from dpone.contracts.dbt_publish_models import (
        CompiledDbtModel,
        CompiledDbtWorkflow,
        DbtCompileReport,
    )


def workload_definition(
    model: CompiledDbtModel,
    manifest_path: str,
) -> GitOpsWorkloadDefinition:
    if model.profile.semantic_refresh is not None:
        raise DbtPublishingError(
            "DPONE_DBT_V2_GENERIC_PACK_FORBIDDEN",
            "semantic refresh requires its dedicated post-deployment projection",
            path=model.model.original_file_path,
            remediation="Activate the release through the protected semantic-refresh plan compiler.",
        )
    runtime = dict(model.profile.runtime)
    effective = dict(runtime)
    effective["image"] = model.profile.runtime_image
    effective.setdefault("namespace", "default")
    # Defaults belong to the projected workload, not the checked profile.
    effective["airflow"] = dict(runtime.get("airflow", {}))
    effective["airflow"].setdefault("execution", _execution_policy(model))
    return GitOpsWorkloadDefinition(
        workload_id=model.workload_id,
        manifest=manifest_path,
        domain=model.model.group,
        catalog_path=model.profile.name,
        effective_config=effective,
        provenance={},
    )


def semantic_refresh_topology_template(
    workflow: CompiledDbtWorkflow,
) -> dict[str, Any]:
    """Build a deployment-neutral topology consumed only after protected admission."""

    if not workflow.models or any(item.profile.semantic_refresh is None for item in workflow.models):
        raise DbtPublishingError(
            "DPONE_DBT_V2_TOPOLOGY_INVALID",
            "semantic-refresh topology requires one non-empty exact V2 model closure",
        )
    profile_digests = {
        item.profile.semantic_refresh.profile_sha256
        for item in workflow.models
        if item.profile.semantic_refresh is not None
    }
    if len(profile_digests) != 1:
        raise DbtPublishingError(
            "DPONE_DBT_V2_TOPOLOGY_INVALID",
            "semantic-refresh topology mixes platform profile authorities",
        )
    by_id = {item.model.unique_id: item for item in workflow.models}
    if len(by_id) != len(workflow.models):
        raise DbtPublishingError(
            "DPONE_DBT_V2_TOPOLOGY_INVALID",
            "semantic-refresh topology contains duplicate model identities",
        )
    dependencies = {
        model_id: tuple(sorted(parent for parent in item.model.depends_on if parent in by_id))
        for model_id, item in sorted(by_id.items())
    }
    ordered = _topological_model_ids(dependencies)
    outputs = {model_id: _semantic_output_asset_uri(by_id[model_id]) for model_id in sorted(by_id)}
    unsigned: dict[str, Any] = {
        "activation": "POST_DEPLOYMENT_AUTHORITY_REQUIRED",
        "dag_id": workflow.dag_id,
        "dag_policy": _semantic_dag_policy(workflow),
        "dependencies": {key: list(value) for key, value in dependencies.items()},
        "logical_output_asset_uris": outputs,
        "model_unique_ids": list(ordered),
        "profile_sha256": profile_digests.pop(),
        "project_config_overlay": semantic_refresh_project_overlay(tuple(item.model.fqn for item in workflow.models)),
        "schema": "dpone.dbt-semantic-refresh-topology-template.v1",
        "workflow_name": workflow.workflow,
    }
    return {**unsigned, "topology_sha256": canonical_fingerprint(unsigned)}


def _semantic_dag_policy(workflow: CompiledDbtWorkflow) -> dict[str, Any]:
    profile = workflow.profile
    max_active_tasks = min(_parallelism(item) for item in workflow.models)
    tags = sorted({"dbt", "dpone", "semantic-refresh-v2", *profile.tags})
    if (
        (profile.schedule is not None and (not isinstance(profile.schedule, str) or not profile.schedule.strip()))
        or not isinstance(profile.start_date, str)
        or not profile.start_date.strip()
        or not isinstance(profile.timezone, str)
        or not profile.timezone.strip()
        or not isinstance(profile.owner, str)
        or not profile.owner.strip()
        or not isinstance(profile.catchup, bool)
        or isinstance(profile.max_active_runs, bool)
        or not isinstance(profile.max_active_runs, int)
        or profile.max_active_runs <= 0
        or isinstance(max_active_tasks, bool)
        or not isinstance(max_active_tasks, int)
        or max_active_tasks <= 0
        or any(not isinstance(tag, str) or not tag.strip() for tag in tags)
    ):
        raise DbtPublishingError(
            "DPONE_DBT_V2_TOPOLOGY_INVALID",
            "semantic-refresh DAG policy must be explicit and bounded",
        )
    return {
        "catchup": profile.catchup,
        "max_active_runs": profile.max_active_runs,
        "max_active_tasks": max_active_tasks,
        "owner": profile.owner,
        "schedule": profile.schedule,
        "start_date": profile.start_date,
        "tags": tags,
        "timezone": profile.timezone,
    }


def dag_spec(workflow: CompiledDbtWorkflow) -> dict[str, Any]:
    dbt_node = f"dbt__{workflow.workflow}"
    workload_ids = tuple(item.workload_id for item in workflow.models)
    nodes = [
        {
            "node_id": dbt_node,
            "workload_id": dbt_node,
            "pack_ref": f"cached://workloads/{dbt_node}",
        },
        *(
            {
                "node_id": item.workload_id,
                "workload_id": item.workload_id,
                "pack_ref": f"cached://workloads/{item.workload_id}",
            }
            for item in workflow.models
        ),
    ]
    max_parallel = min(_parallelism(item) for item in workflow.models)
    edges = [
        {
            "upstream": dbt_node,
            "downstream": workload_id,
            "reason": "curated",
            "origin": "dbt.workflow",
        }
        for workload_id in workload_ids
    ]
    payload: dict[str, Any] = {
        "kind": "gitops.airflow_dag_spec",
        "schema_version": "1",
        "producer": "dpone dbt compile",
        "dag_id": workflow.dag_id,
        "domain": workflow.models[0].model.group,
        "description": (f"Generated dbt -> dpone workflow {workflow.workflow}; do not edit"),
        "schedule": workflow.profile.schedule,
        "start_date": workflow.profile.start_date,
        "timezone": workflow.profile.timezone,
        "catchup": workflow.profile.catchup,
        "max_active_runs": workflow.profile.max_active_runs,
        "max_active_tasks": max_parallel,
        "tags": sorted({"dbt", "dpone", *workflow.profile.tags}),
        "default_args": {"owner": workflow.profile.owner, "retries": 0},
        "source": {"type": "dbt", "workflow": workflow.workflow},
        "wiring": {
            "mode": "explicit",
            "max_parallel_workloads": max_parallel,
            "dependencies": {workload_id: [dbt_node] for workload_id in workload_ids},
        },
        "nodes": nodes,
        "edges": edges,
        "topological_order": [dbt_node, *workload_ids],
        "workflow_outcome": {
            "schema": "dpone.dbt-workflow-outcome.v1",
            "task_id": "workflow_outcome",
            "workflow_id": workflow.workflow,
            "expected_terminal_task_ids": [f"{workload_id}__outcome_gate" for workload_id in sorted(workload_ids)],
        },
        "warnings": [],
    }
    payload["spec_fingerprint"] = compute_spec_fingerprint(payload)
    return payload


def dbt_pool(workflow: CompiledDbtWorkflow) -> str:
    model = workflow.models[0]
    configured = str(model.profile.execution.get("dbt_pool") or "").strip()
    if configured:
        return configured
    unique_id_parts = model.model.unique_id.split(".")
    project_id = unique_id_parts[1] if len(unique_id_parts) > 2 else "project"
    target_id = str(model.profile.runtime.get("dbt_target") or "runtime")
    return f"dpone_dbt__{_pool_segment(project_id)}__{_pool_segment(target_id)}"


def xcom_sidecar_image(model: CompiledDbtModel) -> str:
    value = str(model.profile.runtime.get("xcom_sidecar_image") or "").strip()
    if not value:
        raise ValueError("dbt publish runtime requires xcom_sidecar_image")
    return value


def optional_mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def route_certifications(report: DbtCompileReport) -> list[dict[str, Any]]:
    by_variant: dict[str, dict[str, Any]] = {}
    for model in report.models:
        capability = dict(model.route_capability)
        if model.profile.semantic_refresh is not None:
            projected = _semantic_route_certification(model, capability)
            variant_id = str(projected["variant_id"])
            existing = by_variant.get(variant_id)
            if existing is not None and existing != projected:
                raise ValueError("route certification variant identity maps to conflicting evidence")
            by_variant[variant_id] = projected
            continue
        route_id = capability.get("route_id")
        if not isinstance(route_id, str) or not route_id:
            raise ValueError("compiled dbt model lacks route capability identity")
        regular_variant_id = capability.get("variant_id")
        if not isinstance(regular_variant_id, str) or not regular_variant_id:
            raise ValueError("compiled dbt model lacks exact route certification variant")
        projected = {
            "variant_id": regular_variant_id,
            "route_id": route_id,
            "transport": capability.get("transport"),
            "schema_evolution": capability.get("schema_evolution"),
            "airflow_runtime_mode": capability.get("airflow_runtime_mode"),
            "snapshot_id": capability.get("snapshot_id"),
            "support": capability.get("support"),
            "certification_level": capability.get("certification_level"),
            "evidence_status": capability.get("evidence_status"),
            "evidence_refs": capability.get("evidence_refs", []),
            "evidence_reason_codes": capability.get("evidence_reason_codes", []),
        }
        existing = by_variant.get(regular_variant_id)
        if existing is not None and existing != projected:
            raise ValueError("route certification variant identity maps to conflicting evidence")
        by_variant[regular_variant_id] = projected
    return [by_variant[key] for key in sorted(by_variant)]


def _semantic_route_certification(
    model: CompiledDbtModel,
    capability: dict[str, Any],
) -> dict[str, Any]:
    coordinate = capability.get("certification_coordinate_sha256")
    receipt = capability.get("certification_receipt_sha256")
    if capability.get("status") != "CERTIFIED" or not isinstance(coordinate, str) or not isinstance(receipt, str):
        raise DbtPublishingError(
            "DPONE_DBT_V2_LIVE_UNVERIFIED",
            "semantic-refresh release lacks an exact protected live certification receipt",
            path=model.model.original_file_path,
        )
    return {
        "certification_level": "exact_live",
        "evidence_refs": [receipt],
        "evidence_status": "certified",
        "route_id": "semantic_refresh_v2",
        "variant_id": coordinate,
    }


def _topological_model_ids(
    dependencies: dict[str, tuple[str, ...]],
) -> tuple[str, ...]:
    pending = {key: set(value) for key, value in dependencies.items()}
    ordered: list[str] = []
    while pending:
        ready = sorted(key for key, parents in pending.items() if not parents)
        if not ready:
            raise DbtPublishingError(
                "DPONE_DBT_V2_TOPOLOGY_INVALID",
                "semantic-refresh selected-model dependency graph contains a cycle",
            )
        ordered.extend(ready)
        for key in ready:
            pending.pop(key)
        for parents in pending.values():
            parents.difference_update(ready)
    return tuple(ordered)


def _semantic_output_asset_uri(model: CompiledDbtModel) -> str:
    target_schema = model.intent.target_schema or model.profile.target_schema
    target_table = model.intent.target_table or model.model.alias
    return f"{model.profile.sink_type}://{target_schema}/{target_table}"


def is_semantic_refresh_workflow(workflow: Any) -> bool:
    """Reject mixed workflows and report whether all models use V2."""

    flags = tuple(model.profile.semantic_refresh is not None for model in workflow.models)
    if any(flags) and not all(flags):
        raise DbtPublishingError(
            "DPONE_DBT_V2_TOPOLOGY_INVALID",
            "one workflow cannot mix semantic-refresh and generic transfer models",
        )
    return bool(flags and flags[0])


def semantic_refresh_pre_release_bundles(
    report: DbtCompileReport,
    values: Mapping[str, SemanticRefreshPreReleaseProofBundle] | None,
) -> dict[str, SemanticRefreshPreReleaseProofBundle]:
    """Require one exact typed pre-release proof for every V2 workflow."""

    semantic_ids = tuple(sorted(item.workflow for item in report.workflows if is_semantic_refresh_workflow(item)))
    if not semantic_ids:
        if values:
            raise DbtPublishingError(
                "DPONE_DBT_V2_PROOF_INVALID",
                "generic dbt release cannot accept semantic-refresh proof bundles",
            )
        return {}
    if not isinstance(values, Mapping) or tuple(sorted(values)) != semantic_ids:
        raise DbtPublishingError(
            "DPONE_DBT_V2_PROOF_UNVERIFIED",
            "semantic-refresh release requires one exact typed pre-release proof per workflow",
        )
    result = dict(values)
    if any(
        not isinstance(item, SemanticRefreshPreReleaseProofBundle) or item.workflow_name != workflow_id
        for workflow_id, item in result.items()
    ):
        raise DbtPublishingError(
            "DPONE_DBT_V2_PROOF_INVALID",
            "semantic-refresh pre-release proof workflow closure is invalid",
        )
    return result


def _parallelism(model: CompiledDbtModel) -> int:
    return model.intent.max_parallelism or int(model.profile.execution.get("max_parallelism", 2) or 2)


def _pool_segment(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("._-")
    return normalized or "default"


def dbt_task_executor(model: CompiledDbtModel) -> str:
    """Return the declared task executor policy with the contract default."""

    return str(dict(model.profile.execution).get("task_executor") or "KubernetesExecutor")


def _execution_policy(model: CompiledDbtModel) -> dict[str, Any]:
    execution = dict(model.profile.execution)
    return {
        "task_executor": dbt_task_executor(model),
        "deferrable": bool(execution.get("deferrable", True)),
        "on_finish_action": str(execution.get("on_finish_action") or "delete_succeeded_pod"),
        "get_logs": bool(execution.get("get_logs", True)),
        "logging_interval_seconds": int(execution.get("logging_interval_seconds", 60)),
    }


__all__ = [
    "dag_spec",
    "dbt_pool",
    "dbt_task_executor",
    "is_semantic_refresh_workflow",
    "optional_mapping",
    "route_certifications",
    "semantic_refresh_pre_release_bundles",
    "semantic_refresh_topology_template",
    "workload_definition",
    "xcom_sidecar_image",
]
