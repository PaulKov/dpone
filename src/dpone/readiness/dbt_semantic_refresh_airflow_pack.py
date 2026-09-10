"""Build and validate dedicated semantic-refresh Airflow pack artifacts."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from dpone_airflow_pack.pack_identity import PACK_IDENTITY_SCHEMA, compute_pack_fingerprint
from dpone_airflow_pack.semantic_refresh_projection import (
    SemanticRefreshTaskProjection,
    build_semantic_refresh_task_projection,
)

from dpone.contracts.dbt_publishing import DbtExecutionPack
from dpone.readiness.dbt_semantic_refresh_airflow_validation import (
    SemanticRefreshTemplateProofAuthority,
)
from dpone.readiness.dbt_semantic_refresh_airflow_validation import (
    protected_template as _protected_template,
)
from dpone.readiness.dbt_semantic_refresh_airflow_validation import (
    validate_activation_inputs as _validate_activation_inputs,
)
from dpone.readiness.dbt_semantic_refresh_airflow_validation import (
    validate_plan_run_template as _validate_plan_run_template,
)
from dpone.readiness.dbt_semantic_refresh_airflow_validation import (
    validate_plan_template as _validate_plan_template,
)
from dpone.readiness.dbt_semantic_refresh_airflow_validation import (
    validate_pre_release_template as _validate_pre_release_template,
)

if TYPE_CHECKING:
    from dpone.contracts.dbt_semantic_refresh_activation import (
        SemanticRefreshActivationAuthorityReceipt,
    )
    from dpone.contracts.dbt_semantic_refresh_plan_contracts import SemanticRefreshPlanBundle
    from dpone.contracts.dbt_semantic_refresh_run_contracts import (
        SemanticRefreshRunExecutionBundle,
    )


@dataclass(frozen=True, slots=True)
class ActivatedSemanticRefreshPack:
    """Run-bound dedicated pack plus its authenticated task projection."""

    payload: Mapping[str, Any]
    task_projection: SemanticRefreshTaskProjection
    dbt_execution_pack: DbtExecutionPack

    def to_dict(self) -> dict[str, Any]:
        return dict(self.payload)


def semantic_refresh_template_pack(
    *,
    workflow_id: str,
    execution_pack: DbtExecutionPack,
    runtime_payload_ids: tuple[str, ...],
    topology: Mapping[str, Any],
    proof_authority: SemanticRefreshTemplateProofAuthority,
) -> dict[str, Any]:
    """Bind the non-executable V2 topology and dbt gate before deployment."""

    _validate_pre_release_template(execution_pack, topology, proof_authority)
    if execution_pack.workflow_id != workflow_id or topology.get("workflow_name") != workflow_id:
        raise ValueError("semantic-refresh topology and dbt execution workflow differ")
    if topology.get("activation") != "POST_DEPLOYMENT_AUTHORITY_REQUIRED":
        raise ValueError("semantic-refresh topology is not deployment-gated")
    payload: dict[str, Any] = {
        "activation": "POST_DEPLOYMENT_AUTHORITY_REQUIRED",
        "dbt_execution_pack": execution_pack.to_dict(),
        "executable": False,
        "kind": "gitops.airflow_pack",
        "pack_identity": {"schema": PACK_IDENTITY_SCHEMA},
        "producer": "dpone dbt compile",
        "runtime_command": "",
        "runtime_payload_ids": list(runtime_payload_ids),
        "schema_version": "3",
        "semantic_refresh": {
            "mode": "semantic_refresh_v2_template",
            "package_artifacts_sha256": proof_authority.package_artifacts_sha256,
            "pre_release_bundle_sha256": proof_authority.pre_release_bundle_sha256,
            "topology": dict(topology),
        },
        "workload": {
            "effective_config": {"authoring": {"mode": "semantic_refresh_v2_template"}},
            "workload_id": f"semantic__{workflow_id}",
        },
    }
    payload["pack_fingerprint"] = compute_pack_fingerprint(payload)
    return payload


def semantic_refresh_activated_pack(
    *,
    template_pack: Mapping[str, Any],
    plan_bundle: SemanticRefreshPlanBundle,
    run_execution: SemanticRefreshRunExecutionBundle,
    authority_receipt: SemanticRefreshActivationAuthorityReceipt,
) -> ActivatedSemanticRefreshPack:
    """Activate one release template only after protected deployment/run authority."""

    topology, execution_pack, pre_release_sha256, package_sha256 = _protected_template(template_pack)
    _validate_activation_inputs(
        plan_bundle,
        run_execution,
        authority_receipt,
        topology,
        execution_pack,
        pre_release_sha256,
        package_sha256,
    )
    operations = {item.model_unique_id: item.operation_id for item in plan_bundle.operation_plans}
    execution_binding = run_execution.workflow_execution_binding.to_dict()
    projection = build_semantic_refresh_task_projection(
        execution_binding=execution_binding,
        topology=dict(template_pack["semantic_refresh"]["topology"]),
        operation_ids_by_model=operations,
    )
    payload: dict[str, Any] = {
        "activation": "PROTECTED_RUN_AUTHORITY_BOUND",
        "dbt_execution_pack": execution_pack.to_dict(),
        "executable": True,
        "kind": "gitops.airflow_pack",
        "pack_identity": {"schema": PACK_IDENTITY_SCHEMA},
        "producer": "dpone semantic-refresh activate",
        "runtime_command": "",
        "runtime_payload_ids": list(template_pack["runtime_payload_ids"]),
        "schema_version": "3",
        "semantic_refresh": {
            "activation_authority": authority_receipt.to_dict(),
            "execution_binding": execution_binding,
            "mode": "semantic_refresh_v2_activated",
            "operation_ids_by_model": dict(sorted(operations.items())),
            "package_artifacts_sha256": package_sha256,
            "plan_bundle_sha256": plan_bundle.plan_bundle_sha256,
            "pre_release_bundle_sha256": pre_release_sha256,
            "run_execution_bundle_sha256": run_execution.run_execution_bundle_sha256,
            "task_projection": _json_projection(projection),
            "topology": dict(template_pack["semantic_refresh"]["topology"]),
            "topology_sha256": topology.topology_sha256,
            "workflow_execution_binding_sha256": projection.workflow_execution_binding_sha256,
            "workflow_execution_id": projection.workflow_execution_id,
            "workflow_plan_sha256": plan_bundle.workflow_plan.workflow_plan_sha256,
        },
        "workload": {
            "effective_config": {"authoring": {"mode": "semantic_refresh_v2_activated"}},
            "workload_id": f"semantic__{topology.workflow_name}",
        },
    }
    payload["pack_fingerprint"] = compute_pack_fingerprint(payload)
    return ActivatedSemanticRefreshPack(payload, projection, execution_pack)


def validate_semantic_refresh_activation_readiness(
    *,
    template_pack: Mapping[str, Any],
    plan_bundle: SemanticRefreshPlanBundle,
    run_execution: SemanticRefreshRunExecutionBundle,
) -> None:
    """Validate template/plan/run closure before protected authority persistence."""

    topology, execution_pack, pre_release_sha256, package_sha256 = _protected_template(template_pack)
    _validate_plan_run_template(
        plan_bundle,
        run_execution,
        topology,
        execution_pack,
        pre_release_sha256,
        package_sha256,
    )


def validate_semantic_refresh_deployment_readiness(
    *,
    template_pack: Mapping[str, Any],
    plan_bundle: SemanticRefreshPlanBundle,
) -> None:
    """Validate the run-neutral template/plan closure before persistence."""

    topology, execution_pack, pre_release_sha256, package_sha256 = _protected_template(template_pack)
    _validate_plan_template(
        plan_bundle,
        topology,
        execution_pack,
        pre_release_sha256,
        package_sha256,
    )


def _json_projection(projection: SemanticRefreshTaskProjection) -> dict[str, Any]:
    def json_value(value: Any) -> Any:
        if isinstance(value, Mapping):
            return {str(key): json_value(item) for key, item in value.items()}
        if isinstance(value, tuple | list):
            return [json_value(item) for item in value]
        return value

    result = json_value(projection.to_mapping())
    if not isinstance(result, dict):
        raise TypeError("semantic-refresh task projection must be a JSON object")
    return result


__all__ = [
    "ActivatedSemanticRefreshPack",
    "SemanticRefreshTemplateProofAuthority",
    "semantic_refresh_activated_pack",
    "semantic_refresh_template_pack",
    "validate_semantic_refresh_activation_readiness",
    "validate_semantic_refresh_deployment_readiness",
]
