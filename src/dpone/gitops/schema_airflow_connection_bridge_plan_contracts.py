from __future__ import annotations

from dpone.gitops.schema_airflow_connection_bridge_contracts import airflow_connection_env_ref_schema
from dpone.gitops.schema_contract_primitives import (
    GitOpsSchemaContract,
    airflow_connection_id_schema,
    array_schema,
    boolean_schema,
    const_schema,
    contract,
    issue_ref,
    object_schema,
    string_schema,
)
from dpone.gitops.schema_kubernetes_contracts import kubernetes_secret_name_schema


def airflow_connection_bridge_plan_contract() -> GitOpsSchemaContract:
    return contract(
        name="airflow-connection-bridge-plan",
        kind="gitops.airflow_connection_bridge_plan",
        title="dpone GitOps Airflow connection bridge plan contract",
        required=(
            "kind",
            "schema_version",
            "producer",
            "artifact_dir",
            "output_path",
            "runtime_profile_path",
            "pod_contract_path",
            "mode",
            "runtime_mode",
            "required_connection_ids",
            "env",
            "artifacts",
        ),
        properties={
            "kind": const_schema("gitops.airflow_connection_bridge_plan"),
            "schema_version": string_schema(),
            "producer": string_schema(),
            "artifact_dir": string_schema(),
            "output_path": string_schema(),
            "runtime_profile_path": string_schema(),
            "pod_contract_path": string_schema(),
            "mode": string_schema(),
            "runtime_mode": string_schema(),
            "secret_name": kubernetes_secret_name_schema(nullable=True),
            "required_connection_ids": array_schema(airflow_connection_id_schema()),
            "env": array_schema(airflow_connection_env_ref_schema()),
            "artifacts": array_schema(airflow_connection_bridge_plan_artifact_schema()),
            "warnings": array_schema(issue_ref()),
            "blockers": array_schema(issue_ref()),
        },
    )


def airflow_connection_bridge_plan_artifact_schema() -> dict[str, object]:
    return object_schema(
        required=("path", "kind", "required", "exists", "reason"),
        properties={
            "path": string_schema(),
            "kind": string_schema(),
            "required": boolean_schema(),
            "exists": boolean_schema(),
            "reason": string_schema(),
        },
    )


__all__ = [
    "airflow_connection_bridge_plan_artifact_schema",
    "airflow_connection_bridge_plan_contract",
]
