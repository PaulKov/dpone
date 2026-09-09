from __future__ import annotations

from dpone.gitops.schema_contract_primitives import (
    GitOpsSchemaContract,
    array_schema,
    boolean_schema,
    const_schema,
    contract,
    integer_schema,
    issue_ref,
    number_schema,
    object_schema,
    string_schema,
)


def airflow_cluster_doctor_contract() -> GitOpsSchemaContract:
    return contract(
        name="airflow-cluster-doctor",
        kind="gitops.airflow_cluster_doctor",
        title="dpone GitOps Airflow cluster doctor contract",
        required=(
            "kind",
            "schema_version",
            "producer",
            "mode",
            "runner_policy",
            "artifact_dir",
            "runtime_profile_path",
            "pod_contract_path",
            "namespace",
            "service_account",
            "timeout_seconds",
            "secret_refs",
            "external_secret_refs",
            "checks",
            "commands",
            "results",
        ),
        properties={
            "kind": const_schema("gitops.airflow_cluster_doctor"),
            "schema_version": string_schema(),
            "producer": string_schema(),
            "mode": string_schema(),
            "runner_policy": string_schema(),
            "artifact_dir": string_schema(),
            "runtime_profile_path": string_schema(),
            "pod_contract_path": string_schema(),
            "connection_bridge_plan_path": {"type": ["string", "null"]},
            "namespace": string_schema(),
            "service_account": string_schema(),
            "timeout_seconds": integer_schema(),
            "secret_refs": array_schema(airflow_cluster_secret_ref_schema()),
            "external_secret_refs": array_schema(airflow_cluster_external_secret_ref_schema()),
            "checks": array_schema(airflow_cluster_check_schema()),
            "commands": array_schema(airflow_cluster_command_schema()),
            "results": array_schema(airflow_cluster_result_schema()),
            "warnings": array_schema(issue_ref()),
            "blockers": array_schema(issue_ref()),
        },
    )


def airflow_cluster_check_schema() -> dict[str, object]:
    return object_schema(
        required=("name", "passed", "severity", "message", "path", "source"),
        properties={
            "name": string_schema(),
            "passed": boolean_schema(),
            "severity": string_schema(),
            "message": string_schema(),
            "path": string_schema(),
            "source": string_schema(),
        },
    )


def airflow_cluster_secret_ref_schema() -> dict[str, object]:
    return object_schema(
        required=("name", "kind", "source", "required", "required_keys"),
        properties={
            "name": string_schema(),
            "kind": string_schema(),
            "source": string_schema(),
            "required": boolean_schema(),
            "required_keys": array_schema(string_schema()),
        },
    )


def airflow_cluster_external_secret_ref_schema() -> dict[str, object]:
    return object_schema(
        required=("name", "source", "required"),
        properties={"name": string_schema(), "source": string_schema(), "required": boolean_schema()},
    )


def airflow_cluster_command_schema() -> dict[str, object]:
    return object_schema(
        required=("name", "kind", "command", "required", "timeout_seconds", "expected_keys", "executed"),
        properties={
            "name": string_schema(),
            "kind": string_schema(),
            "command": string_schema(),
            "required": boolean_schema(),
            "timeout_seconds": integer_schema(),
            "expected_keys": array_schema(string_schema()),
            "executed": boolean_schema(),
        },
    )


def airflow_cluster_result_schema() -> dict[str, object]:
    return object_schema(
        required=("name", "command", "exit_code", "stdout", "stderr", "duration_seconds"),
        properties={
            "name": string_schema(),
            "command": string_schema(),
            "exit_code": integer_schema(),
            "stdout": string_schema(),
            "stderr": string_schema(),
            "duration_seconds": number_schema(),
        },
    )


__all__ = [
    "airflow_cluster_check_schema",
    "airflow_cluster_command_schema",
    "airflow_cluster_doctor_contract",
    "airflow_cluster_external_secret_ref_schema",
    "airflow_cluster_result_schema",
    "airflow_cluster_secret_ref_schema",
]
