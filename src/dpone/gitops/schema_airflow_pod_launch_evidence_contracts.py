from __future__ import annotations

from typing import Any

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


def airflow_pod_launch_evidence_contract() -> GitOpsSchemaContract:
    return contract(
        name="airflow-pod-launch-evidence",
        kind="gitops.airflow_pod_launch_evidence",
        title="dpone GitOps Airflow pod launch evidence contract",
        required=(
            "kind",
            "schema_version",
            "producer",
            "mode",
            "runner_policy",
            "runtime_profile_path",
            "pod_contract_path",
            "pod_name",
            "namespace",
            "service_account",
            "image",
            "expected_phase",
            "timeout_seconds",
            "log_tail_lines",
            "checks",
            "commands",
            "results",
            "observed_pod",
        ),
        properties={
            "kind": const_schema("gitops.airflow_pod_launch_evidence"),
            "schema_version": string_schema(),
            "producer": string_schema(),
            "mode": string_schema(),
            "runner_policy": string_schema(),
            "runtime_profile_path": string_schema(),
            "pod_contract_path": string_schema(),
            "image_contract_path": string_schema(),
            "runtime_evidence_path": string_schema(),
            "xcom_summary_path": string_schema(),
            "pod_name": string_schema(),
            "namespace": string_schema(),
            "service_account": string_schema(),
            "image": string_schema(),
            "image_digest": string_schema(),
            "expected_phase": string_schema(),
            "timeout_seconds": integer_schema(),
            "log_tail_lines": integer_schema(),
            "checks": array_schema(airflow_pod_launch_check_schema()),
            "commands": array_schema(airflow_pod_launch_command_schema()),
            "results": array_schema(airflow_pod_launch_result_schema()),
            "observed_pod": nullable_object_schema(airflow_observed_pod_schema()),
            "warnings": array_schema(issue_ref()),
            "blockers": array_schema(issue_ref()),
        },
    )


def airflow_pod_launch_check_schema() -> dict[str, Any]:
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


def airflow_pod_launch_command_schema() -> dict[str, Any]:
    return object_schema(
        required=("name", "kind", "command", "required", "timeout_seconds", "executed"),
        properties={
            "name": string_schema(),
            "kind": string_schema(),
            "command": string_schema(),
            "required": boolean_schema(),
            "timeout_seconds": integer_schema(),
            "executed": boolean_schema(),
        },
    )


def airflow_pod_launch_result_schema() -> dict[str, Any]:
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


def airflow_observed_pod_schema() -> dict[str, Any]:
    return object_schema(
        required=("pod_name", "namespace", "phase", "service_account", "containers", "events", "logs_tail"),
        properties={
            "pod_name": string_schema(),
            "namespace": string_schema(),
            "phase": string_schema(),
            "service_account": string_schema(),
            "node_name": string_schema(),
            "containers": array_schema(airflow_observed_container_schema()),
            "events": array_schema(airflow_observed_event_schema()),
            "logs_tail": string_schema(),
        },
    )


def airflow_observed_container_schema() -> dict[str, Any]:
    return object_schema(
        required=("name", "image", "image_id", "ready", "restart_count", "state"),
        properties={
            "name": string_schema(),
            "image": string_schema(),
            "image_id": string_schema(),
            "ready": boolean_schema(),
            "restart_count": integer_schema(),
            "state": string_schema(),
            "exit_code": integer_schema(),
            "reason": string_schema(),
            "message": string_schema(),
        },
    )


def airflow_observed_event_schema() -> dict[str, Any]:
    return object_schema(
        required=("type", "reason", "message", "count"),
        properties={
            "type": string_schema(),
            "reason": string_schema(),
            "message": string_schema(),
            "count": integer_schema(),
        },
    )


def nullable_object_schema(schema: dict[str, Any]) -> dict[str, Any]:
    return {"anyOf": [schema, {"type": "null"}]}


__all__ = ["airflow_pod_launch_evidence_contract"]
