from __future__ import annotations

from typing import Any

from dpone.gitops.schema_contract_primitives import (
    array_schema,
    boolean_schema,
    const_schema,
    integer_schema,
    number_schema,
    object_schema,
    string_schema,
)

AIRFLOW_RUNTIME_STEP_REQUIRED: tuple[str, ...] = tuple(
    "name kind command required status exit_code started_at finished_at duration_seconds".split()
)
AIRFLOW_RUN_SPEC_REQUIRED: tuple[str, ...] = tuple(
    "kind schema_version producer bundle_path image worktree evidence_output entries steps".split()
)
AIRFLOW_RUNTIME_EVIDENCE_REQUIRED: tuple[str, ...] = tuple(
    "kind schema_version producer run_spec_path bundle_path image status started_at finished_at duration_seconds steps".split()
)
AIRFLOW_RUNTIME_PROFILE_REQUIRED: tuple[str, ...] = tuple(
    (
        "kind schema_version producer bundle_path run_spec_path runtime_evidence_path xcom_summary_path "
        "dag_factory_path outcome_gate_path image namespace service_account resources artifact_sink runner_policy outcome_mode"
    ).split()
)
AIRFLOW_K8S_SMOKE_REQUIRED: tuple[str, ...] = tuple(
    (
        "kind schema_version producer mode runner_kind runner_policy run_spec_path runtime_profile_path pod_contract_path "
        "namespace service_account image image_ref smoke_name timeout_seconds checks commands results"
    ).split()
)


def airflow_artifact_schema() -> dict[str, Any]:
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


def airflow_runtime_resources_schema() -> dict[str, Any]:
    return object_schema(
        required=("requests", "limits"),
        properties={
            "requests": object_schema(),
            "limits": object_schema(),
        },
    )


def airflow_artifact_sink_schema() -> dict[str, Any]:
    return object_schema(
        required=("kind", "path"),
        properties={
            "kind": string_schema(),
            "path": string_schema(),
        },
    )


def airflow_env_var_schema() -> dict[str, Any]:
    return object_schema(
        required=("name", "value"),
        properties={
            "name": string_schema(),
            "value": string_schema(),
        },
    )


def airflow_check_schema() -> dict[str, Any]:
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


def airflow_image_contract_schema() -> dict[str, Any]:
    return object_schema(
        required=("kind", "schema_version", "producer", "image", "tools"),
        properties={
            "kind": const_schema("gitops.airflow_image_contract.v1"),
            "schema_version": string_schema(),
            "producer": string_schema(),
            "image": string_schema(),
            "image_digest": _nullable_string_schema(),
            "dpone_version": string_schema(),
            "python_version": string_schema(),
            "airflow_provider_version": string_schema(),
            "tools": array_schema(string_schema()),
            "user": string_schema(),
            "workdir": string_schema(),
            "entrypoint": string_schema(),
        },
    )


def airflow_run_spec_entry_schema() -> dict[str, Any]:
    return object_schema(
        required=("manifest", "plan_path", "verify_path", "passed"),
        properties={
            "manifest": string_schema(),
            "plan_path": string_schema(),
            "verify_path": string_schema(),
            "passed": boolean_schema(),
        },
    )


def airflow_run_spec_step_schema() -> dict[str, Any]:
    return object_schema(
        required=("name", "kind", "command", "required"),
        properties={
            "name": string_schema(),
            "kind": string_schema(),
            "command": string_schema(),
            "required": boolean_schema(),
            "manifest": string_schema(),
            "depends_on": array_schema(string_schema()),
        },
    )


def airflow_runtime_step_schema() -> dict[str, Any]:
    return object_schema(
        required=AIRFLOW_RUNTIME_STEP_REQUIRED,
        properties={
            "name": string_schema(),
            "kind": string_schema(),
            "command": string_schema(),
            "required": boolean_schema(),
            "status": string_schema(),
            "exit_code": integer_schema(),
            "started_at": string_schema(),
            "finished_at": string_schema(),
            "duration_seconds": number_schema(),
            "manifest": string_schema(),
        },
    )


def airflow_k8s_smoke_command_schema() -> dict[str, Any]:
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


def airflow_k8s_smoke_result_schema() -> dict[str, Any]:
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


def _nullable_string_schema() -> dict[str, object]:
    return {"type": ["string", "null"]}
