from __future__ import annotations

from typing import Any

from dpone.gitops.schema_airflow_correlation_contracts import airflow_correlation_schema
from dpone.gitops.schema_airflow_run_identity_contracts import airflow_run_identity_schema
from dpone.gitops.schema_contract_primitives import (
    GitOpsSchemaContract,
    array_schema,
    boolean_schema,
    const_schema,
    contract,
    integer_schema,
    issue_ref,
    object_schema,
    string_schema,
)


def airflow_evidence_bundle_contract() -> GitOpsSchemaContract:
    return contract(
        name="airflow-evidence-bundle",
        kind="gitops.airflow_evidence_bundle",
        title="dpone GitOps Airflow evidence bundle contract",
        required=(
            "kind",
            "schema_version",
            "producer",
            "runner_policy",
            "attempt",
            "pod",
            "artifacts",
        ),
        properties={
            "kind": const_schema("gitops.airflow_evidence_bundle"),
            "schema_version": string_schema(),
            "producer": string_schema(),
            "runner_policy": string_schema(),
            "attempt": airflow_attempt_schema(),
            "pod": airflow_pod_correlation_schema(),
            "artifacts": array_schema(airflow_evidence_artifact_schema()),
            "run_identity": airflow_run_identity_schema(),
            "correlation": airflow_correlation_schema(),
            "warnings": array_schema(issue_ref()),
            "blockers": array_schema(issue_ref()),
        },
    )


def airflow_attempt_schema() -> dict[str, Any]:
    return object_schema(
        required=("dag_id", "task_id", "run_id", "try_number", "map_index"),
        properties={
            "dag_id": string_schema(),
            "task_id": string_schema(),
            "run_id": string_schema(),
            "try_number": integer_schema(),
            "map_index": integer_schema(),
        },
    )


def airflow_pod_correlation_schema() -> dict[str, Any]:
    return object_schema(
        required=("pod_name", "pod_uid", "namespace", "service_account", "image", "image_digest"),
        properties={
            "pod_name": nullable_string_schema(),
            "pod_uid": nullable_string_schema(),
            "namespace": nullable_string_schema(),
            "service_account": nullable_string_schema(),
            "image": nullable_string_schema(),
            "image_digest": nullable_string_schema(),
        },
    )


def airflow_evidence_artifact_schema() -> dict[str, Any]:
    return object_schema(
        required=(
            "name",
            "path",
            "expected_kind",
            "actual_kind",
            "required",
            "exists",
            "sha256",
            "bytes",
            "passed",
            "reason",
        ),
        properties={
            "name": string_schema(),
            "path": string_schema(),
            "expected_kind": string_schema(),
            "actual_kind": nullable_string_schema(),
            "required": boolean_schema(),
            "exists": boolean_schema(),
            "sha256": nullable_string_schema(),
            "bytes": nullable_integer_schema(),
            "passed": boolean_schema(),
            "reason": string_schema(),
        },
    )


def nullable_string_schema() -> dict[str, Any]:
    return {"anyOf": [string_schema(), {"type": "null"}]}


def nullable_integer_schema() -> dict[str, Any]:
    return {"anyOf": [integer_schema(), {"type": "null"}]}


__all__ = ["airflow_evidence_bundle_contract"]
