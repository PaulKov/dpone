from __future__ import annotations

from typing import Any

from dpone.gitops.schema_airflow_connection_bridge_contracts import airflow_connection_bridge_schema
from dpone.gitops.schema_airflow_git_sync_contracts import airflow_git_sync_schema
from dpone.gitops.schema_contract_primitives import (
    GitOpsSchemaContract,
    array_schema,
    boolean_schema,
    const_schema,
    contract,
    issue_ref,
    object_schema,
    string_schema,
)


def airflow_outcome_gate_contract() -> GitOpsSchemaContract:
    return contract(
        name="airflow-outcome-gate",
        kind="gitops.airflow_outcome_gate",
        title="dpone GitOps Airflow outcome gate contract",
        required=(
            "kind",
            "schema_version",
            "producer",
            "xcom_summary_path",
            "required_status",
            "status",
            "passed",
        ),
        properties={
            "kind": const_schema("gitops.airflow_outcome_gate"),
            "schema_version": string_schema(),
            "producer": string_schema(),
            "xcom_summary_path": string_schema(),
            "required_status": string_schema(),
            "status": string_schema(),
            "passed": boolean_schema(),
            "run_spec_path": string_schema(),
            "runtime_evidence_path": string_schema(),
            "runtime_evidence_sha256": string_schema(),
            "failed_step": {"type": ["string", "null"]},
            "step_counts": object_schema(),
            "warnings": array_schema(issue_ref()),
            "blockers": array_schema(issue_ref()),
        },
    )


def airflow_pod_contract_contract() -> GitOpsSchemaContract:
    return contract(
        name="airflow-pod-contract",
        kind="gitops.airflow_pod_contract",
        title="dpone GitOps Airflow pod contract",
        required=(
            "kind",
            "schema_version",
            "producer",
            "bundle_path",
            "run_spec_path",
            "runtime_profile_path",
            "pod_spec_path",
            "kpo_kwargs_path",
            "image",
            "namespace",
            "service_account",
            "xcom",
            "pod_spec",
            "kpo_kwargs",
        ),
        properties={
            "kind": const_schema("gitops.airflow_pod_contract"),
            "schema_version": string_schema(),
            "producer": string_schema(),
            "bundle_path": string_schema(),
            "run_spec_path": string_schema(),
            "runtime_profile_path": string_schema(),
            "pod_spec_path": string_schema(),
            "kpo_kwargs_path": string_schema(),
            "image": string_schema(),
            "namespace": string_schema(),
            "service_account": string_schema(),
            "xcom": airflow_xcom_contract_schema(),
            "pod_spec": object_schema(),
            "kpo_kwargs": object_schema(),
            "git_sync": airflow_git_sync_schema(),
            "connection_bridge": airflow_connection_bridge_schema(),
            "warnings": array_schema(issue_ref()),
            "blockers": array_schema(issue_ref()),
        },
    )


def airflow_pod_doctor_contract() -> GitOpsSchemaContract:
    return contract(
        name="airflow-pod-doctor",
        kind="gitops.airflow_pod_doctor",
        title="dpone GitOps Airflow pod doctor contract",
        required=(
            "kind",
            "schema_version",
            "producer",
            "artifact_dir",
            "pod_contract_path",
            "pod_spec_path",
            "kpo_kwargs_path",
            "runner_policy",
            "checks",
        ),
        properties={
            "kind": const_schema("gitops.airflow_pod_doctor"),
            "schema_version": string_schema(),
            "producer": string_schema(),
            "artifact_dir": string_schema(),
            "pod_contract_path": string_schema(),
            "pod_spec_path": string_schema(),
            "kpo_kwargs_path": string_schema(),
            "runner_policy": string_schema(),
            "checks": array_schema(airflow_check_schema()),
            "warnings": array_schema(issue_ref()),
            "blockers": array_schema(issue_ref()),
        },
    )


def airflow_xcom_contract_schema() -> dict[str, Any]:
    return object_schema(
        required=("enabled", "return_path", "summary_path", "mode", "outcome_mode"),
        properties={
            "enabled": boolean_schema(),
            "return_path": string_schema(),
            "summary_path": string_schema(),
            "mode": string_schema(),
            "outcome_mode": string_schema(),
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


__all__ = [
    "airflow_check_schema",
    "airflow_outcome_gate_contract",
    "airflow_pod_contract_contract",
    "airflow_pod_doctor_contract",
]
