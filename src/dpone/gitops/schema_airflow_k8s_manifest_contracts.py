from __future__ import annotations

from dpone.gitops.schema_contract_primitives import (
    array_schema,
    boolean_schema,
    const_schema,
    contract,
    issue_ref,
    object_schema,
    string_schema,
)


def airflow_k8s_manifests_contract() -> object:
    return contract(
        name="airflow-k8s-manifests",
        kind="gitops.airflow_k8s_manifests",
        title="dpone GitOps Airflow Kubernetes manifests report",
        required=(
            "kind",
            "schema_version",
            "producer",
            "artifact_dir",
            "manifest_path",
            "runtime_profile_path",
            "pod_contract_path",
            "gitops_controller",
            "namespace",
            "service_account",
            "objects",
            "controller_hints",
            "warnings",
            "blockers",
        ),
        properties={
            "kind": const_schema("gitops.airflow_k8s_manifests"),
            "schema_version": string_schema(),
            "producer": string_schema(),
            "artifact_dir": string_schema(),
            "manifest_path": string_schema(),
            "runtime_profile_path": string_schema(),
            "pod_contract_path": string_schema(),
            "connection_bridge_plan_path": {"type": ["string", "null"]},
            "gitops_controller": string_schema(),
            "namespace": string_schema(),
            "service_account": string_schema(),
            "objects": array_schema(_manifest_object_schema()),
            "controller_hints": array_schema(string_schema()),
            "warnings": array_schema(issue_ref()),
            "blockers": array_schema(issue_ref()),
        },
    )


def _manifest_object_schema() -> dict[str, object]:
    return object_schema(
        required=("api_version", "kind", "name", "namespace", "source", "required"),
        properties={
            "api_version": string_schema(),
            "kind": string_schema(),
            "name": string_schema(),
            "namespace": string_schema(),
            "source": string_schema(),
            "required": boolean_schema(),
        },
    )


__all__ = ["airflow_k8s_manifests_contract"]
