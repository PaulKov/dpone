from __future__ import annotations

from dpone_airflow_pack.provider_execution_contract import provider_execution_json_schema

from dpone.gitops.schema_airflow_mapping_contracts import airflow_mapping_plan_schema
from dpone.gitops.schema_airflow_pack_steps import airflow_pack_step_schema
from dpone.gitops.schema_contract_primitives import (
    array_schema,
    boolean_schema,
    const_schema,
    contract,
    integer_schema,
    issue_ref,
    object_schema,
    string_schema,
)


def airflow_pack_contract() -> object:
    return contract(
        name="airflow-pack",
        kind="gitops.airflow_pack",
        title="dpone GitOps Airflow runtime pack report",
        required=(
            "kind",
            "schema_version",
            "pack_identity",
            "pack_fingerprint",
            "producer",
            "artifact_dir",
            "output_path",
            "bundle_path",
            "mode",
            "runner_policy",
            "include_live_gates",
            "artifacts",
            "steps",
            "next_actions",
            "warnings",
            "blockers",
        ),
        properties={
            "kind": const_schema("gitops.airflow_pack"),
            "schema_version": string_schema(),
            "pack_identity": {
                "type": "object",
                "required": ["schema"],
                "additionalProperties": False,
                "properties": {
                    "schema": {"const": "dpone.airflow-pack-identity.v1"},
                },
            },
            "producer": string_schema(),
            "artifact_dir": string_schema(),
            "output_path": string_schema(),
            "bundle_path": string_schema(),
            "image": {"type": ["string", "null"]},
            "image_digest": {"type": ["string", "null"]},
            "mode": string_schema(),
            "runner_policy": string_schema(),
            "include_live_gates": boolean_schema(),
            "artifacts": array_schema(_pack_artifact_schema()),
            "steps": array_schema(airflow_pack_step_schema()),
            "runtime_selection": object_schema(
                properties={
                    "mode": string_schema(),
                    "required_for_selected_nodes": boolean_schema(),
                }
            ),
            "process_plans": {
                "type": "object",
                "additionalProperties": _pack_process_plan_schema(),
            },
            "mapping_plan": airflow_mapping_plan_schema(),
            "workload_dependencies": array_schema(_pack_dependency_schema()),
            "next_actions": array_schema(string_schema()),
            "warnings": array_schema(issue_ref()),
            "blockers": array_schema(issue_ref()),
            "workload": object_schema(),
            "effective_config": object_schema(),
            "runtime_command": string_schema(),
            "runtime_manifest": object_schema(
                required=("path", "sha256", "kind"),
                properties={
                    "path": string_schema(),
                    "sha256": {"type": ["string", "null"]},
                    "kind": string_schema(),
                },
            ),
            "runtime_bootstrap": _runtime_bootstrap_schema(),
            "runtime_payload": _runtime_payload_schema(),
            "pod_spec": object_schema(),
            "connection_projection": object_schema(),
            "xcom": object_schema(),
            "outcome_gate": object_schema(),
            "provider_execution": _provider_execution_schema(),
            "kpo_kwargs": object_schema(),
            "mapped_kpo_kwargs": object_schema(),
            "artifact_index": object_schema(),
            "pack_fingerprint": {
                "type": "string",
                "pattern": "^sha256:[0-9a-f]{64}$",
            },
        },
    )


def airflow_reconcile_contract() -> object:
    return contract(
        name="airflow-reconcile",
        kind="gitops.airflow_reconcile",
        title="dpone GitOps Airflow reconcile report",
        required=(
            "kind",
            "schema_version",
            "producer",
            "workload_set",
            "env",
            "affected_workloads",
            "packs",
            "dag_specs",
            "warnings",
            "blockers",
        ),
        properties={
            "kind": const_schema("gitops.airflow_reconcile"),
            "schema_version": const_schema("1"),
            "producer": const_schema("dpone gitops airflow reconcile"),
            "workload_set": string_schema(),
            "env": string_schema(),
            "selection_mode": {"enum": ["affected", "all"]},
            "affected_workloads": array_schema(string_schema()),
            "packs": array_schema(string_schema()),
            "dag_specs": array_schema(string_schema()),
            "warnings": array_schema(issue_ref()),
            "blockers": array_schema(issue_ref()),
            "meta": object_schema(),
        },
    )


def _provider_execution_schema() -> dict[str, object]:
    return provider_execution_json_schema()


def _runtime_bootstrap_schema() -> dict[str, object]:
    return {
        "type": "object",
        "required": ["schema", "commands"],
        "additionalProperties": False,
        "properties": {
            "schema": {"const": "dpone.airflow-runtime-bootstrap.v1"},
            "commands": {
                "type": "object",
                "minProperties": 1,
                "additionalProperties": {
                    "type": "object",
                    "required": ["argv", "env"],
                    "additionalProperties": False,
                    "properties": {
                        "argv": {
                            "type": "array",
                            "minItems": 3,
                            "items": {"type": "string"},
                        },
                        "env": {
                            "type": "object",
                            "required": ["DPONE_SKIP_AIRFLOW_SEPARATE_HOOKS"],
                            "additionalProperties": False,
                            "properties": {"DPONE_SKIP_AIRFLOW_SEPARATE_HOOKS": {"const": "1"}},
                        },
                    },
                },
            },
        },
    }


def _runtime_payload_schema() -> dict[str, object]:
    return {
        "type": "object",
        "required": ["schema", "archive"],
        "additionalProperties": False,
        "properties": {
            "schema": {"const": "dpone.airflow-runtime-payload.v1"},
            "archive": {
                "type": "object",
                "required": [
                    "encoding",
                    "format",
                    "sha256",
                    "bytes",
                    "data",
                ],
                "additionalProperties": False,
                "properties": {
                    "encoding": {"const": "base64"},
                    "format": {"const": "tar+gzip"},
                    "sha256": {
                        "type": "string",
                        "pattern": "^sha256:[0-9a-f]{64}$",
                    },
                    "bytes": {"type": "integer", "minimum": 1},
                    "data": {
                        "type": "string",
                        "minLength": 4,
                        "pattern": ("^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$"),
                    },
                },
            },
        },
    }


def _pack_artifact_schema() -> dict[str, object]:
    return object_schema(
        required=(
            "name",
            "path",
            "format",
            "expected_kind",
            "required",
            "exists",
            "passed",
            "reason",
        ),
        properties={
            "name": string_schema(),
            "path": string_schema(),
            "format": string_schema(),
            "expected_kind": string_schema(),
            "actual_kind": {"type": ["string", "null"]},
            "required": boolean_schema(),
            "exists": boolean_schema(),
            "passed": boolean_schema(),
            "reason": string_schema(),
            "sha256": {"type": ["string", "null"]},
            "bytes": {"type": ["integer", "null"]},
        },
    )


def _pack_dependency_schema() -> dict[str, object]:
    return object_schema(
        required=("kind", "path", "sha256"),
        properties={"kind": string_schema(), "path": string_schema(), "sha256": string_schema()},
    )


def _pack_process_plan_schema() -> dict[str, object]:
    return object_schema(
        required=("selector", "dag_node", "runtime_commands", "steps"),
        properties={
            "selector": {"type": ["string", "null"]},
            "dag_node": object_schema(
                required=(
                    "process_name",
                    "visibility",
                    "task_group",
                    "estimated_visible_tasks",
                    "depends_on_process_selectors",
                ),
                properties={
                    "node_id": string_schema(),
                    "process_name": string_schema(),
                    "visibility": {"enum": ["inline", "task", "group"]},
                    "task_group": {"type": ["string", "null"]},
                    "estimated_visible_tasks": {**integer_schema(), "minimum": 1},
                    "depends_on_process_selectors": array_schema(string_schema()),
                },
            ),
            "runtime_commands": object_schema(
                required=("inline", "expanded"),
                properties={"inline": string_schema(), "expanded": string_schema()},
            ),
            "mapping_plan": airflow_mapping_plan_schema(),
            "steps": array_schema(airflow_pack_step_schema()),
        },
    )


__all__ = ["airflow_pack_contract", "airflow_reconcile_contract"]
