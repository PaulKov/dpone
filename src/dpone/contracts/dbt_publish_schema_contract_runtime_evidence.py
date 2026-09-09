"""Runtime evidence and source-snapshot JSON Schema fragments."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

from dpone.contracts.dbt_publish_schema_contract_common import (
    DIGEST,
    TOKEN,
    nullable,
    object_schema,
)
from dpone.contracts.dbt_publish_schema_contract_runtime_inputs import (
    sqlserver_runtime_policy_schema,
)


def execution_evidence_schema() -> dict[str, Any]:
    """Return the strict dbt runtime evidence schema."""

    digest_names = (
        "release_id",
        "deployment_id",
        "workload_pack_sha256",
        "project_bundle_sha256",
        "manifest_sha256",
        "selection_sha256",
        "toolchain_sha256",
        "invocation_context_sha256",
        "logical_target_sha256",
        "target_binding_sha256",
        "adapter_policy_sha256",
        "graph_policy_sha256",
    )
    properties = {name: deepcopy(DIGEST) for name in digest_names}
    credential_version = object_schema(
        ("connection_ref", "resolver", "resolved_version"),
        {
            "connection_ref": TOKEN,
            "resolver": TOKEN,
            "resolved_version": nullable(TOKEN),
        },
    )
    airflow = object_schema(
        ("dag_id", "task_id", "run_id", "try_number", "map_index"),
        {
            "dag_id": TOKEN,
            "task_id": TOKEN,
            "run_id": TOKEN,
            "try_number": {"type": "integer"},
            "map_index": {"type": "integer"},
        },
    )
    node = object_schema(
        ("unique_id", "status", "execution_time"),
        {
            "unique_id": TOKEN,
            "status": {
                "enum": [
                    "error",
                    "fail",
                    "no-op",
                    "partial success",
                    "pass",
                    "runtime error",
                    "skipped",
                    "success",
                    "warn",
                ]
            },
            "execution_time": {"type": "number"},
        },
    )
    recovery = object_schema(
        (
            "status",
            "failure_boundary",
            "target_state",
            "checkpoint_state",
            "source_state",
            "safe_to_retry",
            "operator_verification_required",
            "recovery_action",
        ),
        {
            "status": {"const": "COMMIT_UNKNOWN"},
            "failure_boundary": {"const": "target_invocation"},
            "target_state": {"const": "unknown"},
            "checkpoint_state": {"const": "not_advanced"},
            "source_state": {"const": "not_advanced"},
            "safe_to_retry": {"const": False},
            "operator_verification_required": {"const": True},
            "recovery_action": {"const": "operator_verification_required"},
        },
    )
    properties.update(
        {
            "schema": {"const": "dpone.dbt-execution-evidence.v1"},
            "status": {"enum": ["passed", "failed"]},
            "code": TOKEN,
            "workflow_id": TOKEN,
            "adapter_runtime": sqlserver_runtime_policy_schema(),
            "preflight_status": {"enum": ["not_started", "passed", "failed"]},
            "build_started": {"type": "boolean"},
            "dbt_exit_code": nullable({"type": "integer"}),
            "dbt_warning_policy": {"enum": ["fail", "allow"]},
            "dbt_warning_count": {"type": "integer", "minimum": 0},
            "dbt_schema_version": nullable(TOKEN),
            "dbt_version": nullable(TOKEN),
            "invocation_id": nullable(TOKEN),
            "started_at": {"type": "string", "format": "date-time"},
            "finished_at": {"type": "string", "format": "date-time"},
            "airflow": airflow,
            "credential_versions": {
                "type": "array",
                "items": credential_version,
                "uniqueItems": True,
            },
            "nodes": {
                "type": "array",
                "items": node,
                "uniqueItems": True,
            },
            "recovery": recovery,
        }
    )
    schema = object_schema(
        (
            "schema",
            "status",
            "code",
            "workflow_id",
            *digest_names,
            "adapter_runtime",
            "preflight_status",
            "build_started",
            "dbt_exit_code",
            "dbt_warning_policy",
            "dbt_warning_count",
            "started_at",
            "finished_at",
            "airflow",
            "credential_versions",
            "nodes",
        ),
        properties,
    )
    schema["oneOf"] = [
        {
            "properties": {
                "status": {"const": "passed"},
                "code": {"const": "DPONE_DBT_EXECUTION_PASSED"},
                "preflight_status": {"const": "passed"},
                "build_started": {"const": True},
                "dbt_exit_code": {"const": 0},
                "dbt_schema_version": TOKEN,
                "dbt_version": TOKEN,
                "invocation_id": TOKEN,
                "nodes": {
                    "type": "array",
                    "minItems": 1,
                    "items": node,
                    "uniqueItems": True,
                },
            },
            "required": [
                "status",
                "code",
                "dbt_exit_code",
                "dbt_schema_version",
                "dbt_version",
                "invocation_id",
            ],
            "allOf": [
                {
                    "if": {
                        "properties": {
                            "dbt_warning_policy": {"const": "fail"},
                        },
                        "required": ["dbt_warning_policy"],
                    },
                    "then": {
                        "properties": {
                            "dbt_warning_count": {"const": 0},
                            "nodes": {
                                "type": "array",
                                "minItems": 1,
                                "items": {
                                    **deepcopy(node),
                                    "properties": {
                                        **deepcopy(node["properties"]),
                                        "status": {
                                            "enum": [
                                                "success",
                                                "pass",
                                                "no-op",
                                            ]
                                        },
                                    },
                                },
                                "uniqueItems": True,
                            },
                        }
                    },
                    "else": {
                        "properties": {
                            "nodes": {
                                "type": "array",
                                "minItems": 1,
                                "items": {
                                    **deepcopy(node),
                                    "properties": {
                                        **deepcopy(node["properties"]),
                                        "status": {
                                            "enum": [
                                                "success",
                                                "pass",
                                                "no-op",
                                                "warn",
                                            ]
                                        },
                                    },
                                },
                                "uniqueItems": True,
                            }
                        }
                    },
                }
            ],
        },
        {
            "properties": {
                "status": {"const": "failed"},
                "code": {"not": {"const": "DPONE_DBT_EXECUTION_PASSED"}},
            },
            "required": ["status", "code"],
        },
    ]
    schema["allOf"] = [
        {
            "if": {
                "properties": {"code": {"const": "COMMIT_UNKNOWN"}},
                "required": ["code"],
            },
            "then": {
                "properties": {
                    "status": {"const": "failed"},
                    "build_started": {"const": True},
                    "recovery": recovery,
                },
                "required": ["recovery"],
            },
            "else": {"not": {"required": ["recovery"]}},
        }
    ]
    schema["$comment"] = (
        "dbt_warning_count must equal the number of warn nodes and node "
        "unique_id values must be unique; the canonical Python parser "
        "enforces these cross-item invariants."
    )
    schema["x-dpone-semantic-invariants"] = [
        "dbt_warning_count_equals_warn_node_count",
        "node_unique_ids_are_unique",
        "commit_unknown_requires_non_retryable_recovery",
    ]
    return schema


def source_snapshot_schema() -> dict[str, Any]:
    """Return the environment-neutral dbt source snapshot schema."""

    return object_schema(
        (
            "schema",
            "project_bundle_sha256",
            "manifest_sha256",
            "snapshot_sha256",
        ),
        {
            "schema": {"const": "dpone.dbt-source-snapshot.v1"},
            "project_bundle_sha256": DIGEST,
            "manifest_sha256": DIGEST,
            "snapshot_sha256": DIGEST,
        },
    )


__all__ = ["execution_evidence_schema", "source_snapshot_schema"]
