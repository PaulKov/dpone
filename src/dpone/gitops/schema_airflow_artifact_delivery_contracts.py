"""Public report schemas for immutable Airflow artifact delivery."""

from __future__ import annotations

from typing import Any

from dpone.gitops.schema_contract_primitives import GitOpsSchemaContract, documented_contract


def airflow_artifact_delivery_schema_contracts() -> tuple[GitOpsSchemaContract, ...]:
    return (
        _publish_contract(),
        _publish_v2_contract(),
        _materialize_contract(),
        _loader_ack_v1_contract(),
        _loader_ack_v2_contract(),
    )


def _publish_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="airflow-artifact-publish",
        kind="dpone.airflow-artifact-publish.v1",
        title="dpone GitOps Airflow artifact publication report",
        required=(
            "schema",
            "passed",
            "changes",
            "errors",
            "status",
            "release_id",
            "deployment_id",
            "environment",
            "artifact_registry_ref",
            "created_objects",
            "existing_equal_objects",
            "published_release",
            "published_deployment",
        ),
        properties={
            **_common_properties("dpone.airflow-artifact-publish.v1", ("published", "no_op", "failed")),
            "created_objects": non_negative_integer_schema(),
            "existing_equal_objects": non_negative_integer_schema(),
            "published_release": {"type": "boolean"},
            "published_deployment": {"type": "boolean"},
        },
        additional_properties=False,
    )


def _publish_v2_contract() -> GitOpsSchemaContract:
    result = documented_contract(
        name="airflow-artifact-publish-v2",
        kind="dpone.airflow-artifact-publish.v2",
        title="dpone GitOps verified Airflow artifact publication report",
        required=(
            "schema",
            "passed",
            "changes",
            "errors",
            "status",
            "release_id",
            "deployment_id",
            "environment",
            "artifact_registry_ref",
            "created_objects",
            "existing_equal_objects",
            "verified_objects",
            "published_release",
            "published_deployment",
            "publication_commitment",
        ),
        properties={
            **_common_properties("dpone.airflow-artifact-publish.v2", ("published", "no_op", "failed")),
            "created_objects": non_negative_integer_schema(),
            "existing_equal_objects": non_negative_integer_schema(),
            "verified_objects": non_negative_integer_schema(),
            "published_release": {"type": "boolean"},
            "published_deployment": {"type": "boolean"},
            "publication_commitment": {
                "anyOf": [
                    _publication_commitment_schema(),
                    {"type": "null"},
                ]
            },
        },
        additional_properties=False,
    )
    result.schema["allOf"] = [
        {
            "if": {"properties": {"status": {"enum": ["published", "no_op"]}}},
            "then": {"properties": {"publication_commitment": _publication_commitment_schema()}},
            "else": {"properties": {"publication_commitment": {"type": "null"}}},
        }
    ]
    return result


def _publication_commitment_schema() -> dict[str, Any]:
    descriptor = {
        "type": "object",
        "required": ["object_key", "sha256", "bytes"],
        "additionalProperties": False,
        "properties": {
            "object_key": {
                "type": "string",
                "pattern": "^(?:releases|deployments)/[A-Za-z0-9._/-]+$",
                "maxLength": 512,
            },
            "sha256": sha256_schema(),
            "bytes": {"type": "integer", "minimum": 1},
        },
    }
    return {
        "type": "object",
        "required": [
            "schema",
            "verification_mode",
            "registry_scope_id",
            "projection_verified",
            "release",
            "deployment",
            "airflow_index",
        ],
        "additionalProperties": False,
        "properties": {
            "schema": {"const": "dpone.airflow-publication-commitment.v1"},
            "verification_mode": {"const": "remote_readback_sha256"},
            "registry_scope_id": sha256_schema(),
            "projection_verified": {"const": True},
            "release": descriptor,
            "deployment": descriptor,
            "airflow_index": descriptor,
        },
    }


def _materialize_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="airflow-cache-materialize",
        kind="dpone.airflow-cache-materialize.v1",
        title="dpone GitOps Airflow cache materialization report",
        required=(
            "schema",
            "passed",
            "changes",
            "errors",
            "status",
            "release_id",
            "deployment_id",
            "environment",
            "artifact_registry_ref",
            "downloaded_objects",
            "downloaded_bytes",
            "local_release_state",
            "local_deployment_state",
            "projection_verified",
            "activated",
        ),
        properties={
            **_common_properties("dpone.airflow-cache-materialize.v1", ("materialized", "no_op", "failed")),
            "downloaded_objects": non_negative_integer_schema(),
            "downloaded_bytes": non_negative_integer_schema(),
            "local_release_state": {"enum": ["created", "no_op", "not_installed"]},
            "local_deployment_state": {"enum": ["created", "no_op", "not_installed"]},
            "projection_verified": {"type": "boolean"},
            "activated": {"const": False},
        },
        additional_properties=False,
    )


def _loader_ack_v1_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="airflow-loader-ack",
        kind="dpone.airflow_loader_ack.v1",
        title="dpone GitOps legacy Airflow loader acknowledgement",
        required=(
            "schema",
            "release_id",
            "deployment_id",
            "airflow_index_sha256",
            "loaded_dag_ids",
            "skipped_dag_ids",
            "error_codes",
            "fatal",
            "acknowledged_at",
        ),
        properties={
            "schema": {"const": "dpone.airflow_loader_ack.v1"},
            "release_id": sha256_schema(),
            "deployment_id": sha256_schema(),
            "airflow_index_sha256": sha256_schema(),
            **_loader_ack_result_properties(),
        },
        additional_properties=False,
    )


def _loader_ack_v2_contract() -> GitOpsSchemaContract:
    return documented_contract(
        name="airflow-loader-ack-v2",
        kind="dpone.airflow_loader_ack.v2",
        title="dpone GitOps exact Airflow loader acknowledgement",
        required=(
            "schema",
            "release_id",
            "deployment_id",
            "airflow_index_sha256",
            "activation_id",
            "loaded_dag_ids",
            "skipped_dag_ids",
            "error_codes",
            "fatal",
            "acknowledged_at",
        ),
        properties={
            "schema": {"const": "dpone.airflow_loader_ack.v2"},
            "release_id": sha256_schema(),
            "deployment_id": sha256_schema(),
            "airflow_index_sha256": sha256_schema(),
            "activation_id": {
                "type": "string",
                "pattern": ("^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"),
            },
            **_loader_ack_result_properties(),
        },
        additional_properties=False,
    )


def _loader_ack_result_properties() -> dict[str, Any]:
    return {
        "loaded_dag_ids": _dag_id_list_schema(),
        "skipped_dag_ids": _dag_id_list_schema(),
        "error_codes": {
            "type": "array",
            "items": {
                "type": "string",
                "pattern": "^DPONE_[A-Z0-9_]+$",
            },
            "uniqueItems": True,
        },
        "fatal": {"type": "boolean"},
        "acknowledged_at": {
            "type": "string",
            "format": "date-time",
        },
    }


def _dag_id_list_schema() -> dict[str, Any]:
    return {
        "type": "array",
        "items": {
            "type": "string",
            "pattern": "^[A-Za-z0-9_.~-]+$",
        },
        "uniqueItems": True,
    }


def _common_properties(schema: str, statuses: tuple[str, ...]) -> dict[str, Any]:
    return {
        "schema": {"const": schema},
        "passed": {"type": "boolean"},
        "changes": {"type": "array", "items": {"type": "object"}},
        "errors": {"type": "array", "items": {"type": "object"}},
        "status": {"enum": list(statuses)},
        "release_id": nullable_schema(sha256_schema()),
        "deployment_id": nullable_schema(sha256_schema()),
        "environment": nullable_schema({"type": "string", "minLength": 1}),
        "artifact_registry_ref": nullable_schema({"type": "string", "minLength": 1}),
    }


def sha256_schema() -> dict[str, str]:
    return {"type": "string", "pattern": "^sha256:[0-9a-f]{64}$"}


def non_negative_integer_schema() -> dict[str, Any]:
    return {"type": "integer", "minimum": 0}


def nullable_schema(schema: dict[str, Any]) -> dict[str, Any]:
    return {"anyOf": [schema, {"type": "null"}]}


__all__ = ["airflow_artifact_delivery_schema_contracts"]
