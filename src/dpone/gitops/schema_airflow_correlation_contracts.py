from __future__ import annotations

from typing import Any

from dpone.gitops.schema_contract_primitives import GitOpsSchemaContract, documented_contract

_DIGEST = {"type": "string", "pattern": r"^sha256:[0-9a-f]{64}$"}
_TEXT = {"type": "string", "minLength": 1, "maxLength": 1024}
_RESOURCE_TEXT = {"type": "string", "minLength": 1, "maxLength": 253}


def airflow_correlation_contract() -> GitOpsSchemaContract:
    schema = airflow_correlation_schema()
    return documented_contract(
        name="airflow-correlation",
        kind="dpone.airflow-correlation.v1",
        title="dpone GitOps Airflow attempt correlation contract",
        required=tuple(schema["required"]),
        properties=dict(schema["properties"]),
        additional_properties=False,
    )


def airflow_correlation_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": [
            "schema",
            "correlation_id",
            "attempt_ref",
            "airflow",
            "dpone",
            "artifacts",
            "pod",
        ],
        "additionalProperties": False,
        "properties": {
            "schema": {"const": "dpone.airflow-correlation.v1"},
            "correlation_id": _DIGEST,
            "attempt_ref": _DIGEST,
            "airflow": _airflow_attempt_schema(),
            "dpone": _dpone_run_schema(),
            "artifacts": _artifact_correlation_schema(),
            "pod": _pod_correlation_schema(),
        },
    }


def _airflow_attempt_schema() -> dict[str, Any]:
    return _strict_object(
        required=("dag_id", "task_id", "run_id", "try_number", "map_index"),
        properties={
            "dag_id": _TEXT,
            "task_id": _TEXT,
            "run_id": _TEXT,
            "try_number": {"type": "integer", "minimum": 1},
            "map_index": {"type": "integer", "minimum": -1},
        },
    )


def _dpone_run_schema() -> dict[str, Any]:
    return _strict_object(
        required=("run_id", "process"),
        properties={
            "run_id": _nullable(_TEXT),
            "process": _nullable(_RESOURCE_TEXT),
        },
    )


def _artifact_correlation_schema() -> dict[str, Any]:
    return _strict_object(
        required=(
            "release_id",
            "deployment_id",
            "workload_id",
            "workload_pack_sha256",
            "runtime_evidence_sha256",
        ),
        properties={
            "release_id": _DIGEST,
            "deployment_id": _DIGEST,
            "workload_id": _RESOURCE_TEXT,
            "workload_pack_sha256": _DIGEST,
            "runtime_evidence_sha256": _nullable(_DIGEST),
        },
    )


def _pod_correlation_schema() -> dict[str, Any]:
    return _strict_object(
        required=("name", "uid", "namespace", "image_digest"),
        properties={
            "name": _nullable(_RESOURCE_TEXT),
            "uid": _nullable(_TEXT),
            "namespace": _nullable(_RESOURCE_TEXT),
            "image_digest": _nullable(_DIGEST),
        },
    )


def _strict_object(*, required: tuple[str, ...], properties: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "object",
        "required": list(required),
        "additionalProperties": False,
        "properties": properties,
    }


def _nullable(schema: dict[str, Any]) -> dict[str, Any]:
    return {"anyOf": [schema, {"type": "null"}]}


__all__ = ["airflow_correlation_contract", "airflow_correlation_schema"]
