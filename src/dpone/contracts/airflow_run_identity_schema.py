from __future__ import annotations

from typing import Any

_DIGEST = {"type": "string", "pattern": r"^sha256:[0-9a-f]{64}$"}


def airflow_run_identity_schema() -> dict[str, Any]:
    """Return the closed canonical shape for one Airflow run identity."""

    return {
        "type": "object",
        "required": [
            "schema",
            "release_id",
            "deployment_id",
            "dag_spec",
            "workload_pack",
            "runtime_image_digest",
            "binding_set_ref",
            "connection_registry_ref",
            "credential_runtime_ref",
            "airflow_bundle",
        ],
        "additionalProperties": False,
        "properties": {
            "schema": {"const": "dpone.airflow-run-identity.v1"},
            "release_id": _DIGEST,
            "deployment_id": _DIGEST,
            "dag_spec": {"anyOf": [_artifact_identity_schema(), {"type": "null"}]},
            "workload_pack": _artifact_identity_schema(),
            "runtime_image_digest": _nullable_digest_schema(),
            "binding_set_ref": _nullable_digest_schema(),
            "connection_registry_ref": _nullable_digest_schema(),
            "credential_runtime_ref": _nullable_digest_schema(),
            "airflow_bundle": {"anyOf": [_bundle_identity_schema(), {"type": "null"}]},
        },
    }


def _artifact_identity_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["id", "sha256"],
        "additionalProperties": False,
        "properties": {
            "id": {"type": "string", "minLength": 1},
            "sha256": _DIGEST,
        },
    }


def _bundle_identity_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["backend", "ref", "versioned", "version", "snapshot_ref"],
        "additionalProperties": False,
        "properties": {
            "backend": {"type": "string", "minLength": 1},
            "ref": {
                "type": "string",
                "minLength": 1,
                "pattern": r"^\S+$",
                "not": {
                    "pattern": "(?:[?&](?:token|password|signature|credential|x-amz-signature|x-goog-signature)=|://[^/]*@)"
                },
            },
            "versioned": {"type": "boolean"},
            "version": {"anyOf": [{"type": "string", "minLength": 1}, {"type": "null"}]},
            "snapshot_ref": _nullable_digest_schema(),
        },
        "allOf": [
            {
                "if": {"properties": {"versioned": {"const": True}}},
                "then": {"properties": {"version": {"type": "string", "minLength": 1}}},
                "else": {"properties": {"version": {"type": "null"}}},
            }
        ],
    }


def _nullable_digest_schema() -> dict[str, Any]:
    return {"anyOf": [_DIGEST, {"type": "null"}]}


__all__ = ["airflow_run_identity_schema"]
