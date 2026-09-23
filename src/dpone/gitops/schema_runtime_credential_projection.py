"""Closed credential-projection v1 and exact descriptor schema fragments."""

from __future__ import annotations

from typing import Any

from dpone.gitops.schema_contract_primitives import GitOpsSchemaContract, documented_contract


def credential_projection_descriptor_schema() -> dict[str, Any]:
    return _object(
        {
            "artifact_ref": {
                "type": "string",
                "pattern": r"^cache://runtime-credential-projections/sha256-[0-9a-f]{64}/credential-projection[.]json$",
            },
            "sha256": _digest(),
            "bytes": {"type": "integer", "minimum": 1, "maximum": 1048576},
        }
    )


def runtime_credential_projection_contract() -> GitOpsSchemaContract:
    member = _object(
        {"connection_ref": _token(), "registry_ref": _token(), "role": {"enum": ["workload", "workspace_control"]}}
    )
    source = _object(
        {
            "registry_ref": _token(),
            "connection_id": _token(),
            "secret_name": {"type": "string", "pattern": "^[a-z0-9](?:[-a-z0-9]{0,251}[a-z0-9])?$"},
            "secret_key": {"type": "string", "pattern": "^AIRFLOW_CONN_[A-Z0-9_]+$"},
            "mount_path": {
                "type": "string",
                "pattern": "^/run/secrets/dpone/airflow-connections/[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$",
            },
            "filename": {"const": "uri"},
        }
    )
    properties = {
        "schema": {"const": "dpone.runtime-credential-projection.v1"},
        "environment": _token(),
        **{
            field: _digest()
            for field in (
                "release_id",
                "binding_set_sha256",
                "source_registry_sha256",
                "runtime_registry_sha256",
                "publish_authority_sha256",
            )
        },
        "workspace_authority_connection_ref": _token(),
        "sources": {"type": "array", "maxItems": 1024, "items": source},
        "workloads": {
            "type": "array",
            "minItems": 1,
            "maxItems": 1024,
            "items": _object(
                {
                    "workload_id": _token(),
                    "connections": {"type": "array", "minItems": 1, "maxItems": 256, "items": member},
                }
            ),
        },
    }
    return documented_contract(
        name="runtime-credential-projection",
        kind="dpone.runtime-credential-projection.v1",
        title="dpone deployment-owned runtime credential projection",
        required=tuple(properties),
        properties=properties,
        additional_properties=False,
    )


def _object(properties: dict[str, Any]) -> dict[str, Any]:
    return {"type": "object", "additionalProperties": False, "required": list(properties), "properties": properties}


def _token() -> dict[str, str]:
    return {"type": "string", "pattern": "^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$"}


def _digest() -> dict[str, str]:
    return {"type": "string", "pattern": "^sha256:[0-9a-f]{64}$"}
