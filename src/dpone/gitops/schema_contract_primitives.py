from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from dpone.airflow_connection_names import AIRFLOW_CONNECTION_ID_PATTERN
from dpone.contracts.credential_env import CONNECTION_REF_MAX_LENGTH, CONNECTION_REF_PATTERN
from dpone.contracts.runtime_artifact_delivery import ARTIFACT_REGISTRY_REF_PATTERN

JSON_SCHEMA_DRAFT = "https://json-schema.org/draft/2020-12/schema"
SCHEMA_BASE_ID = "https://paulkov.github.io/dpone/schemas/gitops"
_MUTABLE_CACHE_ALIAS_PATTERN = r"(?:[Cc][Uu][Rr][Rr][Ee][Nn][Tt]|[Ll][Aa][Tt][Ee][Ss][Tt])"
PINNED_CACHE_REF_PATTERN = (
    rf"^cache://(?!{_MUTABLE_CACHE_ALIAS_PATTERN}(?:/|$))"
    rf"(?![^\s]*/{_MUTABLE_CACHE_ALIAS_PATTERN}(?:/|$))[^\s]+$"
)


@dataclass(frozen=True, slots=True)
class GitOpsSchemaContract:
    name: str
    kind: str
    schema: dict[str, Any]


def contract(
    *,
    name: str,
    kind: str,
    title: str,
    required: tuple[str, ...],
    properties: dict[str, Any],
) -> GitOpsSchemaContract:
    schema = {
        "$schema": JSON_SCHEMA_DRAFT,
        "$id": f"{SCHEMA_BASE_ID}/{name}.schema.json",
        "title": title,
        "type": "object",
        "required": list(required),
        "properties": properties,
        "$defs": {
            "issue": issue_schema(),
        },
        "additionalProperties": True,
    }
    return GitOpsSchemaContract(name=name, kind=kind, schema=schema)


def documented_contract(
    *,
    name: str,
    kind: str,
    title: str,
    required: tuple[str, ...],
    properties: dict[str, Any],
    defs: dict[str, Any] | None = None,
    additional_properties: bool = True,
) -> GitOpsSchemaContract:
    schema: dict[str, Any] = {
        "$schema": JSON_SCHEMA_DRAFT,
        "$id": f"{SCHEMA_BASE_ID}/{name}.schema.json",
        "title": title,
        "type": "object",
        "required": list(required),
        "additionalProperties": additional_properties,
        "properties": properties,
    }
    if defs is not None:
        schema["$defs"] = defs
    return GitOpsSchemaContract(name=name, kind=kind, schema=schema)


def issue_schema() -> dict[str, Any]:
    return object_schema(
        required=("code", "message", "path", "source"),
        properties={
            "code": string_schema(),
            "message": string_schema(),
            "path": string_schema(),
            "source": string_schema(),
        },
    )


def issue_ref() -> dict[str, str]:
    return {"$ref": "#/$defs/issue"}


def object_schema(
    *,
    required: tuple[str, ...] = (),
    properties: dict[str, Any] | None = None,
    additional_properties: bool = True,
) -> dict[str, Any]:
    schema: dict[str, Any] = {
        "type": "object",
        "additionalProperties": additional_properties,
    }
    if required:
        schema["required"] = list(required)
    if properties is not None:
        schema["properties"] = properties
    return schema


def array_schema(items: dict[str, Any]) -> dict[str, Any]:
    return {"type": "array", "items": items}


def string_schema() -> dict[str, str]:
    return {"type": "string"}


def non_empty_string_schema() -> dict[str, Any]:
    return {"type": "string", "minLength": 1}


def boolean_schema() -> dict[str, str]:
    return {"type": "boolean"}


def integer_schema() -> dict[str, str]:
    return {"type": "integer"}


def number_schema() -> dict[str, str]:
    return {"type": "number"}


def const_schema(value: str) -> dict[str, str]:
    return {"const": value}


def connection_ref_schema() -> dict[str, Any]:
    return {
        "type": "string",
        "pattern": CONNECTION_REF_PATTERN,
        "maxLength": CONNECTION_REF_MAX_LENGTH,
    }


def airflow_connection_id_schema() -> dict[str, str]:
    return {"type": "string", "pattern": AIRFLOW_CONNECTION_ID_PATTERN}


def pinned_cache_ref_schema() -> dict[str, str]:
    return {"type": "string", "pattern": PINNED_CACHE_REF_PATTERN}


def artifact_registry_ref_schema() -> dict[str, str]:
    return {"type": "string", "pattern": ARTIFACT_REGISTRY_REF_PATTERN}


def safe_volume_mount_path_schema() -> dict[str, Any]:
    return {
        "type": "string",
        "pattern": "^/run/secrets/dpone/\\S+$",
        "not": {"pattern": "(^|/)\\.\\.(/|$)"},
    }


__all__ = [
    "GitOpsSchemaContract",
    "ARTIFACT_REGISTRY_REF_PATTERN",
    "PINNED_CACHE_REF_PATTERN",
    "airflow_connection_id_schema",
    "array_schema",
    "artifact_registry_ref_schema",
    "boolean_schema",
    "connection_ref_schema",
    "const_schema",
    "contract",
    "documented_contract",
    "integer_schema",
    "issue_ref",
    "issue_schema",
    "number_schema",
    "non_empty_string_schema",
    "object_schema",
    "pinned_cache_ref_schema",
    "safe_volume_mount_path_schema",
    "string_schema",
]
