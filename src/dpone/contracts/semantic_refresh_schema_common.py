"""Deterministic JSON Schema building blocks for semantic-refresh contracts."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from dpone.contracts.semantic_refresh_types import (
    DATE_DOMAIN_MAX,
    DATE_DOMAIN_MIN,
    DATETIME_DOMAIN_MAX,
    DATETIME_DOMAIN_MIN,
    ClosureStatus,
    EffectiveKeyMapping,
    WorkflowMode,
)

DIGEST_SCHEMA: dict[str, object] = {
    "pattern": "^sha256:[0-9a-f]{64}$",
    "type": "string",
}
TEXT_SCHEMA: dict[str, object] = {"minLength": 1, "type": "string"}
POSITIVE_INTEGER_SCHEMA: dict[str, object] = {"minimum": 1, "type": "integer"}
UTC_DAY_SCHEMA: dict[str, object] = {
    "format": "date-time",
    "pattern": "T00:00:00Z$",
    "type": "string",
}


def closed_schema(
    schema_id: str,
    properties: Mapping[str, object],
    required: Sequence[str],
    *,
    comment: str | None = None,
    one_of: Sequence[Mapping[str, object]] | None = None,
) -> dict[str, Any]:
    """Build one draft-2020-12 closed object schema."""

    result: dict[str, Any] = {
        "$id": f"https://dpone.dev/schemas/{schema_id}.json",
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "additionalProperties": False,
        "properties": dict(properties),
        "required": list(required),
        "type": "object",
    }
    if comment is not None:
        result["$comment"] = comment
    if one_of is not None:
        result["oneOf"] = list(one_of)
    return result


def schema_discriminator(schema_id: str) -> dict[str, object]:
    """Return an exact schema-version discriminator."""

    return {"const": schema_id}


def string_set_schema(*, allow_empty: bool = False) -> dict[str, object]:
    """Describe a unique string array; Python additionally enforces sort order."""

    result: dict[str, object] = {
        "items": TEXT_SCHEMA,
        "type": "array",
        "uniqueItems": True,
    }
    if not allow_empty:
        result["minItems"] = 1
    return result


CLOSURE_STATUS_SCHEMA: dict[str, object] = {
    "enum": [item.value for item in ClosureStatus],
    "type": "string",
}
WORKFLOW_MODE_SCHEMA: dict[str, object] = {
    "enum": [item.value for item in WorkflowMode],
    "type": "string",
}


__all__ = [
    "CLOSURE_STATUS_SCHEMA",
    "DATE_DOMAIN_MAX",
    "DATE_DOMAIN_MIN",
    "DATETIME_DOMAIN_MAX",
    "DATETIME_DOMAIN_MIN",
    "DIGEST_SCHEMA",
    "EffectiveKeyMapping",
    "POSITIVE_INTEGER_SCHEMA",
    "TEXT_SCHEMA",
    "UTC_DAY_SCHEMA",
    "WORKFLOW_MODE_SCHEMA",
    "closed_schema",
    "schema_discriminator",
    "string_set_schema",
]
