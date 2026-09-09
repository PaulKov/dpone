"""Shared JSON Schema vocabulary for dbt publishing contracts."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

DRAFT = "https://json-schema.org/draft/2020-12/schema"
DIGEST: dict[str, Any] = {"type": "string", "pattern": "^sha256:[0-9a-f]{64}$"}
TOKEN = {"type": "string", "minLength": 1, "maxLength": 256}
NONBLANK_TOKEN = {**TOKEN, "pattern": ".*\\S.*"}
IDENTIFIER = {
    **TOKEN,
    "maxLength": 128,
    "pattern": "^[A-Za-z_][A-Za-z0-9_]*$",
}
STRATEGIES = ("full_refresh", "incremental_merge", "partition_replace")
QUALITY_PRESETS = ("standard", "strict")
RELATIVE = {
    "type": "string",
    "minLength": 1,
    "maxLength": 1024,
    "pattern": "^(?!/)(?!.*(?:^|/)\\.\\.(?:/|$))(?!.*\\\\).+$",
}


def document(contract_id: str, schema: dict[str, Any]) -> dict[str, Any]:
    """Wrap one contract body in the canonical schema document metadata."""

    return {
        "$schema": DRAFT,
        "$id": f"https://dpone.dev/schemas/{contract_id}.json",
        **schema,
    }


def object_schema(
    required: tuple[str, ...],
    properties: dict[str, Any],
    *,
    additional: bool = False,
) -> dict[str, Any]:
    """Return one closed object schema unless extension is explicitly allowed."""

    return {
        "type": "object",
        "required": list(required),
        "additionalProperties": additional,
        "properties": properties,
    }


def nullable(schema: dict[str, Any]) -> dict[str, Any]:
    """Return a non-mutating nullable projection of one schema."""

    return {"anyOf": [deepcopy(schema), {"type": "null"}]}


def nullable_fields(names: tuple[str, ...]) -> dict[str, Any]:
    """Return a closed object whose required fields are nullable tokens."""

    return object_schema(names, {name: nullable(TOKEN) for name in names})


def nonempty_tokens() -> dict[str, Any]:
    """Return a unique non-empty token-array schema."""

    return {
        "type": "array",
        "minItems": 1,
        "uniqueItems": True,
        "items": TOKEN,
    }


__all__ = [
    "DIGEST",
    "DRAFT",
    "IDENTIFIER",
    "NONBLANK_TOKEN",
    "QUALITY_PRESETS",
    "RELATIVE",
    "STRATEGIES",
    "TOKEN",
    "document",
    "nonempty_tokens",
    "nullable",
    "nullable_fields",
    "object_schema",
]
