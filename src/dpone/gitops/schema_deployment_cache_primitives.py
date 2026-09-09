"""Shared JSON-schema primitives for deployment-cache evidence."""

from __future__ import annotations

from typing import Any


def nullable_sha256_schema() -> dict[str, Any]:
    return {"anyOf": [{"$ref": "#/$defs/sha256"}, {"type": "null"}]}


def non_empty_string_schema() -> dict[str, Any]:
    return {"type": "string", "minLength": 1}


def status_file_name_schema() -> dict[str, Any]:
    return {"type": "string", "minLength": 1, "maxLength": 255, "pattern": "^[^/\\\\]+$"}


def sha256_schema() -> dict[str, str]:
    return {"type": "string", "pattern": "^sha256:[0-9a-f]{64}$"}


def uuid4_schema() -> dict[str, str]:
    return {
        "type": "string",
        "pattern": "^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
    }


__all__ = [
    "non_empty_string_schema",
    "nullable_sha256_schema",
    "sha256_schema",
    "status_file_name_schema",
    "uuid4_schema",
]
