"""Shared JSON Schema fragments for pinned runtime connection artifacts."""

from __future__ import annotations

from typing import Any


def exact_runtime_artifact_schema(*, artifact_ref_schema: dict[str, Any]) -> dict[str, Any]:
    """Return the exact immutable artifact descriptor used across deployment wires."""

    return {
        "type": "object",
        "required": ["artifact_ref", "sha256", "bytes"],
        "additionalProperties": False,
        "properties": {
            "artifact_ref": artifact_ref_schema,
            "sha256": {"$ref": "#/$defs/sha256"},
            "bytes": {"type": "integer", "minimum": 1},
        },
    }


def runtime_connection_artifact_schema(filename: str) -> dict[str, Any]:
    """Constrain one descriptor to the content-addressed connection-context root."""

    escaped_filename = filename.replace(".", "[.]")
    return {
        "allOf": [
            {"$ref": "#/$defs/exactArtifact"},
            {
                "properties": {
                    "artifact_ref": {
                        "pattern": (f"^cache://runtime-connection-contexts/sha256-[0-9a-f]{{64}}/{escaped_filename}$")
                    }
                }
            },
        ]
    }


def runtime_pinned_cache_ref_schema() -> dict[str, str]:
    """Reject mutable aliases, traversal segments, whitespace, and URI ambiguity."""

    segment = r"[^/\\\s?#%]+"
    mutable = r"(?:[Cc][Uu][Rr][Rr][Ee][Nn][Tt]|[Ll][Aa][Tt][Ee][Ss][Tt])"
    return {
        "type": "string",
        "pattern": (
            rf"^cache://(?!(?:{segment}/)*{mutable}(?:/|$))"
            rf"(?!(?:{segment}/)*(?:\.|\.\.)(?:/|$)){segment}(?:/{segment})*$"
        ),
    }


__all__ = [
    "exact_runtime_artifact_schema",
    "runtime_connection_artifact_schema",
    "runtime_pinned_cache_ref_schema",
]
