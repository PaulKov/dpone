"""Compatibility-preserving compact promotion schema for release-set v1."""

from __future__ import annotations

from typing import Any

COMPACT_PROMOTION_SCHEMA = "dpone.compact-pack-release-promotion.v1"
COMPACT_PROMOTION_PROFILE = "compact_v2_runtime_connection_context"
COMPACT_PROMOTION_SCHEMA_NAMESPACE = r"^dpone\.compact-pack-release-promotion(?:\.|$)"
COMPACT_PROMOTION_PROFILE_NAMESPACE = r"^compact_v[0-9]+_runtime_connection_context(?:_|$)"


def compact_pack_promotion_guards() -> tuple[dict[str, Any], ...]:
    """Validate only payloads that explicitly signal the dpone compact namespace."""

    return (
        _promotion_guard("schema", COMPACT_PROMOTION_SCHEMA_NAMESPACE),
        _promotion_guard("profile", COMPACT_PROMOTION_PROFILE_NAMESPACE),
    )


def _promotion_guard(field: str, pattern: str) -> dict[str, Any]:
    return {
        "if": {
            "type": "object",
            "required": ["promotion"],
            "properties": {
                "promotion": {
                    "type": "object",
                    "required": [field],
                    "properties": {
                        field: {
                            "type": "string",
                            "pattern": pattern,
                        },
                    },
                }
            },
        },
        "then": {
            "properties": {
                "promotion": {
                    "type": "object",
                    "required": ["schema", "profile"],
                    "additionalProperties": False,
                    "properties": {
                        "schema": {"const": COMPACT_PROMOTION_SCHEMA},
                        "profile": {"const": COMPACT_PROMOTION_PROFILE},
                    },
                }
            }
        },
    }


__all__ = [
    "COMPACT_PROMOTION_PROFILE",
    "COMPACT_PROMOTION_PROFILE_NAMESPACE",
    "COMPACT_PROMOTION_SCHEMA",
    "COMPACT_PROMOTION_SCHEMA_NAMESPACE",
    "compact_pack_promotion_guards",
]
