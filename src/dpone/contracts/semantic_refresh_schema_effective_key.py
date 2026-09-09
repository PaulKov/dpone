"""Deterministic effective-key JSON Schema for semantic-refresh operations."""

from __future__ import annotations

from typing import Any

from dpone.contracts.semantic_refresh_schema_common import (
    DATE_DOMAIN_MAX,
    DATE_DOMAIN_MIN,
    DATETIME_DOMAIN_MAX,
    DATETIME_DOMAIN_MIN,
    DIGEST_SCHEMA,
    TEXT_SCHEMA,
    EffectiveKeyMapping,
)


def effective_key_schema() -> dict[str, Any]:
    """Return the closed injective MSSQL-to-ClickHouse key schema."""

    properties: dict[str, object] = {
        "domain_max": TEXT_SCHEMA,
        "domain_min": TEXT_SCHEMA,
        "mapping": {"enum": [item.value for item in EffectiveKeyMapping], "type": "string"},
        "name": TEXT_SCHEMA,
        "nullable": {"const": False},
        "source_type": TEXT_SCHEMA,
        "target_type": TEXT_SCHEMA,
        "utc_assurance_sha256": DIGEST_SCHEMA,
    }
    no_domain = {
        "not": {
            "anyOf": [
                {"required": ["domain_min"]},
                {"required": ["domain_max"]},
                {"required": ["utc_assurance_sha256"]},
            ]
        }
    }
    scalar_pairs = [
        ("bit", "Bool", EffectiveKeyMapping.BIT_BOOL),
        ("tinyint", "UInt8", EffectiveKeyMapping.INTEGER_WIDTH_PRESERVING),
        ("smallint", "Int16", EffectiveKeyMapping.INTEGER_WIDTH_PRESERVING),
        ("int", "Int32", EffectiveKeyMapping.INTEGER_WIDTH_PRESERVING),
        ("bigint", "Int64", EffectiveKeyMapping.INTEGER_WIDTH_PRESERVING),
        ("uniqueidentifier", "UUID", EffectiveKeyMapping.UUID_EQUALITY),
    ]
    variants: list[dict[str, object]] = [
        {
            **no_domain,
            "properties": {
                "mapping": {"const": mapping.value},
                "source_type": {"const": source},
                "target_type": {"const": target},
            },
        }
        for source, target, mapping in scalar_pairs
    ]
    variants.extend(
        [
            {
                "not": {"required": ["utc_assurance_sha256"]},
                "properties": {
                    "domain_max": {"const": DATE_DOMAIN_MAX},
                    "domain_min": {"const": DATE_DOMAIN_MIN},
                    "mapping": {"const": EffectiveKeyMapping.DATE.value},
                    "source_type": {"const": "date"},
                    "target_type": {"const": "Date"},
                },
                "required": ["domain_max", "domain_min"],
            },
            {
                "properties": {
                    "domain_max": {"const": DATETIME_DOMAIN_MAX},
                    "domain_min": {"const": DATETIME_DOMAIN_MIN},
                    "mapping": {"const": EffectiveKeyMapping.DATETIME2_UTC.value},
                    "source_type": {"const": "datetime2(6)"},
                    "target_type": {"const": "DateTime64(6,'UTC')"},
                },
                "required": ["domain_max", "domain_min", "utc_assurance_sha256"],
            },
            {
                **no_domain,
                "properties": {
                    "mapping": {"const": EffectiveKeyMapping.DECIMAL_EXACT.value},
                    "source_type": {"pattern": "^decimal\\((?:[1-9]|[12][0-9]|3[0-8]),[0-9]{1,2}\\)$"},
                    "target_type": {"pattern": "^Decimal\\((?:[1-9]|[12][0-9]|3[0-8]),[0-9]{1,2}\\)$"},
                },
            },
        ]
    )
    return {
        "additionalProperties": False,
        "oneOf": variants,
        "properties": properties,
        "required": ["mapping", "name", "nullable", "source_type", "target_type"],
        "type": "object",
    }


__all__ = ["effective_key_schema"]
