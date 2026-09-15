"""Authoring, normalized-intent and platform-policy JSON Schemas."""

from __future__ import annotations

from typing import Any

from dpone.contracts.dbt_publish_schema_contract_common import (
    IDENTIFIER,
    NONBLANK_TOKEN,
    QUALITY_PRESETS,
    STRATEGIES,
    object_schema,
)
from dpone.contracts.dbt_publish_schema_contract_intent import intent_v2_schema
from dpone.contracts.dbt_publish_schema_contract_policy import (
    policy_schema,
    policy_v2_schema,
    policy_v3_schema,
)


def authoring_schema_contracts() -> dict[str, dict[str, Any]]:
    """Return raw authoring-side contract bodies."""

    return {
        "dpone.dbt-publish-authoring.v1": _authoring(),
        "dpone.dbt-publish-intent.v2": intent_v2_schema(),
        "dpone.dbt-publish-policy.v1": policy_schema(),
        "dpone.dbt-publish-policy.v2": policy_v2_schema(),
        "dpone.dbt-publish-policy.v3": policy_v3_schema(),
    }


def _authoring() -> dict[str, Any]:
    schema = object_schema(
        ("enabled",),
        {
            "enabled": {"type": "boolean"},
            "profile": NONBLANK_TOKEN,
            "workflow": NONBLANK_TOKEN,
            "target": object_schema(
                (),
                {"schema": IDENTIFIER, "table": IDENTIFIER},
            ),
            "strategy": object_schema(
                (),
                {
                    "mode": {"enum": ["auto", *STRATEGIES]},
                    "unique_key": {
                        "type": "array",
                        "items": IDENTIFIER,
                        "uniqueItems": True,
                    },
                    "partition_key": IDENTIFIER,
                    "window_days": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 366,
                    },
                },
            ),
            "physical_design": object_schema(
                (),
                {
                    "profile": NONBLANK_TOKEN,
                    "order_by": {
                        "type": "array",
                        "items": IDENTIFIER,
                        "uniqueItems": True,
                    },
                },
            ),
            "execution": object_schema(
                (),
                {
                    "profile": NONBLANK_TOKEN,
                    "max_parallelism": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 1024,
                    },
                },
            ),
            "quality": object_schema(
                (),
                {"preset": {"enum": list(QUALITY_PRESETS)}},
            ),
            "lineage": object_schema((), {"enabled": {"type": "boolean"}}),
        },
    )
    schema["allOf"] = [
        {
            "if": {
                "properties": {"enabled": {"const": True}},
                "required": ["enabled"],
            },
            "then": {"required": ["profile", "workflow"]},
        }
    ]
    return schema


__all__ = ["authoring_schema_contracts"]
