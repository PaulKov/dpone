"""Stable legacy and opt-in native normalized intent schema bodies."""

from __future__ import annotations

from typing import Any

from dpone.contracts.dbt_publish_schema_contract_common import (
    IDENTIFIER,
    NONBLANK_TOKEN,
    QUALITY_PRESETS,
    STRATEGIES,
    nullable,
    object_schema,
)


def intent_v2_schema() -> dict[str, Any]:
    return object_schema(
        (
            "schema",
            "enabled",
            "profile",
            "workflow",
            "target",
            "strategy",
            "physical_design",
            "execution",
            "quality",
            "lineage",
        ),
        {
            "schema": {"const": "dpone.dbt-publish-intent.v2"},
            "enabled": {"type": "boolean"},
            "profile": NONBLANK_TOKEN,
            "workflow": NONBLANK_TOKEN,
            "target": object_schema(
                ("schema", "table"),
                {
                    "schema": nullable(IDENTIFIER),
                    "table": nullable(IDENTIFIER),
                },
            ),
            "strategy": object_schema(
                ("mode", "unique_key", "partition_key", "window_days"),
                {
                    "mode": {"enum": ["auto", *STRATEGIES]},
                    "unique_key": {
                        "type": "array",
                        "items": IDENTIFIER,
                        "uniqueItems": True,
                    },
                    "partition_key": nullable(IDENTIFIER),
                    "window_days": nullable(
                        {
                            "type": "integer",
                            "minimum": 1,
                            "maximum": 366,
                        }
                    ),
                },
            ),
            "physical_design": object_schema(
                ("profile", "engine", "order_by", "partition_by"),
                {
                    "profile": nullable(NONBLANK_TOKEN),
                    "engine": nullable(NONBLANK_TOKEN),
                    "order_by": {
                        "type": "array",
                        "items": IDENTIFIER,
                        "uniqueItems": True,
                    },
                    "partition_by": nullable(NONBLANK_TOKEN),
                },
            ),
            "execution": object_schema(
                ("profile", "max_parallelism"),
                {
                    "profile": nullable(NONBLANK_TOKEN),
                    "max_parallelism": nullable(
                        {
                            "type": "integer",
                            "minimum": 1,
                            "maximum": 1024,
                        }
                    ),
                },
            ),
            "quality": object_schema(
                ("preset",),
                {"preset": {"enum": list(QUALITY_PRESETS)}},
            ),
            "lineage": object_schema(
                ("enabled",),
                {"enabled": {"type": "boolean"}},
            ),
        },
    )
