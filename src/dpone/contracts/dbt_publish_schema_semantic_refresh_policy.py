"""JSON Schema fragment for the semantic-refresh V2 policy cell."""

from __future__ import annotations

from typing import Any

from dpone.contracts.dbt_publish_schema_contract_common import object_schema


def semantic_refresh_policy() -> dict[str, Any]:
    """Return the only platform-owned V2.0 capability cell."""

    return object_schema(
        (
            "schema",
            "enabled",
            "capability",
            "scope",
            "mutation",
            "initial_load",
            "concurrency",
            "source_snapshot",
            "publication",
            "workflow_publish_atomicity",
            "automatic_sql_retry",
        ),
        {
            "schema": {"const": "dpone.semantic-refresh-profile.v1"},
            "enabled": {"const": True},
            "capability": {"const": "scope_stable_event_fact"},
            "scope": object_schema(
                ("grain", "timezone", "interval"),
                {
                    "grain": {"const": "day"},
                    "timezone": {"const": "UTC"},
                    "interval": {"const": "half_open"},
                },
            ),
            "mutation": object_schema(
                ("protocol", "deletes"),
                {
                    "protocol": {"const": "update_insert_v1"},
                    "deletes": {"const": "ignore_missing"},
                },
            ),
            "initial_load": {"const": "require_existing_complete_relation"},
            "concurrency": {"const": "exclusive_workflow"},
            "source_snapshot": {"const": "snapshot"},
            "publication": object_schema(
                ("database_engine", "table_engine", "replica_count", "strategy"),
                {
                    "database_engine": {"const": "Atomic"},
                    "table_engine": {"const": "MergeTree"},
                    "replica_count": {"const": 1},
                    "strategy": {"const": "full_table_exchange"},
                },
            ),
            "workflow_publish_atomicity": {"const": "none"},
            "automatic_sql_retry": {"const": False},
        },
    )
