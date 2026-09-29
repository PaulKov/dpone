"""Closed connector pairing rule for dbt-published MSSQL row streams."""

from __future__ import annotations

from typing import Any


def validated_stream_profile_condition() -> list[dict[str, Any]]:
    """Constrain the opt-in mode without changing legacy policy versions."""

    return [
        {
            "if": {
                "properties": {
                    "source": {
                        "properties": {"options": {"required": ["mssql_export_mode"]}},
                        "required": ["options"],
                    }
                }
            },
            "then": {
                "properties": {
                    "source": {"properties": {"type": {"const": "mssql"}}},
                    "sink": {"properties": {"type": {"const": "clickhouse"}}},
                }
            },
        }
    ]


__all__ = ["validated_stream_profile_condition"]
