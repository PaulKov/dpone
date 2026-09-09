"""Bounded query parameter definitions for the Studio OpenAPI contract."""

from __future__ import annotations

from typing import Any


def studio_query_parameters(operation_id: str) -> list[dict[str, Any]]:
    """Return bounded query parameters for a Studio operation."""

    if operation_id in {"pipelines", "audit_events", "runs"}:
        parameters = [
            {
                "name": "limit",
                "in": "query",
                "required": False,
                "schema": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 200,
                    "default": 100 if operation_id == "audit_events" else 200,
                },
            }
        ]
        if operation_id == "runs":
            parameters.append(
                {
                    "name": "cursor",
                    "in": "query",
                    "required": False,
                    "schema": {
                        "type": "integer",
                        "minimum": 0,
                        "default": 0,
                    },
                }
            )
        return parameters
    if operation_id == "state_inspect":
        return [_string_query_parameter(name) for name in ("backend", "state_type", "identity")]
    if operation_id == "schema_explorer":
        return [_string_query_parameter(name) for name in ("source", "sink")]
    return []


def _string_query_parameter(name: str) -> dict[str, Any]:
    return {
        "name": name,
        "in": "query",
        "required": False,
        "schema": {"type": "string", "minLength": 1},
    }


__all__ = ["studio_query_parameters"]
