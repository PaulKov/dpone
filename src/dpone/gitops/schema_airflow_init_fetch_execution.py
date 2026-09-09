"""Versioned execution selection schema for Airflow init-fetch plans."""

from __future__ import annotations

from typing import Any


def init_fetch_execution_schema(*, explicit_hook_execution: bool = False) -> dict[str, Any]:
    """Build the execution wire schema for legacy or explicit hook ownership."""

    explicit_fields = (
        {
            "scope": {"enum": ["workload", "process"]},
            "process_selector": {
                "anyOf": [
                    bounded_execution_token_schema(),
                    {"type": "null"},
                ]
            },
            "hook_execution": {"enum": ["inline", "externalized"]},
        }
        if explicit_hook_execution
        else {}
    )
    schema: dict[str, Any] = {
        "type": "object",
        "required": [
            "kind",
            "selector",
            "hook_name",
            *(("scope", "process_selector", "hook_execution") if explicit_hook_execution else ()),
        ],
        "additionalProperties": False,
        "properties": {
            "kind": {"enum": ["runtime", "pre_hook"]},
            "selector": bounded_execution_token_schema(),
            "hook_name": {
                "anyOf": [
                    bounded_execution_token_schema(),
                    {"type": "null"},
                ]
            },
            **explicit_fields,
        },
    }
    schema["allOf"] = [
        {
            "if": {
                "properties": {"kind": {"const": "runtime"}},
                "required": ["kind"],
            },
            "then": {"properties": {"hook_name": {"type": "null"}}},
        },
        {
            "if": {
                "properties": {"kind": {"const": "pre_hook"}},
                "required": ["kind"],
            },
            "then": {
                "properties": {
                    "hook_name": bounded_execution_token_schema(),
                    **({"hook_execution": {"const": "externalized"}} if explicit_hook_execution else {}),
                },
            },
        },
        *(
            (
                {
                    "if": {
                        "properties": {"scope": {"const": "workload"}},
                        "required": ["scope"],
                    },
                    "then": {
                        "properties": {
                            "process_selector": {"type": "null"},
                            "hook_execution": {"const": "externalized"},
                        },
                    },
                },
            )
            if explicit_hook_execution
            else ()
        ),
    ]
    return schema


def bounded_execution_token_schema() -> dict[str, Any]:
    return {
        "type": "string",
        "pattern": "^[A-Za-z0-9_.:-]{1,256}$",
        "maxLength": 256,
    }


__all__ = [
    "bounded_execution_token_schema",
    "init_fetch_execution_schema",
]
