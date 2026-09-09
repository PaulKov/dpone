"""JSON schemas for compact Airflow pack steps."""

from __future__ import annotations

from dpone.gitops.schema_airflow_init_fetch_execution import (
    bounded_execution_token_schema,
)
from dpone.gitops.schema_contract_primitives import (
    array_schema,
    boolean_schema,
    object_schema,
    string_schema,
)


def airflow_pack_step_schema() -> dict[str, object]:
    """Return the static step schema shared by pack and process plans."""

    return object_schema(
        required=(
            "name",
            "phase",
            "command",
            "required",
            "credential_required",
            "produces",
            "reason",
        ),
        properties={
            "name": string_schema(),
            "phase": string_schema(),
            "command": string_schema(),
            "required": boolean_schema(),
            "credential_required": boolean_schema(),
            "produces": array_schema(string_schema()),
            "reason": string_schema(),
            "depends_on": array_schema(string_schema()),
            "runtime_command": _pre_hook_runtime_command_schema(),
        },
    )


def _pre_hook_runtime_command_schema() -> dict[str, object]:
    return object_schema(
        additional_properties=False,
        required=(
            "schema",
            "hook_id",
            "process_selector",
            "argv",
        ),
        properties={
            "schema": {"const": "dpone.airflow-pre-hook-command.v1"},
            "hook_id": bounded_execution_token_schema(),
            "process_selector": {
                "anyOf": [
                    bounded_execution_token_schema(),
                    {"type": "null"},
                ]
            },
            "argv": {
                "type": "array",
                "minItems": 8,
                "maxItems": 10,
                "items": {"type": "string"},
            },
        },
    )


__all__ = ["airflow_pack_step_schema"]
