from __future__ import annotations

from typing import Any

from dpone.airflow_connection_names import AIRFLOW_CONN_ENV_NAME_PATTERN
from dpone.gitops.schema_contract_primitives import airflow_connection_id_schema
from dpone.gitops.schema_kubernetes_contracts import kubernetes_secret_name_schema


def airflow_connection_bridge_schema() -> dict[str, Any]:
    return _object_schema(
        required=("enabled", "mode", "runtime_mode", "required_connection_ids", "env"),
        properties={
            "enabled": {"type": "boolean"},
            "mode": _string_schema(),
            "runtime_mode": _string_schema(),
            "secret_name": kubernetes_secret_name_schema(nullable=True),
            "required_connection_ids": _array_schema(airflow_connection_id_schema()),
            "env": _array_schema(airflow_connection_env_ref_schema()),
            "warnings": _array_schema(_issue_ref()),
            "blockers": _array_schema(_issue_ref()),
        },
    )


def airflow_connection_env_ref_schema() -> dict[str, Any]:
    return _object_schema(
        required=("connection_id", "env_name"),
        properties={
            "connection_id": airflow_connection_id_schema(),
            "env_name": _airflow_conn_env_name_schema(),
            "secret_ref": _object_schema(
                required=("name", "key"),
                properties={"name": kubernetes_secret_name_schema(), "key": _airflow_conn_env_name_schema()},
            ),
        },
    )


def _object_schema(*, required: tuple[str, ...] = (), properties: dict[str, Any] | None = None) -> dict[str, Any]:
    schema: dict[str, Any] = {"type": "object", "additionalProperties": True}
    if required:
        schema["required"] = list(required)
    if properties is not None:
        schema["properties"] = properties
    return schema


def _array_schema(items: dict[str, Any]) -> dict[str, Any]:
    return {"type": "array", "items": items}


def _string_schema() -> dict[str, str]:
    return {"type": "string"}


def _airflow_conn_env_name_schema() -> dict[str, str]:
    return {"type": "string", "pattern": AIRFLOW_CONN_ENV_NAME_PATTERN}


def _issue_ref() -> dict[str, str]:
    return {"$ref": "#/$defs/issue"}


__all__ = ["airflow_connection_bridge_schema"]
