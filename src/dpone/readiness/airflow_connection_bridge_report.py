"""Secret-free Airflow Connection operator-bridge reporting."""

from __future__ import annotations

from typing import Any

from dpone.airflow_connection_names import airflow_conn_env_name, is_valid_airflow_connection_id

_DEFAULT_SECRET_NAME = "dpone-airflow-connection-bridge"
_DEFAULT_MOUNT_ROOT = "/run/secrets/dpone/airflow-connections"


def airflow_connection_bridge_report(
    registry: dict[str, Any] | None,
    connection_ref_map: dict[str, str],
) -> dict[str, Any]:
    """Return the operator-side bridge intent required by registry entries."""

    connections = registry.get("connections") if isinstance(registry, dict) else None
    entries: list[dict[str, str]] = []
    if isinstance(connections, dict):
        for logical_ref, registry_ref in sorted(connection_ref_map.items()):
            connection = connections.get(registry_ref)
            if not isinstance(connection, dict):
                continue
            credentials = connection.get("credentials")
            if not isinstance(credentials, dict) or credentials.get("resolver") != "airflow_connection":
                continue
            connection_id = _text(credentials.get("connection_id"))
            if connection_id and is_valid_airflow_connection_id(connection_id):
                entries.append(
                    {
                        "connection_ref": logical_ref,
                        "registry_connection_ref": registry_ref,
                        "connection_id": connection_id,
                        "env_name": airflow_conn_env_name(connection_id),
                    }
                )
    required = bool(entries)
    return {
        "required": required,
        "execution_mode": "operator_bridge" if required else None,
        "resolver_location": "operator_execution" if required else None,
        "parse_safe": True,
        "secrets": False,
        "required_connection_ids": sorted({entry["connection_id"] for entry in entries}),
        "connections": entries,
        "projection": _projection(entries) if required else None,
        "next_actions": [_bridge_command()] if required else [],
    }


def _projection(entries: list[dict[str, str]]) -> dict[str, Any]:
    return {
        "mode": "kubernetes_secret_volume",
        "secret_name": _DEFAULT_SECRET_NAME,
        "mount_path": _DEFAULT_MOUNT_ROOT,
        "payload_format": "airflow_connection_uri",
        "secret_values": False,
        "connections": [
            {
                "connection_ref": entry["connection_ref"],
                "registry_connection_ref": entry["registry_connection_ref"],
                "connection_id": entry["connection_id"],
                "secret_key": entry["env_name"],
                "mount_path": f"{_DEFAULT_MOUNT_ROOT}/{entry['connection_ref']}",
                "fields": {"uri": "uri"},
            }
            for entry in entries
        ],
    }


def _bridge_command() -> str:
    return "dpone gitops airflow connection-bridge-plan --artifact-dir .dpone/gitops/airflow"


def _text(value: object) -> str:
    return str(value or "").strip()


__all__ = ["airflow_connection_bridge_report"]
