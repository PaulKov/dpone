"""Closed Airflow Connection bridge contract for strict init-fetch packs."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone_airflow_pack.connection_names import (
    require_airflow_conn_env_name,
    require_airflow_connection_id,
    require_kubernetes_dns_label,
)
from dpone_airflow_pack.init_fetch_contract import InitFetchProviderError

_MOUNT_ROOT_PREFIX = "/run/secrets/dpone/"
_CLEANUP_POLICIES = frozenset({"after_execute", "retain"})


def is_closed_init_fetch_connection_bridge(projection: object) -> bool:
    """Return whether *projection* is the approved strict init-fetch bridge."""

    try:
        require_closed_init_fetch_connection_bridge(projection)
    except InitFetchProviderError:
        return False
    return True


def require_closed_init_fetch_connection_bridge(projection: object) -> Mapping[str, Any]:
    """Validate and return the closed bridge projection or fail closed."""

    if not isinstance(projection, Mapping) or not projection:
        raise _bridge_error("strict init-fetch connection_projection must be a non-empty object")
    mode = str(projection.get("mode") or "").strip()
    if mode == "unsafe_airflow_env":
        raise _bridge_error(
            "strict init-fetch rejects unsafe_airflow_env; use kubernetes_secret_volume "
            "airflow_connection_uri bridge projection"
        )
    if mode != "kubernetes_secret_volume":
        raise _bridge_error("strict init-fetch connection_projection.mode must be kubernetes_secret_volume")
    if str(projection.get("payload_format") or "").strip() != "airflow_connection_uri":
        raise _bridge_error("strict init-fetch connection_projection.payload_format must be airflow_connection_uri")
    if projection.get("secret_values") not in (None, False):
        raise _bridge_error("strict init-fetch connection_projection.secret_values must be false")
    cleanup_policy = str(projection.get("cleanup_policy") or "after_execute").strip()
    if cleanup_policy not in _CLEANUP_POLICIES:
        raise _bridge_error("strict init-fetch connection_projection.cleanup_policy is invalid")
    try:
        secret_name = require_kubernetes_dns_label(
            str(projection.get("secret_name") or "").strip(),
            context="strict init-fetch connection_projection.secret_name",
        )
    except ValueError as exc:
        raise _bridge_error(str(exc)) from exc
    mount_path = str(projection.get("mount_path") or "").strip().rstrip("/")
    if not mount_path.startswith(_MOUNT_ROOT_PREFIX):
        raise _bridge_error("strict init-fetch connection_projection.mount_path must stay under /run/secrets/dpone/")
    connections = projection.get("connections")
    if not isinstance(connections, list) or not connections:
        raise _bridge_error("strict init-fetch connection_projection.connections must be a non-empty array")
    normalized_connections = tuple(_require_connection_entry(item, root=mount_path) for item in connections)
    closed: dict[str, Any] = {
        "mode": "kubernetes_secret_volume",
        "secret_name": secret_name,
        "mount_path": mount_path,
        "payload_format": "airflow_connection_uri",
        "secret_values": False,
        "cleanup_policy": cleanup_policy,
        "connections": [dict(item) for item in normalized_connections],
    }
    # Preserve operator-side URI overrides (MSSQL TrustServerCertificate, CH scheme/db).
    query_overrides = projection.get("query_overrides")
    if isinstance(query_overrides, Mapping) and query_overrides:
        closed["query_overrides"] = {
            str(conn_id): {str(item_key): str(item_value) for item_key, item_value in values.items()}
            for conn_id, values in query_overrides.items()
            if isinstance(values, Mapping)
        }
    for key in ("scheme_overrides", "database_overrides"):
        value = projection.get(key)
        if isinstance(value, Mapping) and value:
            closed[key] = {str(conn_id): str(raw) for conn_id, raw in value.items() if str(raw).strip()}
    return closed


def _require_connection_entry(item: object, *, root: str) -> Mapping[str, Any]:
    if not isinstance(item, Mapping):
        raise _bridge_error("strict init-fetch connection_projection.connections entries must be objects")
    try:
        connection_id = require_airflow_connection_id(
            str(item.get("connection_id") or "").strip(),
            context="strict init-fetch connection_projection.connection_id",
        )
        secret_key = require_airflow_conn_env_name(
            str(item.get("secret_key") or "").strip(),
            context="strict init-fetch connection_projection.secret_key",
        )
    except ValueError as exc:
        raise _bridge_error(str(exc)) from exc
    entry_mount = str(item.get("mount_path") or "").strip().rstrip("/")
    if not entry_mount.startswith(root + "/"):
        raise _bridge_error("strict init-fetch connection entry mount_path must stay under projection mount_path")
    fields = item.get("fields")
    if not isinstance(fields, Mapping) or str(fields.get("uri") or "").strip() != "uri":
        raise _bridge_error("strict init-fetch connection entry fields.uri must be 'uri'")
    connection_ref = str(item.get("connection_ref") or connection_id).strip()
    registry_connection_ref = str(item.get("registry_connection_ref") or connection_ref).strip()
    if not connection_ref or not registry_connection_ref:
        raise _bridge_error("strict init-fetch connection entry requires logical connection refs")
    return {
        "connection_ref": connection_ref,
        "registry_connection_ref": registry_connection_ref,
        "connection_id": connection_id,
        "secret_key": secret_key,
        "mount_path": entry_mount,
        "fields": {"uri": "uri"},
    }


def _bridge_error(message: str) -> InitFetchProviderError:
    return InitFetchProviderError(
        "DPONE_INIT_FETCH_CONNECTION_BRIDGE_INVALID",
        message,
    )


__all__ = [
    "is_closed_init_fetch_connection_bridge",
    "require_closed_init_fetch_connection_bridge",
]
