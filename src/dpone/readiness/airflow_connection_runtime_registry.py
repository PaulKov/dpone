"""Rewrite operator-bridge registry entries for in-pod RuntimeConnectionContext."""

from __future__ import annotations

import json
from collections.abc import Mapping
from copy import deepcopy
from typing import Any

from dpone.contracts.airflow_deployment import canonical_fingerprint
from dpone.contracts.credential_security import forbidden_inline_secret_paths
from dpone.readiness.airflow_connection_bridge_report import airflow_connection_bridge_report
from dpone.readiness.airflow_deployment_artifacts import json_bytes

_DEFAULT_SECRET_NAME = "dpone-airflow-connection-bridge"
_DEFAULT_MOUNT_ROOT = "/run/secrets/dpone/airflow-connections"


def runtime_connection_snapshots(
    *,
    binding_set: Mapping[str, Any],
    connection_registry: Mapping[str, Any],
    credential_runtime: Mapping[str, Any],
    projection: Mapping[str, Any] | None = None,
) -> dict[str, bytes]:
    """Build verified init-fetch connection-context payloads."""

    _require_non_secret_snapshot("connection_registry", connection_registry)
    _require_non_secret_snapshot("credential_runtime", credential_runtime)
    runtime_registry = runtime_connection_registry_for_init_fetch(
        connection_registry,
        projection=projection,
    )
    _require_non_secret_snapshot("runtime_connection_registry", runtime_registry)
    return {
        "binding_set": json_bytes(binding_set),
        "connection_registry": json_bytes(runtime_registry),
        "credential_runtime": json_bytes(credential_runtime),
    }


def runtime_connection_snapshot_fingerprint(payload: bytes) -> str:
    """Content-address the exact JSON bytes published for RuntimeConnectionContext."""

    return canonical_fingerprint(json.loads(payload.decode("utf-8")))


def runtime_connection_registry_for_init_fetch(
    connection_registry: Mapping[str, Any],
    *,
    projection: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Return a runtime registry snapshot pods can ``resolve()`` without Airflow.

    Git/source registries keep ``resolver: airflow_connection`` for offline check
    and bridge intent. Published init-fetch snapshots rewrite those entries to
    ``kubernetes_secret_volume`` mounts that match the operator bridge projection.
    """

    payload = deepcopy(dict(connection_registry))
    connections = payload.get("connections")
    if not isinstance(connections, Mapping) or not connections:
        return payload
    mounts = _mounts_by_registry_ref(connections, projection=projection)
    if not mounts:
        return payload
    rewritten: dict[str, Any] = {}
    for ref, entry in connections.items():
        if not isinstance(entry, Mapping):
            rewritten[str(ref)] = entry
            continue
        credentials = entry.get("credentials")
        if not isinstance(credentials, Mapping) or credentials.get("resolver") != "airflow_connection":
            rewritten[str(ref)] = deepcopy(dict(entry))
            continue
        mount = mounts.get(str(ref))
        if mount is None:
            rewritten[str(ref)] = deepcopy(dict(entry))
            continue
        next_entry = deepcopy(dict(entry))
        next_entry["credentials"] = {
            "resolver": "kubernetes_secret_volume",
            "secret_name": mount["secret_name"],
            "mount_path": mount["mount_path"],
            "payload_format": "airflow_connection_uri",
            "fields": {"uri": "uri"},
        }
        for key in ("version_policy", "resolution_scope"):
            if key in credentials:
                next_entry["credentials"][key] = credentials[key]
        rewritten[str(ref)] = next_entry
    payload["connections"] = rewritten
    return payload


def _require_non_secret_snapshot(label: str, payload: Mapping[str, Any]) -> None:
    paths = forbidden_inline_secret_paths(payload)
    if paths:
        raise ValueError(
            f"{label} contains forbidden inline secret fields (DPONE_SECRET_VALUE_IN_REGISTRY): {', '.join(paths)}"
        )


def _mounts_by_registry_ref(
    connections: Mapping[str, Any],
    *,
    projection: Mapping[str, Any] | None,
) -> dict[str, dict[str, str]]:
    if projection is not None:
        return _mounts_from_projection(projection)
    identity_map = {str(ref): str(ref) for ref in connections}
    report = airflow_connection_bridge_report(
        {"connections": dict(connections)},
        identity_map,
    )
    return _mounts_from_projection(report.get("projection"))


def _mounts_from_projection(projection: object) -> dict[str, dict[str, str]]:
    if not isinstance(projection, Mapping):
        return {}
    secret_name = str(projection.get("secret_name") or _DEFAULT_SECRET_NAME).strip() or _DEFAULT_SECRET_NAME
    entries = projection.get("connections")
    if not isinstance(entries, list):
        return {}
    mounts: dict[str, dict[str, str]] = {}
    for item in entries:
        if not isinstance(item, Mapping):
            continue
        registry_ref = str(item.get("registry_connection_ref") or item.get("connection_ref") or "").strip()
        mount_path = str(item.get("mount_path") or "").strip().rstrip("/")
        if not registry_ref or not mount_path:
            continue
        if not mount_path.startswith(_DEFAULT_MOUNT_ROOT):
            continue
        mounts[registry_ref] = {
            "secret_name": secret_name,
            "mount_path": mount_path,
        }
    return mounts


__all__ = [
    "runtime_connection_registry_for_init_fetch",
    "runtime_connection_snapshot_fingerprint",
    "runtime_connection_snapshots",
]
