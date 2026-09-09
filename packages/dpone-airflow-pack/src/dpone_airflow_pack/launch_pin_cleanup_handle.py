"""Structural validation and construction for launch-pin cleanup handles."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

AIRFLOW_LAUNCH_PIN_CLEANUP_HANDLE_SCHEMA = "dpone.airflow-launch-pin-cleanup-handle.v1"


def _normalized_conn(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError("kubernetes_conn_id must be null or a non-empty string")
    stripped = value.strip()
    return stripped or None


def build_cleanup_handle_from_pin(
    pin: Mapping[str, Any],
    *,
    store_backend: str | None = None,
    store_kubernetes_conn_id: str | None = None,
    store_namespace: str | None = None,
) -> dict[str, Any]:
    """Build the occurrence-bound cleanup handle published by outcome_gate."""

    from dpone_airflow_pack.launch_pin_locator import (
        KUBERNETES_CONFIGMAP_BACKEND,
        store_authority_digest,
    )

    resource_version = pin.get("pointer_resource_version")
    pin_store_ns = pin.get("store_namespace")
    frozen_conn = _normalized_conn(store_kubernetes_conn_id)
    frozen_backend = str(store_backend or pin.get("store_backend") or KUBERNETES_CONFIGMAP_BACKEND)
    frozen_ns = (
        str(store_namespace).strip()
        if isinstance(store_namespace, str) and store_namespace.strip()
        else (str(pin_store_ns).strip() if isinstance(pin_store_ns, str) and str(pin_store_ns).strip() else None)
    )
    if frozen_ns is None:
        raise ValueError("cleanup handle requires a verified attempt-pinned store namespace")
    store = {
        "backend": frozen_backend,
        "kubernetes_conn_id": frozen_conn,
        "namespace": frozen_ns,
        "authority_digest": store_authority_digest(
            backend=frozen_backend,
            kubernetes_conn_id=frozen_conn,
            namespace=frozen_ns,
        ),
    }
    return {
        "schema": AIRFLOW_LAUNCH_PIN_CLEANUP_HANDLE_SCHEMA,
        "try_number": int(pin["try_number"]),
        "pod_uid": str(pin["pod_uid"]),
        "pod_namespace": str(pin["pod_namespace"]),
        "pod_name": str(pin["pod_name"]),
        "pin_sha256": str(pin["pin_sha256"]),
        "dag_id": str(pin.get("dag_id") or ""),
        "run_id": str(pin.get("run_id") or ""),
        "task_id": str(pin.get("task_id") or ""),
        "map_index": int(pin.get("map_index", -1)),
        "kubernetes_conn_id": frozen_conn,
        "store_backend": frozen_backend,
        "pointer_resource_version": (
            str(resource_version).strip() if isinstance(resource_version, str) and resource_version.strip() else None
        ),
        "store": store,
    }


def cleanup_handle_error(value: object) -> str:
    """Return a diagnostic when a cleanup handle is structurally invalid."""

    if not isinstance(value, Mapping):
        return "cleanup handle must be an object"
    if value.get("schema") != AIRFLOW_LAUNCH_PIN_CLEANUP_HANDLE_SCHEMA:
        return "cleanup handle schema is invalid"
    for field in ("pod_namespace", "pod_name", "pod_uid", "pin_sha256"):
        if not isinstance(value.get(field), str) or not str(value.get(field)).strip():
            return f"cleanup handle.{field} must be a non-empty string"
    if not isinstance(value.get("try_number"), int) or isinstance(value.get("try_number"), bool):
        return "cleanup handle.try_number must be an integer"
    if int(value["try_number"]) < 1:
        return "cleanup handle.try_number must be >= 1"
    for field in ("dag_id", "run_id", "task_id"):
        if not isinstance(value.get(field), str) or not str(value.get(field)).strip():
            return f"cleanup handle.{field} must be a non-empty string"
    map_index = value.get("map_index")
    if not isinstance(map_index, int) or isinstance(map_index, bool):
        return "cleanup handle.map_index must be an integer"
    resource_version = value.get("pointer_resource_version")
    if not isinstance(resource_version, str) or not resource_version.strip():
        return "cleanup handle.pointer_resource_version must be a non-empty string"
    store = value.get("store")
    if not isinstance(store, Mapping):
        return "cleanup handle.store must be an object"
    frozen_ns = store.get("namespace")
    if not isinstance(frozen_ns, str) or not frozen_ns.strip():
        return "cleanup handle.store.namespace must be a non-empty string"
    from dpone_airflow_pack.launch_pin_locator import (
        KUBERNETES_CONFIGMAP_BACKEND,
        LAUNCH_PIN_STORE_BACKENDS,
        store_authority_digest,
    )

    backend = str(store.get("backend") or KUBERNETES_CONFIGMAP_BACKEND)
    if backend not in LAUNCH_PIN_STORE_BACKENDS:
        return "cleanup handle.store.backend is invalid"
    top_backend = str(value.get("store_backend") or KUBERNETES_CONFIGMAP_BACKEND)
    if top_backend != backend:
        return "cleanup handle.store_backend must equal cleanup handle.store.backend"

    try:
        store_conn = _normalized_conn(store.get("kubernetes_conn_id"))
        top_conn = _normalized_conn(value.get("kubernetes_conn_id"))
    except TypeError as exc:
        return str(exc)
    if top_conn != store_conn:
        return "cleanup handle.kubernetes_conn_id must equal cleanup handle.store.kubernetes_conn_id"
    expected_digest = store_authority_digest(
        backend=backend,
        kubernetes_conn_id=store_conn,
        namespace=frozen_ns.strip(),
    )
    observed_digest = store.get("authority_digest")
    if not isinstance(observed_digest, str) or observed_digest != expected_digest:
        return "cleanup handle.store.authority_digest does not match frozen store coordinates"
    return ""


__all__ = [
    "AIRFLOW_LAUNCH_PIN_CLEANUP_HANDLE_SCHEMA",
    "build_cleanup_handle_from_pin",
    "cleanup_handle_error",
]
