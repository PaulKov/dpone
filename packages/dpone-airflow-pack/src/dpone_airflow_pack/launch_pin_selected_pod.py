"""Common construction and XCom publication for one selected runtime Pod."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from dpone_airflow_pack.launch_pin_codes import (
    PIN_INVALID,
    PIN_RECOVERY_REQUIRED,
    PIN_UNAVAILABLE,
)
from dpone_airflow_pack.launch_pin_envelope import launch_envelope_from_pod, pod_coordinates
from dpone_airflow_pack.launch_pin_locator import (
    LaunchPinStoreLocator,
    launch_pin_store_locator_to_mapping,
    require_same_cluster_launch_pin_store,
    store_authority_digest,
)
from dpone_airflow_pack.launch_pin_pod import remember_launch_pin_pod
from dpone_airflow_pack.launch_pin_resolve import AIRFLOW_RUNTIME_LAUNCH_PIN_XCOM_KEY


@dataclass(frozen=True)
class SelectedPodLaunchPin:
    """Validated pointer candidate and its frozen authority coordinates."""

    pin: dict[str, Any]
    task_instance: Any
    kubernetes_conn_id: str | None
    locator: LaunchPinStoreLocator


def build_selected_pod_launch_pin(
    *,
    context: Any,
    pod: Any,
    kubernetes_conn_id: str | None = None,
    launch_pin_store: Mapping[str, Any] | None = None,
) -> SelectedPodLaunchPin:
    """Build one full pin from task coordinates and immutable Pod authority."""

    from dpone_airflow_pack.launch_pin import build_launch_pin, launch_pin_error

    ti = context.get("ti") if isinstance(context, Mapping) else None
    if ti is None:
        raise RuntimeError(f"{PIN_UNAVAILABLE}: Airflow task instance is required")
    pod_namespace, pod_name, pod_uid = pod_coordinates(pod)
    remember_launch_pin_pod(pod)
    envelope = launch_envelope_from_pod(pod)
    normalized_conn = normalize_kubernetes_conn_id(
        kubernetes_conn_id,
        context=context,
    )
    closed = require_same_cluster_launch_pin_store(
        kpo_kubernetes_conn_id=normalized_conn,
        launch_pin_store=launch_pin_store,
    )
    pin = build_launch_pin(
        attempt=attempt_coordinates(ti),
        pod_namespace=pod_namespace,
        pod_name=pod_name,
        pod_uid=pod_uid,
        kubernetes_conn_id=closed.kubernetes_conn_id or normalized_conn,
        run_identity=envelope["run_identity"],
        deployment_identity=envelope["deployment_identity"],
        expected_runtime_evidence_sha256=envelope["expected_runtime_evidence_sha256"],
    )
    pin["store_backend"] = closed.backend
    pin["store_namespace"] = closed.namespace
    pin["store_authority_digest"] = store_authority_digest(
        backend=closed.backend,
        kubernetes_conn_id=closed.kubernetes_conn_id,
        namespace=closed.namespace,
    )
    identity_error = launch_pin_error(
        pin,
        require_digest=True,
        require_identities=True,
    )
    if identity_error:
        raise RuntimeError(f"{PIN_INVALID}: {identity_error}")
    return SelectedPodLaunchPin(
        pin=pin,
        task_instance=ti,
        kubernetes_conn_id=normalized_conn,
        locator=closed,
    )


def publish_launch_pin_locator(
    *,
    task_instance: Any,
    pin: Mapping[str, Any],
    launch_pin_ref: Mapping[str, Any],
) -> None:
    """Publish the bounded pointer claim consumed by the downstream gate."""

    if not hasattr(task_instance, "xcom_push"):
        raise RuntimeError(f"{PIN_RECOVERY_REQUIRED}: launch pin locator XCom push requires task instance xcom_push")
    store_conn = pin.get("kubernetes_conn_id")
    store_ns = pin.get("store_namespace")
    store_backend = str(pin.get("store_backend") or "kubernetes_configmap")
    closed_store = launch_pin_store_locator_to_mapping(
        LaunchPinStoreLocator(
            kubernetes_conn_id=(
                str(store_conn).strip() if isinstance(store_conn, str) and str(store_conn).strip() else None
            ),
            namespace=(str(store_ns).strip() if isinstance(store_ns, str) and str(store_ns).strip() else None),
            backend=store_backend,
        )
    )
    locator = {
        **dict(launch_pin_ref),
        "pod_namespace": str(pin["pod_namespace"]),
        "pod_name": str(pin["pod_name"]),
        "dag_id": str(pin["dag_id"]),
        "run_id": str(pin["run_id"]),
        "task_id": str(pin["task_id"]),
        "map_index": int(pin["map_index"]),
        "kubernetes_conn_id": pin.get("kubernetes_conn_id"),
        "store_backend": store_backend,
        "store_namespace": pin.get("store_namespace"),
        "store_authority_digest": pin.get("store_authority_digest"),
        "launch_pin_store": closed_store,
        "state": pin.get("state"),
        "schema": pin.get("schema"),
        "run_identity": pin.get("run_identity"),
        "deployment_identity": pin.get("deployment_identity"),
        "expected_runtime_evidence_sha256": pin.get("expected_runtime_evidence_sha256"),
        "launch_pin_ref": dict(launch_pin_ref),
    }
    try:
        task_instance.xcom_push(
            key=AIRFLOW_RUNTIME_LAUNCH_PIN_XCOM_KEY,
            value=locator,
        )
    except Exception as exc:  # noqa: BLE001 - ACTIVE pointer must be transferable
        raise RuntimeError(
            f"{PIN_RECOVERY_REQUIRED}: launch pin locator XCom publish failed after ACTIVE: {exc}"
        ) from exc


def normalize_kubernetes_conn_id(
    kubernetes_conn_id: str | None,
    *,
    context: Any,
) -> str | None:
    """Resolve the operator connection without importing Airflow metadata APIs."""

    if isinstance(kubernetes_conn_id, str) and kubernetes_conn_id.strip():
        return kubernetes_conn_id.strip()
    task = context.get("task") if isinstance(context, Mapping) else None
    value = getattr(task, "kubernetes_conn_id", None)
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def attempt_coordinates(ti: Any) -> dict[str, Any]:
    """Return exact Airflow task-attempt coordinates from a task instance."""

    return {
        "dag_id": str(getattr(ti, "dag_id", "") or ""),
        "run_id": str(getattr(ti, "run_id", "") or getattr(ti, "dag_run_id", "") or ""),
        "task_id": str(getattr(ti, "task_id", "") or ""),
        "map_index": int(getattr(ti, "map_index", -1)),
        "try_number": int(getattr(ti, "try_number", 1) or 1),
    }


__all__ = [
    "SelectedPodLaunchPin",
    "attempt_coordinates",
    "build_selected_pod_launch_pin",
    "normalize_kubernetes_conn_id",
    "publish_launch_pin_locator",
]
