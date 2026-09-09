"""Airflow 3.3 task-state composition for exact runtime Pod launch pins."""

from __future__ import annotations

import inspect
from collections.abc import Mapping
from importlib import import_module
from typing import Any

from dpone_airflow_pack.launch_pin_codes import (
    PIN_INVALID,
    PIN_RECOVERY_REQUIRED,
    PIN_UNAVAILABLE,
)
from dpone_airflow_pack.launch_pin_selected_pod import (
    build_selected_pod_launch_pin,
    publish_launch_pin_locator,
)

KPO_POD_IDENTIFIER_STATE_KEY = "pod_identifier"
TASK_STATE_POINTER_PREFIX = "airflow-task-state:"


def configure_durable_kpo_kwargs(
    *,
    kpo_class: type[Any],
    kwargs: Mapping[str, Any],
) -> dict[str, Any]:
    """Require the Airflow 3.3/provider 10.20 durable KPO constructor."""

    configured = dict(kwargs)
    try:
        parameters = inspect.signature(kpo_class.__init__).parameters
    except (TypeError, ValueError) as exc:
        raise RuntimeError(f"{PIN_UNAVAILABLE}: cannot inspect KubernetesPodOperator durable capability") from exc
    if "durable" not in parameters:
        raise RuntimeError(
            f"{PIN_UNAVAILABLE}: airflow_task_state launch-pin backend requires "
            "Airflow 3.3 and apache-airflow-providers-cncf-kubernetes>=10.20 "
            "with KubernetesPodOperator(durable=...)"
        )
    if configured.get("durable") is False:
        raise RuntimeError(
            f"{PIN_INVALID}: airflow_task_state launch-pin backend requires KubernetesPodOperator durable=True"
        )
    if configured.get("reattach_on_restart") is False:
        raise RuntimeError(
            f"{PIN_INVALID}: airflow_task_state launch-pin backend is incompatible with reattach_on_restart=False"
        )
    configured["durable"] = True
    return configured


def commit_task_state_launch_pin_for_selected_pod(
    *,
    context: Any,
    pod: Any,
    operator: Any,
    kubernetes_conn_id: str | None = None,
    launch_pin_store: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Bind a selected Pod to KPO's persisted task-state identity.

    The task-state value is owned by KPO. dpone never writes or overwrites it;
    it verifies the persisted name/namespace, records the server UID, and
    publishes a bounded pointer claim that the gate later verifies against the
    live Pod authority.
    """

    if getattr(operator, "durable", None) is not True:
        raise RuntimeError(
            f"{PIN_INVALID}: airflow_task_state launch-pin backend requires "
            "KubernetesPodOperator durable=True at execution"
        )
    task_state = _task_state_accessor(context)
    selected = build_selected_pod_launch_pin(
        context=context,
        pod=pod,
        kubernetes_conn_id=kubernetes_conn_id,
        launch_pin_store=launch_pin_store,
    )
    _require_persisted_pod_identity(
        task_state=task_state,
        pod_namespace=str(selected.pin["pod_namespace"]),
        pod_name=str(selected.pin["pod_name"]),
    )
    pin = dict(selected.pin)
    pin["state"] = "ACTIVE"
    pin["pointer_resource_version"] = TASK_STATE_POINTER_PREFIX + str(pin["pin_sha256"])
    from dpone_airflow_pack.launch_pin_commit import build_launch_pin_ref

    ref = build_launch_pin_ref(pin)
    publish_launch_pin_locator(
        task_instance=selected.task_instance,
        pin=pin,
        launch_pin_ref=ref,
    )
    pin["launch_pin_ref"] = ref
    return pin


def _task_state_accessor(context: Any) -> Any:
    task_state = context.get("task_state_store") if isinstance(context, Mapping) else None
    if task_state is None or not callable(getattr(task_state, "get", None)):
        raise RuntimeError(
            f"{PIN_UNAVAILABLE}: airflow_task_state launch-pin backend requires "
            "the Airflow 3.3 task_state_store context accessor"
        )
    return task_state


def _require_persisted_pod_identity(
    *,
    task_state: Any,
    pod_namespace: str,
    pod_name: str,
) -> None:
    key = _provider_pod_identifier_key()
    try:
        stored = task_state.get(key)
    except Exception as exc:  # noqa: BLE001 - backend/API failures are unavailable
        raise RuntimeError(f"{PIN_UNAVAILABLE}: KPO durable Pod identity read from task state failed: {exc}") from exc
    if not isinstance(stored, Mapping):
        raise RuntimeError(f"{PIN_RECOVERY_REQUIRED}: KPO durable Pod identity is absent or malformed")
    stored_name = stored.get("name")
    stored_namespace = stored.get("namespace")
    if not isinstance(stored_name, str) or not stored_name.strip():
        raise RuntimeError(f"{PIN_RECOVERY_REQUIRED}: KPO durable Pod identity.name is invalid")
    if not isinstance(stored_namespace, str) or not stored_namespace.strip():
        raise RuntimeError(f"{PIN_RECOVERY_REQUIRED}: KPO durable Pod identity.namespace is invalid")
    if stored_name.strip() != pod_name or stored_namespace.strip() != pod_namespace:
        raise RuntimeError(f"{PIN_RECOVERY_REQUIRED}: KPO durable Pod identity does not match the selected Pod")


def _provider_pod_identifier_key() -> str:
    """Read the provider's stable key without making Airflow an import dependency."""

    requested = "airflow.providers.cncf.kubernetes.operators.pod"
    try:
        module = import_module(requested)
    except Exception:  # noqa: BLE001 - dependency-light unit paths use the stable key
        return KPO_POD_IDENTIFIER_STATE_KEY
    key = getattr(module, "POD_IDENTIFIER_STATE_KEY", KPO_POD_IDENTIFIER_STATE_KEY)
    if not isinstance(key, str) or not key.strip():
        raise RuntimeError(f"{PIN_UNAVAILABLE}: KubernetesPodOperator durable Pod state key is invalid")
    return key.strip()


__all__ = [
    "KPO_POD_IDENTIFIER_STATE_KEY",
    "TASK_STATE_POINTER_PREFIX",
    "commit_task_state_launch_pin_for_selected_pod",
    "configure_durable_kpo_kwargs",
]
