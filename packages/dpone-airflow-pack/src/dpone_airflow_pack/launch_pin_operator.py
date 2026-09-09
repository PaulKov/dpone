"""Operator lifecycle composition for backend-neutral launch pins."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone_airflow_pack.launch_pin import (
    ensure_launch_envelope_on_pod,
    maybe_commit_launch_pin_for_selected_pod,
)
from dpone_airflow_pack.launch_pin_barrier import attach_launch_pin_cas_barrier
from dpone_airflow_pack.launch_pin_commit import remember_committed_launch_pin_ref
from dpone_airflow_pack.launch_pin_locator import (
    AIRFLOW_TASK_STATE_BACKEND,
    close_launch_pin_store_locator,
    frozen_launch_pin_store_locator,
)
from dpone_airflow_pack.launch_pin_pod import ensure_selected_pod_with_uid
from dpone_airflow_pack.launch_pin_task_state import (
    commit_task_state_launch_pin_for_selected_pod,
)


def prepare_operator_launch_pin_pod(
    *,
    pod: Any,
    operator: Any,
    context: Any,
) -> Any:
    """Freeze launch-pin authority and inject the immutable Pod envelope."""

    if not _pin_enabled(operator):
        return pod
    locator = getattr(operator, "launch_pin_store", None)
    operator.launch_pin_store = close_launch_pin_store_locator(
        launch_pin_store=locator,
        kpo_kubernetes_conn_id=_operator_conn_id(operator),
        kpo_namespace=_operator_namespace(operator),
    )
    pod = ensure_launch_envelope_on_pod(
        pod,
        run_identity=getattr(operator, "expected_run_identity", None),
        deployment_identity=getattr(operator, "expected_deployment_identity", None),
        expected_runtime_evidence_sha256=getattr(
            operator,
            "expected_runtime_evidence_sha256",
            None,
        ),
    )
    return pod


def attach_operator_launch_pin_barrier(
    *,
    pod: Any,
    operator: Any,
    context: Any,
) -> Any:
    """Attach the ConfigMap barrier after strict Pod topology validation."""

    if not _pin_enabled(operator) or _operator_backend(operator) == AIRFLOW_TASK_STATE_BACKEND:
        return pod
    # Strict init-fetch validates exactly one provider init container before
    # the ConfigMap CAS barrier adds a second init container.
    return attach_launch_pin_cas_barrier(
        pod=pod,
        operator=operator,
        context=context,
    )


def commit_operator_launch_pin(
    *,
    pod: Any,
    operator: Any,
    context: Any,
) -> Any:
    """Hydrate the selected occurrence and dispatch its backend commit."""

    if not _pin_enabled(operator):
        return pod
    selected = ensure_selected_pod_with_uid(pod=pod, operator=operator)
    locator = getattr(operator, "launch_pin_store", None)
    launch_pin_store = locator if isinstance(locator, Mapping) else None
    retained: Mapping[str, Any] | None
    if _operator_backend(operator) == AIRFLOW_TASK_STATE_BACKEND:
        retained = commit_task_state_launch_pin_for_selected_pod(
            context=context,
            pod=selected,
            operator=operator,
            kubernetes_conn_id=_operator_conn_id(operator),
            launch_pin_store=launch_pin_store,
        )
    else:
        retained = maybe_commit_launch_pin_for_selected_pod(
            enabled=True,
            context=context,
            pod=selected,
            kubernetes_conn_id=_operator_conn_id(operator),
            launch_pin_store=launch_pin_store,
        )
    remember_committed_launch_pin_ref(operator, retained)
    return selected


def _pin_enabled(operator: Any) -> bool:
    return bool(
        getattr(
            operator,
            "pin_deployment_identity_for_separate_outcome_gate",
            False,
        )
    )


def _operator_backend(operator: Any) -> str:
    return frozen_launch_pin_store_locator(getattr(operator, "launch_pin_store", None)).backend


def _operator_conn_id(operator: Any) -> str | None:
    locator = getattr(operator, "launch_pin_store", None)
    if isinstance(locator, Mapping):
        value = locator.get("kubernetes_conn_id")
        if isinstance(value, str) and value.strip():
            return value.strip()
    value = getattr(operator, "kubernetes_conn_id", None)
    return value.strip() if isinstance(value, str) and value.strip() else None


def _operator_namespace(operator: Any) -> str | None:
    value = getattr(operator, "namespace", None)
    return value.strip() if isinstance(value, str) and value.strip() else None


__all__ = [
    "attach_operator_launch_pin_barrier",
    "commit_operator_launch_pin",
    "prepare_operator_launch_pin_pod",
]
