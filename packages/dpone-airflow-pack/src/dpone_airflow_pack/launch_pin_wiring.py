"""Pack-time launch-pin lifecycle wiring (keeps pack_tasks under SLOC hard max)."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from dpone_airflow_pack.launch_pin import launch_pin_required
from dpone_airflow_pack.launch_pin_locator import (
    close_launch_pin_store_locator,
    materialize_launch_pin_store_locator,
)


def pin_lifecycle_enabled(*, pack: Mapping[str, Any], separate_outcome_gate: bool) -> bool:
    """Authority C pin lifecycle only when contract requires a launch pin."""

    return bool(separate_outcome_gate) and launch_pin_required(pack)


def apply_pin_keep_pod(kwargs: dict[str, Any], *, pin_enabled: bool) -> dict[str, Any]:
    """Force keep_pod when pin lifecycle is enabled."""

    if pin_enabled:
        kwargs["on_finish_action"] = "keep_pod"
    return kwargs


def closed_locator_mapping(
    pack: Mapping[str, Any],
    *,
    kpo_kwargs: Mapping[str, Any],
) -> dict[str, str | None]:
    """Close the single immutable store locator for runtime/barrier/gate/cleanup."""

    return close_launch_pin_store_locator(
        launch_pin_store=materialize_launch_pin_store_locator(pack),
        kpo_kubernetes_conn_id=kpo_kwargs.get("kubernetes_conn_id"),
        kpo_namespace=kpo_kwargs.get("namespace"),
    )


def attach_outcome_and_cleanup(
    *,
    pack: Mapping[str, Any],
    dag: Any,
    tasks: dict[str, Any],
    runtime: Any,
    upstream_task_id: str,
    pin_enabled: bool,
    closed_locator: Mapping[str, str | None] | None,
    inline_required_status: str | None,
    is_mapped: bool,
    node: Any,
    task_group: Any,
    chain: Any,
    build_outcome: Callable[..., Any],
    build_cleanup: Callable[..., Any],
) -> dict[str, Any]:
    """Attach optional outcome_gate (+ cleanup) after runtime."""

    if inline_required_status is not None or is_mapped:
        return tasks
    outcome = build_outcome(
        pack=pack,
        dag=dag,
        upstream_task_id=upstream_task_id,
        node=node,
        task_group=task_group,
        launch_pin_store=closed_locator,
    )
    if outcome is None:
        return tasks
    tasks["outcome_gate"] = outcome
    chain(runtime, outcome)
    if pin_enabled:
        cleanup = build_cleanup(
            pack=pack,
            dag=dag,
            upstream_task_id=upstream_task_id,
            outcome_gate_task=outcome,
            node=node,
            task_group=task_group,
        )
        tasks["launch_pin_cleanup"] = cleanup
        chain(outcome, cleanup)
    return tasks


__all__ = [
    "apply_pin_keep_pod",
    "attach_outcome_and_cleanup",
    "closed_locator_mapping",
    "pin_lifecycle_enabled",
]
