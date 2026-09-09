"""Resolve terminal workflow state across Airflow runtime generations."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from dpone_airflow_pack.launch_pin_cleanup import (
    AIRFLOW_OUTCOME_GATE_RESULT_XCOM_KEY,
)


def terminal_task_states(
    *,
    dag_run: Any,
    ti: Any,
    expected_task_ids: Sequence[str],
) -> dict[str, tuple[str, ...]]:
    """Return exact-run terminal states without requiring Airflow ORM access.

    Airflow 2 supplies an ORM-backed ``DagRun.get_task_instances`` method.
    Airflow 3 Task SDK intentionally does not. In the latter runtime, a dpone
    outcome gate's own structured XCom receipt is the authority for its
    terminal verdict. An absent or malformed receipt remains non-successful.
    """

    observed = _dag_run_task_states(dag_run)
    for task_id in expected_task_ids:
        if observed.get(task_id):
            continue
        receipt_state = _outcome_gate_receipt_state(ti=ti, task_id=task_id)
        if receipt_state is not None:
            observed[task_id] = (receipt_state,)
    return observed


def aggregate_terminal_task_states(states: Sequence[str]) -> str:
    """Aggregate mapped task states without hiding any non-success instance."""

    if not states:
        return "missing"
    if all(state == "success" for state in states):
        return "success"
    for state in states:
        if state != "success":
            return state or "unknown"
    return "unknown"


def _dag_run_task_states(dag_run: Any) -> dict[str, tuple[str, ...]]:
    get_task_instances = getattr(dag_run, "get_task_instances", None)
    if not callable(get_task_instances):
        return {}
    collected: dict[str, list[str]] = {}
    for task_instance in get_task_instances():
        task_id = str(getattr(task_instance, "task_id", "") or "")
        if task_id:
            collected.setdefault(task_id, []).append(str(getattr(task_instance, "state", "") or "unknown"))
    return {task_id: tuple(states) for task_id, states in collected.items()}


def _outcome_gate_receipt_state(*, ti: Any, task_id: str) -> str | None:
    xcom_pull = getattr(ti, "xcom_pull", None)
    if not callable(xcom_pull):
        return None
    try:
        receipt = xcom_pull(
            task_ids=task_id,
            key=AIRFLOW_OUTCOME_GATE_RESULT_XCOM_KEY,
        )
    except Exception:  # noqa: BLE001 - unavailable XCom is a missing receipt
        return None
    if receipt is None:
        return None
    if not isinstance(receipt, Mapping):
        return "invalid"
    payload = receipt.get("payload")
    outer_passed = receipt.get("passed")
    inner_passed = payload.get("passed") if isinstance(payload, Mapping) else None
    if type(outer_passed) is not bool or type(inner_passed) is not bool:  # noqa: E721
        return "invalid"
    if outer_passed is not inner_passed:
        return "invalid"
    return "success" if outer_passed else "failed"


__all__ = [
    "aggregate_terminal_task_states",
    "terminal_task_states",
]
