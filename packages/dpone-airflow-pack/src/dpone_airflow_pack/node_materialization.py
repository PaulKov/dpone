"""Static process-node materialization contract for the Airflow provider."""

from __future__ import annotations

import re
from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Literal

AirflowStepVisibility = Literal["inline", "task", "group"]

_VISIBILITY = frozenset({"inline", "task", "group"})
_AIRFLOW_ID = re.compile(r"^[A-Za-z0-9_.-]+$")


@dataclass(frozen=True, slots=True)
class PackNodeMaterialization:
    """One immutable DAG-spec node resolved against a workload pack."""

    node_id: str
    workload_id: str
    selector: str | None
    visibility: AirflowStepVisibility
    task_group: str | None

    def __post_init__(self) -> None:
        _require_airflow_id(self.node_id, field="node_id")
        _require_airflow_id(self.workload_id, field="workload_id")
        if self.visibility not in _VISIBILITY:
            raise ValueError(f"DPONE_AIRFLOW_VISIBILITY_INVALID: {self.visibility!r}")
        if self.selector is not None and not self.selector.strip():
            raise ValueError("DPONE_AIRFLOW_NODE_SELECTOR_MISSING: selector cannot be empty")
        if self.task_group is not None:
            _require_airflow_id(self.task_group, field="task_group")

    @property
    def group_id(self) -> str:
        return self.task_group or self.node_id

    def task_id(self, step_name: str) -> str:
        _require_airflow_id(step_name, field="step_name")
        return f"{self.node_id}__{step_name}"


def node_materialization_from_mapping(raw: Mapping[str, Any]) -> PackNodeMaterialization:
    """Parse one static DAG-spec node into the provider value object."""

    node_id = str(raw.get("node_id") or "").strip()
    workload_id = str(raw.get("workload_id") or node_id).strip()
    selector_value = raw.get("selector")
    selector = str(selector_value).strip() if selector_value is not None else None
    visibility = str(raw.get("visibility") or "task").strip()
    task_group_value = raw.get("task_group")
    task_group = str(task_group_value).strip() if task_group_value is not None else None
    return PackNodeMaterialization(
        node_id=node_id,
        workload_id=workload_id,
        selector=selector,
        visibility=visibility,  # type: ignore[arg-type]
        task_group=task_group,
    )


def materialize_process_plan(
    pack: Mapping[str, Any],
    node: PackNodeMaterialization,
) -> dict[str, Any]:
    """Project one immutable selector plan into the existing task builder view."""

    selection = _mapping(pack.get("runtime_selection"))
    plans = _mapping(pack.get("process_plans"))
    if node.selector is None and selection.get("mode") != "process_plan":
        return deepcopy(dict(pack))
    key = node.selector or "__default__"
    raw_plan = plans.get(key)
    if selection.get("mode") != "process_plan" or not isinstance(raw_plan, Mapping):
        raise ValueError(
            "DPONE_AIRFLOW_PACK_SELECTOR_UNSUPPORTED: "
            f"workload {node.workload_id!r} has no immutable process plan for {key!r}"
        )
    plan = dict(raw_plan)
    declared_selector = plan.get("selector")
    if declared_selector != node.selector:
        raise ValueError(
            "DPONE_AIRFLOW_PACK_SELECTOR_UNSUPPORTED: "
            f"process plan selector does not match DAG-spec node {node.node_id!r}"
        )
    dag_node = _mapping(plan.get("dag_node"))
    declared_node_id = dag_node.get("node_id")
    if declared_node_id is not None and declared_node_id != node.node_id:
        raise ValueError(
            "DPONE_AIRFLOW_PACK_SELECTOR_UNSUPPORTED: "
            f"process plan node_id does not match DAG-spec node {node.node_id!r}"
        )
    commands = _mapping(plan.get("runtime_commands"))
    command_key = "inline" if node.visibility == "inline" else "expanded"
    command = commands.get(command_key)
    if not isinstance(command, str) or not command.strip():
        raise ValueError(
            f"DPONE_AIRFLOW_PACK_SELECTOR_UNSUPPORTED: process plan {key!r} has no {command_key} runtime command"
        )
    steps = plan.get("steps")
    if not isinstance(steps, list):
        raise ValueError(f"DPONE_AIRFLOW_PACK_SELECTOR_UNSUPPORTED: process plan {key!r} has no static step list")
    projected = deepcopy(dict(pack))
    projected["runtime_command"] = command
    projected["steps"] = deepcopy(steps)
    projected["mapping_plan"] = deepcopy(plan.get("mapping_plan") or {})
    return projected


def _require_airflow_id(value: str, *, field: str) -> None:
    if not value or not _AIRFLOW_ID.fullmatch(value):
        raise ValueError(f"DPONE_AIRFLOW_TASK_ID_CONFLICT: invalid {field} {value!r}")


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


__all__ = [
    "AirflowStepVisibility",
    "PackNodeMaterialization",
    "materialize_process_plan",
    "node_materialization_from_mapping",
]
