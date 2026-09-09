"""Pure Airflow step-visibility and visible-task budget policies."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Final

AIRFLOW_VISIBILITY_INLINE: Final = "inline"
AIRFLOW_VISIBILITY_TASK: Final = "task"
AIRFLOW_VISIBILITY_GROUP: Final = "group"
AIRFLOW_VISIBILITY_MODES: Final = (
    AIRFLOW_VISIBILITY_INLINE,
    AIRFLOW_VISIBILITY_TASK,
    AIRFLOW_VISIBILITY_GROUP,
)

DEFAULT_VISIBLE_TASK_WARN: Final = 100
DEFAULT_VISIBLE_TASK_MAX: Final = 250
HARD_VISIBLE_TASK_MAX: Final = 250


class AirflowStepVisibilityError(ValueError):
    """A stable authoring or task-budget contract failure."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(message)


@dataclass(frozen=True, slots=True)
class VisibleTaskBudget:
    """Build-time warning and hard limits for one Airflow DAG."""

    warn: int = DEFAULT_VISIBLE_TASK_WARN
    maximum: int = DEFAULT_VISIBLE_TASK_MAX

    def __post_init__(self) -> None:
        valid = (
            isinstance(self.warn, int)
            and not isinstance(self.warn, bool)
            and isinstance(self.maximum, int)
            and not isinstance(self.maximum, bool)
            and self.warn > 0
            and self.maximum > 0
            and self.warn <= self.maximum <= HARD_VISIBLE_TASK_MAX
        )
        if not valid:
            raise AirflowStepVisibilityError(
                "DPONE_AIRFLOW_VISIBLE_TASK_BUDGET_INVALID",
                f"visible task budget requires 0 < warn <= max <= {HARD_VISIBLE_TASK_MAX}",
            )

    def to_jsonable(self) -> dict[str, int]:
        return {"warn": self.warn, "max": self.maximum}


@dataclass(frozen=True, slots=True)
class VisibleTaskPlan:
    """Deterministic visible-task estimate for one DAG."""

    estimated_total: int
    budget: VisibleTaskBudget
    status: str

    def to_jsonable(self) -> dict[str, int | str]:
        return {
            "estimated_total": self.estimated_total,
            "warn_threshold": self.budget.warn,
            "max_tasks": self.budget.maximum,
            "status": self.status,
        }


def resolve_step_visibility(process_config: Mapping[str, Any]) -> str:
    """Resolve one process presentation mode; omitted means ``inline``."""

    raw_execution = process_config.get("execution")
    if raw_execution is None:
        return AIRFLOW_VISIBILITY_INLINE
    if not isinstance(raw_execution, Mapping):
        raise _visibility_error(raw_execution)
    raw_visibility = raw_execution.get("visibility")
    if raw_visibility is None:
        return AIRFLOW_VISIBILITY_INLINE
    if not isinstance(raw_visibility, str) or raw_visibility not in AIRFLOW_VISIBILITY_MODES:
        raise _visibility_error(raw_visibility)
    return raw_visibility


def parse_visible_task_budget(raw: object) -> VisibleTaskBudget:
    """Parse an optional DAG authoring budget mapping."""

    if raw is None:
        return VisibleTaskBudget()
    if not isinstance(raw, Mapping):
        raise AirflowStepVisibilityError(
            "DPONE_AIRFLOW_VISIBLE_TASK_BUDGET_INVALID",
            "wiring.visible_task_budget must be an object with warn and max",
        )
    allowed = {"warn", "max"}
    if set(raw) - allowed:
        raise AirflowStepVisibilityError(
            "DPONE_AIRFLOW_VISIBLE_TASK_BUDGET_INVALID",
            "wiring.visible_task_budget accepts only warn and max",
        )
    return VisibleTaskBudget(
        warn=raw.get("warn", DEFAULT_VISIBLE_TASK_WARN),
        maximum=raw.get("max", DEFAULT_VISIBLE_TASK_MAX),
    )


def separate_airflow_hook_count(process_config: Mapping[str, Any]) -> int:
    """Count pre-hooks explicitly configured as separate Airflow tasks."""

    source = _mapping(process_config.get("source"))
    options = _mapping(source.get("options"))
    hooks = _mapping(options.get("hooks"))
    pre_hooks = hooks.get("pre_hook")
    if not isinstance(pre_hooks, Sequence) or isinstance(pre_hooks, str | bytes):
        return 0
    return sum(1 for hook in pre_hooks if _separate_airflow_hook(hook))


def estimate_visible_tasks(*, visibility: str, separate_hook_count: int) -> int:
    """Count real Airflow task instances, including tasks inside TaskGroups."""

    if visibility not in AIRFLOW_VISIBILITY_MODES:
        raise _visibility_error(visibility)
    if not isinstance(separate_hook_count, int) or isinstance(separate_hook_count, bool) or separate_hook_count < 0:
        raise AirflowStepVisibilityError(
            "DPONE_AIRFLOW_VISIBLE_TASK_ESTIMATE_INVALID",
            "separate hook count must be a non-negative integer",
        )
    if visibility == AIRFLOW_VISIBILITY_INLINE:
        return 1
    return separate_hook_count + 2


def build_visible_task_plan(
    node_estimates: Sequence[int],
    *,
    budget: VisibleTaskBudget,
) -> VisibleTaskPlan:
    """Aggregate node estimates and classify the configured budget."""

    if any(not isinstance(value, int) or isinstance(value, bool) or value < 1 for value in node_estimates):
        raise AirflowStepVisibilityError(
            "DPONE_AIRFLOW_VISIBLE_TASK_ESTIMATE_INVALID",
            "every DAG node must estimate at least one visible task",
        )
    total = sum(node_estimates)
    if total > budget.maximum:
        status = "blocked"
    elif total > budget.warn:
        status = "warning"
    else:
        status = "within_budget"
    return VisibleTaskPlan(estimated_total=total, budget=budget, status=status)


def _visibility_error(value: object) -> AirflowStepVisibilityError:
    return AirflowStepVisibilityError(
        "DPONE_AIRFLOW_VISIBILITY_INVALID",
        f"execution.visibility must be one of {AIRFLOW_VISIBILITY_MODES}, got {value!r}",
    )


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _separate_airflow_hook(raw: object) -> bool:
    if not isinstance(raw, Mapping):
        return False
    execution = _mapping(raw.get("execution"))
    return execution.get("airflow") == "separate_task"


__all__ = [
    "AIRFLOW_VISIBILITY_GROUP",
    "AIRFLOW_VISIBILITY_INLINE",
    "AIRFLOW_VISIBILITY_MODES",
    "AIRFLOW_VISIBILITY_TASK",
    "DEFAULT_VISIBLE_TASK_MAX",
    "DEFAULT_VISIBLE_TASK_WARN",
    "HARD_VISIBLE_TASK_MAX",
    "AirflowStepVisibilityError",
    "VisibleTaskBudget",
    "VisibleTaskPlan",
    "build_visible_task_plan",
    "estimate_visible_tasks",
    "parse_visible_task_budget",
    "resolve_step_visibility",
    "separate_airflow_hook_count",
]
