"""Resolve hook execution from one verified Airflow process plan."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.runtime.init_fetch_contract import InitFetchError
from dpone.runtime.runtime_init_fetch_plan import RuntimeExecutionSelection

DEFAULT_PROCESS_PLAN_KEY = "__default__"
HOOK_SKIP_ENV_NAME = "DPONE_SKIP_AIRFLOW_SEPARATE_HOOKS"
_SKIP_SEPARATE_HOOKS = {HOOK_SKIP_ENV_NAME: "1"}
_VISIBILITY_MODES = frozenset({"inline", "task", "group"})


def verified_runtime_environment(
    pack: Mapping[str, Any],
    *,
    execution: RuntimeExecutionSelection,
    value: object,
) -> Mapping[str, str]:
    """Return the effective hook environment for a verified runtime command.

    Older strict packs encode the expanded-task environment for every runtime
    bootstrap command. The immutable process plan remains authoritative: an
    inline node executes its hooks inside the single runtime task.
    """

    environment = _verified_environment(value)
    if execution.hook_execution is not None:
        if execution.scope == "process":
            exact_verified_process_plan(
                pack,
                selector=execution.process_selector,
            )
        return {} if execution.hook_execution == "inline" else environment
    visibility = _process_visibility(
        legacy_verified_process_plan(
            pack,
            workload_selector=execution.selector,
        )
    )
    return {} if visibility == "inline" else environment


def exact_verified_process_plan(
    pack: Mapping[str, Any],
    *,
    selector: str | None,
) -> Mapping[str, Any]:
    """Return the exact process plan selected by a v3 execution."""

    plans = _process_plans(pack)
    plan_key = selector or DEFAULT_PROCESS_PLAN_KEY
    return _validated_process_plan(
        plans,
        plan_key=plan_key,
        expected_selector=selector,
    )


def legacy_verified_process_plan(
    pack: Mapping[str, Any],
    *,
    workload_selector: str,
) -> Mapping[str, Any]:
    """Resolve an unambiguous selector-coherent v1/v2 process plan."""

    plans = _process_plans(pack)
    if len(plans) != 1:
        raise _policy_error("verified runtime process plan selection is ambiguous")
    del workload_selector
    plan_key = str(next(iter(plans)))
    expected_selector = None if plan_key == DEFAULT_PROCESS_PLAN_KEY else plan_key
    return _validated_process_plan(
        plans,
        plan_key=plan_key,
        expected_selector=expected_selector,
    )


def _process_plans(pack: Mapping[str, Any]) -> Mapping[str, Any]:
    raw_plans = pack.get("process_plans")
    if raw_plans is None or raw_plans == {}:
        raise _policy_error("verified runtime process plan inventory is missing")
    if not isinstance(raw_plans, Mapping):
        raise _policy_error("verified runtime process plans must be an object")
    return raw_plans


def _validated_process_plan(
    plans: Mapping[str, Any],
    *,
    plan_key: str,
    expected_selector: str | None,
) -> Mapping[str, Any]:
    raw_plan = plans.get(plan_key)
    if not isinstance(raw_plan, Mapping):
        raise _policy_error("verified runtime process plan does not match execution selector")
    if raw_plan.get("selector") != expected_selector:
        raise _policy_error("verified runtime process selector is inconsistent")
    return raw_plan


def _verified_environment(value: object) -> dict[str, str]:
    if not isinstance(value, Mapping):
        raise _policy_error("verified workload environment must be an object")
    environment = dict(value)
    if environment != _SKIP_SEPARATE_HOOKS:
        raise _policy_error("verified workload environment contains a forbidden value")
    return environment


def _process_visibility(process_plan: Mapping[str, Any]) -> str:
    raw_node = process_plan.get("dag_node")
    if not isinstance(raw_node, Mapping):
        raise _policy_error("verified runtime process plan has no DAG node")
    visibility = raw_node.get("visibility")
    if visibility not in _VISIBILITY_MODES:
        raise _policy_error("verified runtime process visibility is invalid")
    return str(visibility)


def _policy_error(message: str) -> InitFetchError:
    return InitFetchError("DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED", message)


__all__ = [
    "DEFAULT_PROCESS_PLAN_KEY",
    "HOOK_SKIP_ENV_NAME",
    "exact_verified_process_plan",
    "legacy_verified_process_plan",
    "verified_runtime_environment",
]
