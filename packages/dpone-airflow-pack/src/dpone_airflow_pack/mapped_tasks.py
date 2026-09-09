"""Materialize one validated bounded mapping plan as an Airflow operator."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone_airflow_pack.mapping import ProviderMappingPlan, mapped_env_vars


def build_mapped_runtime(
    *,
    plan: ProviderMappingPlan,
    dag: Any,
    init_kwargs: Mapping[str, Any],
    operator_class: type[Any],
) -> Any:
    """Build one static mapped operator without a scheduler-side discovery task."""

    kwargs = dict(init_kwargs)
    raw_env = kwargs.pop("env_vars", {})
    if not isinstance(raw_env, Mapping):
        raise ValueError("DPONE_AIRFLOW_MAPPING_PLAN_MISMATCH: mapped KPO env_vars must be a mapping")
    kwargs["pool"] = plan.pool
    kwargs["max_active_tis_per_dag"] = plan.max_active
    partial = getattr(operator_class, "partial", None)
    if not callable(partial):
        raise ValueError(
            "DPONE_AIRFLOW_MAPPING_UNSUPPORTED: installed Airflow/provider does not expose operator partial()"
        )
    mapped = partial(dag=dag, **kwargs)
    expand = getattr(mapped, "expand", None)
    if not callable(expand):
        raise ValueError(
            "DPONE_AIRFLOW_MAPPING_UNSUPPORTED: installed Airflow/provider does not expose mapped expand()"
        )
    return expand(env_vars=mapped_env_vars(raw_env, plan))


def reject_mapping_owned_overrides(overrides: Mapping[str, Any]) -> None:
    """Prevent callers from weakening the pack's pool or concurrency limits."""

    forbidden = sorted({"pool", "max_active_tis_per_dag"}.intersection(overrides))
    if forbidden:
        raise ValueError("operator_overrides cannot replace bounded mapping fields: " + ", ".join(forbidden))


def mapped_inline_required_status(pack: Mapping[str, Any]) -> str:
    """Return the per-item XCom status that every mapped operator must enforce."""

    outcome = pack.get("outcome_gate")
    return str(outcome.get("required_status") or "passed") if isinstance(outcome, Mapping) else "passed"


__all__ = ["build_mapped_runtime", "mapped_inline_required_status", "reject_mapping_owned_overrides"]
