"""Support helpers for data product cost governance."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from dpone.readiness import data_product_compliance_support as base_support
from dpone.readiness.data_product_cost_policy import apply_profile, status
from dpone.readiness.data_product_cost_schemas import (
    COST_EVALUATION_SCHEMA,
    COST_FORECAST_SCHEMA,
    COST_GATE_SCHEMA,
    COST_PLAN_SCHEMA,
    COST_REPORT_SCHEMA,
)
from dpone.readiness.migration_control import stable_fingerprint

BYTES_IN_GB = 1024**3
BYTES_IN_TB = 1024**4


@dataclass(frozen=True, slots=True)
class CostGovernanceOptions:
    enabled: bool
    mode: str
    profile: str
    currency: str
    stale_evidence_policy: str
    cost_center: str | None
    require_owner_cost_center: bool
    monthly_max_cost: float | None
    per_run_max_cost: float | None
    per_release_max_cost_delta: float | None
    max_table_growth_ratio: float | None
    max_staging_gb: float | None
    max_query_duration_ms: int | None
    max_concurrent_runs: int | None
    storage_gb_month: float
    staging_gb_day: float
    query_tb_scanned: float
    run_minute: float
    block_unbounded_full_refresh: bool
    block_missing_owner_cost_center: bool
    require_waiver_for_budget_excess: bool

    @classmethod
    def from_manifest(cls, manifest: Mapping[str, Any]) -> CostGovernanceOptions:
        raw = _options(manifest)
        ownership = _mapping(raw.get("ownership"))
        budgets = _mapping(raw.get("budgets"))
        capacity = _mapping(raw.get("capacity"))
        rates = _mapping(raw.get("rates"))
        policies = _mapping(raw.get("policies"))
        return cls(
            enabled=base_support.bool_value(raw.get("enabled"), False),
            mode=str(raw.get("mode") or "gate"),
            profile=str(raw.get("profile") or "prod_strict"),
            currency=str(raw.get("currency") or "USD"),
            stale_evidence_policy=str(raw.get("stale_evidence_policy") or "block"),
            cost_center=str(ownership.get("cost_center")) if ownership.get("cost_center") else None,
            require_owner_cost_center=base_support.bool_value(ownership.get("require_owner_cost_center"), False),
            monthly_max_cost=_float_or_none(budgets.get("monthly_max_cost")),
            per_run_max_cost=_float_or_none(budgets.get("per_run_max_cost")),
            per_release_max_cost_delta=_float_or_none(budgets.get("per_release_max_cost_delta")),
            max_table_growth_ratio=_float_or_none(capacity.get("max_table_growth_ratio")),
            max_staging_gb=_float_or_none(capacity.get("max_staging_gb")),
            max_query_duration_ms=base_support.optional_int(capacity.get("max_query_duration_ms")),
            max_concurrent_runs=base_support.optional_int(capacity.get("max_concurrent_runs")),
            storage_gb_month=_float_or_none(rates.get("storage_gb_month")) or 0.0,
            staging_gb_day=_float_or_none(rates.get("staging_gb_day")) or 0.0,
            query_tb_scanned=_float_or_none(rates.get("query_tb_scanned")) or 0.0,
            run_minute=_float_or_none(rates.get("run_minute")) or 0.0,
            block_unbounded_full_refresh=base_support.bool_value(policies.get("block_unbounded_full_refresh"), False),
            block_missing_owner_cost_center=base_support.bool_value(
                policies.get("block_missing_owner_cost_center"), False
            ),
            require_waiver_for_budget_excess=base_support.bool_value(
                policies.get("require_waiver_for_budget_excess"), False
            ),
        )

    @classmethod
    def from_plan(cls, plan: Mapping[str, Any]) -> CostGovernanceOptions:
        raw = _mapping(plan.get("options"))
        return cls(
            enabled=plan.get("status") != "disabled",
            mode=str(plan.get("mode") or "gate"),
            profile=str(plan.get("profile") or "prod_strict"),
            currency=str(plan.get("currency") or "USD"),
            stale_evidence_policy=str(raw.get("stale_evidence_policy") or "block"),
            cost_center=str(raw.get("cost_center")) if raw.get("cost_center") else None,
            require_owner_cost_center=base_support.bool_value(raw.get("require_owner_cost_center"), False),
            monthly_max_cost=_float_or_none(raw.get("monthly_max_cost")),
            per_run_max_cost=_float_or_none(raw.get("per_run_max_cost")),
            per_release_max_cost_delta=_float_or_none(raw.get("per_release_max_cost_delta")),
            max_table_growth_ratio=_float_or_none(raw.get("max_table_growth_ratio")),
            max_staging_gb=_float_or_none(raw.get("max_staging_gb")),
            max_query_duration_ms=base_support.optional_int(raw.get("max_query_duration_ms")),
            max_concurrent_runs=base_support.optional_int(raw.get("max_concurrent_runs")),
            storage_gb_month=float(raw.get("storage_gb_month") or 0),
            staging_gb_day=float(raw.get("staging_gb_day") or 0),
            query_tb_scanned=float(raw.get("query_tb_scanned") or 0),
            run_minute=float(raw.get("run_minute") or 0),
            block_unbounded_full_refresh=base_support.bool_value(raw.get("block_unbounded_full_refresh"), False),
            block_missing_owner_cost_center=base_support.bool_value(raw.get("block_missing_owner_cost_center"), False),
            require_waiver_for_budget_excess=base_support.bool_value(
                raw.get("require_waiver_for_budget_excess"), False
            ),
        )

    def to_mapping(self) -> dict[str, Any]:
        return {
            "cost_center": self.cost_center,
            "require_owner_cost_center": self.require_owner_cost_center,
            "monthly_max_cost": self.monthly_max_cost,
            "per_run_max_cost": self.per_run_max_cost,
            "per_release_max_cost_delta": self.per_release_max_cost_delta,
            "max_table_growth_ratio": self.max_table_growth_ratio,
            "max_staging_gb": self.max_staging_gb,
            "max_query_duration_ms": self.max_query_duration_ms,
            "max_concurrent_runs": self.max_concurrent_runs,
            "storage_gb_month": self.storage_gb_month,
            "staging_gb_day": self.staging_gb_day,
            "query_tb_scanned": self.query_tb_scanned,
            "run_minute": self.run_minute,
            "stale_evidence_policy": self.stale_evidence_policy,
            "block_unbounded_full_refresh": self.block_unbounded_full_refresh,
            "block_missing_owner_cost_center": self.block_missing_owner_cost_center,
            "require_waiver_for_budget_excess": self.require_waiver_for_budget_excess,
        }


def product_ref(manifest: Mapping[str, Any]) -> dict[str, Any]:
    return base_support.product_ref(base_support.product(manifest))


def payload_id(payload: Mapping[str, Any], key: str) -> dict[str, Any]:
    result = dict(payload)
    result[key] = stable_fingerprint(result)
    return result


def bytes_to_gb(value: Any) -> float:
    return float(_number(value) / BYTES_IN_GB)


def bytes_to_tb(value: Any) -> float:
    return float(_number(value) / BYTES_IN_TB)


def metric(source: Mapping[str, Any], *keys: str) -> float:
    for key in keys:
        if key in source and source[key] is not None:
            return _number(source[key])
    return 0.0


def mappings(raw: Any) -> tuple[Mapping[str, Any], ...]:
    return base_support.mappings(raw)


def strings(raw: Any) -> tuple[str, ...]:
    return base_support.strings(raw)


def _options(manifest: Mapping[str, Any]) -> Mapping[str, Any]:
    return _mapping(base_support.product(manifest).get("cost_governance"))


def _mapping(raw: Any) -> Mapping[str, Any]:
    return raw if isinstance(raw, Mapping) else {}


def _float_or_none(raw: Any) -> float | None:
    if raw in {None, ""}:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _number(raw: Any) -> float:
    try:
        return float(raw or 0)
    except (TypeError, ValueError):
        return 0.0


__all__ = [
    "COST_EVALUATION_SCHEMA",
    "COST_FORECAST_SCHEMA",
    "COST_GATE_SCHEMA",
    "COST_PLAN_SCHEMA",
    "COST_REPORT_SCHEMA",
    "CostGovernanceOptions",
    "apply_profile",
    "bytes_to_gb",
    "bytes_to_tb",
    "mappings",
    "metric",
    "payload_id",
    "product_ref",
    "status",
    "strings",
]
