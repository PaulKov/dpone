"""Provider-neutral data product cost, capacity and quota guardrails."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from dpone.readiness import data_product_cost_support as support
from dpone.readiness.data_product_cost_support import CostGovernanceOptions


class CostGovernancePlanner:
    """Builds product-bound cost governance plans from local manifest and bundle evidence."""

    def plan(self, *, manifest: Mapping[str, Any], bundle: Mapping[str, Any] | None = None) -> dict[str, Any]:
        options = CostGovernanceOptions.from_manifest(manifest)
        product = support.product_ref(manifest)
        if not options.enabled:
            return _plan_payload(options, product, bundle or {}, "disabled", {}, (), ())
        capacity = _planned_capacity(bundle or {})
        blockers: list[str] = []
        warnings: list[str] = []
        if (options.require_owner_cost_center or options.block_missing_owner_cost_center) and not options.cost_center:
            blockers.append("data_product_cost.cost_center_missing")
        if options.block_unbounded_full_refresh and capacity.get("full_refresh"):
            blockers.append("data_product_cost.unbounded_full_refresh")
        blockers, warnings = support.apply_profile(
            blockers=blockers,
            warnings=warnings,
            profile=options.profile,
            mode=options.mode,
        )
        return _plan_payload(
            options, product, bundle or {}, support.status(blockers, warnings, "ready"), capacity, blockers, warnings
        )


class CostBudgetEvaluator:
    """Evaluates runtime and target evidence against cost and capacity guardrails."""

    def evaluate(
        self,
        *,
        plan: Mapping[str, Any],
        runtime_artifact: Mapping[str, Any] | None = None,
        registry_records: Sequence[Mapping[str, Any]] = (),
        target_metrics: Mapping[str, Any] | None = None,
        target_blockers: Sequence[str] = (),
        target_warnings: Sequence[str] = (),
    ) -> dict[str, Any]:
        if plan.get("status") == "disabled":
            return _evaluation_payload(plan, {}, {}, {}, "disabled", (), ())
        options = CostGovernanceOptions.from_plan(plan)
        metrics = _metrics(runtime_artifact or {}, target_metrics or {})
        costs = _estimated_costs(options, metrics, registry_records)
        capacity = _capacity(metrics)
        blockers = [*support.strings(plan.get("blockers")), *support.strings(target_blockers)]
        warnings = [*support.strings(plan.get("warnings")), *support.strings(target_warnings)]
        blockers.extend(_budget_blockers(options, costs))
        blockers.extend(_capacity_blockers(options, capacity))
        blockers, warnings = support.apply_profile(
            blockers=blockers,
            warnings=warnings,
            profile=options.profile,
            mode=options.mode,
        )
        return _evaluation_payload(
            plan,
            metrics,
            costs,
            capacity,
            support.status(blockers, warnings, "allowed"),
            blockers,
            warnings,
        )


class CostGovernanceGate:
    """Profile-aware go/no-go decision over a cost evaluation."""

    def evaluate(self, *, evaluation: Mapping[str, Any], profile: str = "prod_strict") -> dict[str, Any]:
        if evaluation.get("status") == "disabled":
            blockers: list[str] = []
            warnings: list[str] = []
        else:
            blockers = list(support.strings(evaluation.get("blockers")))
            warnings = list(support.strings(evaluation.get("warnings")))
            if profile == "regulated" and not _product_field(evaluation, "owner"):
                blockers.append("data_product_cost.owner_missing")
            if profile == "regulated" and not _options(evaluation).get("cost_center"):
                blockers.append("data_product_cost.cost_center_missing")
        blockers, warnings = support.apply_profile(blockers=blockers, warnings=warnings, profile=profile)
        payload = {
            "schema_version": support.COST_GATE_SCHEMA,
            "status": "allowed"
            if evaluation.get("status") == "disabled"
            else support.status(blockers, warnings, "allowed"),
            "profile": profile,
            "product": _mapping(evaluation.get("product")),
            "product_id": evaluation.get("product_id"),
            "cost_evaluation_id": evaluation.get("cost_evaluation_id"),
            "cost_plan_id": evaluation.get("cost_plan_id"),
            "pack_id": evaluation.get("pack_id"),
            "bundle_id": evaluation.get("bundle_id"),
            "estimated_costs": dict(_mapping(evaluation.get("estimated_costs"))),
            "capacity": dict(_mapping(evaluation.get("capacity"))),
            "blockers": list(dict.fromkeys(blockers)),
            "warnings": list(dict.fromkeys(warnings)),
            "recommendations": _recommendations("blocked" if blockers else "warning" if warnings else "allowed"),
        }
        return support.payload_id(payload, "cost_gate_id")


def _plan_payload(
    options: CostGovernanceOptions,
    product: Mapping[str, Any],
    bundle: Mapping[str, Any],
    status: str,
    capacity: Mapping[str, Any],
    blockers: Sequence[str],
    warnings: Sequence[str],
) -> dict[str, Any]:
    payload = {
        "schema_version": support.COST_PLAN_SCHEMA,
        "status": status,
        "mode": options.mode,
        "profile": options.profile,
        "currency": options.currency,
        "product": dict(product),
        "product_id": product.get("id"),
        "cost_center": options.cost_center,
        "pack_id": bundle.get("pack_id"),
        "bundle_id": bundle.get("bundle_id"),
        "options": options.to_mapping(),
        "planned_capacity": dict(capacity),
        "blockers": list(dict.fromkeys(blockers)),
        "warnings": list(dict.fromkeys(warnings)),
        "recommendations": _recommendations(status),
    }
    return support.payload_id(payload, "cost_plan_id")


def _evaluation_payload(
    plan: Mapping[str, Any],
    metrics: Mapping[str, Any],
    costs: Mapping[str, Any],
    capacity: Mapping[str, Any],
    status: str,
    blockers: Sequence[str],
    warnings: Sequence[str],
) -> dict[str, Any]:
    payload = {
        "schema_version": support.COST_EVALUATION_SCHEMA,
        "status": status,
        "mode": plan.get("mode"),
        "profile": plan.get("profile"),
        "currency": plan.get("currency"),
        "product": _mapping(plan.get("product")),
        "product_id": plan.get("product_id"),
        "cost_plan_id": plan.get("cost_plan_id"),
        "pack_id": plan.get("pack_id"),
        "bundle_id": plan.get("bundle_id"),
        "metrics": dict(metrics),
        "estimated_costs": dict(costs),
        "capacity": dict(capacity),
        "options": dict(_mapping(plan.get("options"))),
        "blockers": list(dict.fromkeys(blockers)),
        "warnings": list(dict.fromkeys(warnings)),
        "recommendations": _recommendations(status),
    }
    return support.payload_id(payload, "cost_evaluation_id")


def _planned_capacity(bundle: Mapping[str, Any]) -> dict[str, Any]:
    summary = _mapping(bundle.get("summary"))
    return {
        "full_refresh": bool(summary.get("full_refresh") or bundle.get("full_refresh")),
        "strategy": summary.get("strategy") or bundle.get("strategy"),
        "changes_count": int(summary.get("changes_count") or 0),
    }


def _metrics(runtime: Mapping[str, Any], target: Mapping[str, Any]) -> dict[str, Any]:
    merged = {**target, **runtime}
    return {
        "duration_ms": support.metric(merged, "duration_ms", "query_duration_ms", "max_query_duration_ms"),
        "bytes_written": support.metric(merged, "bytes_written", "table_bytes"),
        "staging_bytes": support.metric(merged, "staging_bytes", "staging_size_bytes"),
        "query_bytes_read": support.metric(merged, "query_bytes_read", "read_bytes"),
        "table_bytes_before": support.metric(merged, "table_bytes_before", "previous_table_bytes"),
        "table_bytes_after": support.metric(merged, "table_bytes_after", "current_table_bytes", "table_bytes"),
        "max_concurrent_runs": support.metric(merged, "max_concurrent_runs", "concurrent_runs"),
        "full_refresh": bool(merged.get("full_refresh")),
        "run_id": merged.get("run_id"),
    }


def _estimated_costs(
    options: CostGovernanceOptions,
    metrics: Mapping[str, Any],
    registry_records: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    storage = support.bytes_to_gb(metrics.get("bytes_written")) * options.storage_gb_month
    staging = support.bytes_to_gb(metrics.get("staging_bytes")) * options.staging_gb_day
    query = support.bytes_to_tb(metrics.get("query_bytes_read")) * options.query_tb_scanned
    runtime = (float(metrics.get("duration_ms") or 0) / 60000.0) * options.run_minute
    per_run = round(storage + staging + query + runtime, 6)
    previous_month = _previous_monthly_cost(registry_records)
    return {
        "storage_cost": round(storage, 6),
        "staging_cost": round(staging, 6),
        "query_cost": round(query, 6),
        "runtime_cost": round(runtime, 6),
        "per_run_cost": per_run,
        "projected_monthly_cost": round(previous_month + per_run, 6),
        "previous_monthly_cost": round(previous_month, 6),
        "release_cost_delta": _release_delta(per_run, registry_records),
    }


def _capacity(metrics: Mapping[str, Any]) -> dict[str, Any]:
    before = float(metrics.get("table_bytes_before") or 0)
    after = float(metrics.get("table_bytes_after") or 0)
    growth = 0.0 if before <= 0 else max(0.0, (after - before) / before)
    return {
        "table_growth_ratio": round(growth, 6),
        "staging_gb": round(support.bytes_to_gb(metrics.get("staging_bytes")), 6),
        "query_duration_ms": int(float(metrics.get("duration_ms") or 0)),
        "max_concurrent_runs": int(float(metrics.get("max_concurrent_runs") or 0)),
    }


def _budget_blockers(options: CostGovernanceOptions, costs: Mapping[str, Any]) -> list[str]:
    blockers: list[str] = []
    if options.per_run_max_cost is not None and float(costs.get("per_run_cost") or 0) > options.per_run_max_cost:
        blockers.append("data_product_cost.budget_per_run_exceeded")
    if (
        options.monthly_max_cost is not None
        and float(costs.get("projected_monthly_cost") or 0) > options.monthly_max_cost
    ):
        blockers.append("data_product_cost.budget_monthly_exceeded")
    delta = costs.get("release_cost_delta")
    if (
        options.per_release_max_cost_delta is not None
        and delta is not None
        and float(delta) > options.per_release_max_cost_delta
    ):
        blockers.append("data_product_cost.budget_release_delta_exceeded")
    return blockers


def _capacity_blockers(options: CostGovernanceOptions, capacity: Mapping[str, Any]) -> list[str]:
    checks = (
        (
            options.max_table_growth_ratio,
            capacity.get("table_growth_ratio"),
            "data_product_cost.capacity_table_growth_exceeded",
        ),
        (options.max_staging_gb, capacity.get("staging_gb"), "data_product_cost.capacity_staging_gb_exceeded"),
        (
            options.max_query_duration_ms,
            capacity.get("query_duration_ms"),
            "data_product_cost.capacity_query_duration_exceeded",
        ),
        (
            options.max_concurrent_runs,
            capacity.get("max_concurrent_runs"),
            "data_product_cost.capacity_concurrency_exceeded",
        ),
    )
    return [
        code for threshold, actual, code in checks if threshold is not None and float(actual or 0) > float(threshold)
    ]


def _previous_monthly_cost(records: Sequence[Mapping[str, Any]]) -> float:
    return sum(float(record.get("monthly_cost") or 0) for record in records)


def _release_delta(per_run: float, records: Sequence[Mapping[str, Any]]) -> float | None:
    previous = [
        float(_mapping(record.get("estimated_costs")).get("per_run_cost") or 0)
        for record in records
        if isinstance(record.get("estimated_costs"), Mapping)
    ]
    baseline = previous[-1] if previous else 0.0
    return None if baseline <= 0 else round((per_run - baseline) / baseline, 6)


def _recommendations(status: str) -> list[str]:
    if status == "blocked":
        return ["Reduce planned load or attach valid waiver/policy evidence before release closeout."]
    if status == "warning":
        return ["Review cost and capacity warnings with the product owner before promotion."]
    return ["Record cost gate and forecast evidence in the migration evidence registry."]


def _product_field(payload: Mapping[str, Any], key: str) -> Any:
    product = payload.get("product")
    return product.get(key) if isinstance(product, Mapping) else None


def _options(payload: Mapping[str, Any]) -> Mapping[str, Any]:
    return _mapping(payload.get("options"))


def _mapping(raw: Any) -> Mapping[str, Any]:
    return raw if isinstance(raw, Mapping) else {}


__all__ = [
    "CostBudgetEvaluator",
    "CostGovernanceGate",
    "CostGovernanceOptions",
    "CostGovernancePlanner",
]
