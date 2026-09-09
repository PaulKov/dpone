"""Policy scoring for Route Conformance Lab."""

from __future__ import annotations

from collections.abc import Mapping

from dpone.ops.routes.conformance_models import (
    ConformanceStatus,
    RouteConformanceDatasetProfile,
    RouteConformanceDecision,
    RouteConformanceVerificationResult,
)
from dpone.ops.routes.models import RouteKey


class RouteConformancePolicy:
    """Evaluate conformance evidence for one route."""

    def evaluate(
        self,
        *,
        route: RouteKey,
        profile_exists: bool,
        dataset: RouteConformanceDatasetProfile,
        verification: RouteConformanceVerificationResult,
        schema_evolution: Mapping[str, object],
        min_rows: int,
        min_columns: int,
        require_schema_evolution: bool,
    ) -> RouteConformanceDecision:
        blockers = list(verification.blockers)
        warnings = list(verification.warnings)
        if not profile_exists:
            blockers.insert(0, f"route.unsupported:{route.colon_id}")
        if dataset.row_count < min_rows:
            blockers.append("dataset.rows_below_minimum")
        if dataset.column_count < min_columns:
            blockers.append("dataset.columns_below_minimum")
        if require_schema_evolution and not schema_evolution.get("passed"):
            blockers.append("schema_evolution.missing")
        blockers = list(dict.fromkeys(blockers))
        status = _status(blockers, warnings)
        return RouteConformanceDecision(
            passed=status != "blocked",
            status=status,
            score=_score(blockers=blockers, warnings=warnings),
            blockers=tuple(blockers),
            warnings=tuple(dict.fromkeys(warnings)),
            next_actions=_next_actions(blockers),
        )


def aggregate_status(blockers: tuple[str, ...], warnings: tuple[str, ...]) -> ConformanceStatus:
    return _status(list(blockers), list(warnings))


def aggregate_score(total: int, passed: int, blockers: tuple[str, ...]) -> float:
    if total <= 0:
        return 0.0
    if blockers:
        return round((passed / total) * 100.0, 2)
    return 100.0


def _status(blockers: list[str], warnings: list[str]) -> ConformanceStatus:
    if blockers:
        return "blocked"
    if warnings:
        return "warning"
    return "verified"


def _score(*, blockers: list[str], warnings: list[str]) -> float:
    if blockers:
        return max(0.0, 100.0 - 25.0 * len(blockers))
    if warnings:
        return max(80.0, 100.0 - 5.0 * len(warnings))
    return 100.0


def _next_actions(blockers: list[str]) -> tuple[str, ...]:
    if not blockers:
        return (
            "Attach route_conformance.json to route-readiness or route-release-gate evidence.",
            "Promote the route only after live Docker evidence passes for credentialed environments.",
        )
    actions: list[str] = []
    if any(blocker.startswith("route.unsupported") for blocker in blockers):
        actions.append("Add the route to the integration matrix and route profile catalog metadata.")
    if "typed_hash.mismatch" in blockers:
        actions.append("Open mismatch_samples and fix source/sink type conversion or serialization drift.")
    if "row_count.mismatch" in blockers:
        actions.append("Compare chunk receipts and rerun the route with idempotent load semantics.")
    if "schema_evolution.missing" in blockers:
        actions.append("Run schema evolution coverage or disable the release gate requirement explicitly.")
    if "dataset.rows_below_minimum" in blockers:
        actions.append("Increase --rows to the certification profile, normally 10000 rows.")
    if "dataset.columns_below_minimum" in blockers:
        actions.append("Increase --columns to cover the wide-schema profile, normally 200 columns.")
    return tuple(dict.fromkeys(actions or ["Regenerate route conformance evidence and rerun the gate."]))


__all__ = ["RouteConformancePolicy", "aggregate_score", "aggregate_status"]
