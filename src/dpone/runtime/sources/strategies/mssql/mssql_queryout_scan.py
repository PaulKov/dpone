"""Source scan decision helper for MSSQL queryout."""

from __future__ import annotations

from typing import Any

from dpone.runtime.decision_audit import publish_runtime_decision
from dpone.runtime.source_scan import SourceScanDecision, SourceScanPlanner
from dpone.runtime.sources.strategies.mssql.mssql_source_shape import MSSQLSourceShapeInspector


def resolve_mssql_source_scan_decision(connector: Any, load_config: Any) -> SourceScanDecision | None:
    """Return an optional scan decision before partition bounds are resolved."""

    if not _has_partitioning_or_scan_policy(load_config.options):
        return None
    boundary_column = _partitioning_column(load_config.options)
    shape = MSSQLSourceShapeInspector(connector).inspect(load_config, boundary_column=boundary_column)
    if not _has_explicit_scan_policy(load_config.options) and shape.table_kind == "unknown":
        return None
    decision = SourceScanPlanner().plan(source_options=load_config.options, source_shape=shape)
    publish_runtime_decision(
        decision,
        decision_id="source_scan.planner",
        phase="extract",
        component="mssql_source",
        category="scan_selection",
        fallback_allowed=not decision.blockers,
        details={"boundary_column": boundary_column},
    )
    return decision


def _has_partitioning_or_scan_policy(options: dict[str, Any]) -> bool:
    partitioning = options.get("partitioning")
    if isinstance(partitioning, dict) and partitioning.get("column"):
        return True
    native = options.get("native_transfer")
    snapshot = native.get("snapshot") if isinstance(native, dict) else None
    return isinstance(snapshot, dict) and "scan" in snapshot


def _has_explicit_scan_policy(options: dict[str, Any]) -> bool:
    native = options.get("native_transfer")
    snapshot = native.get("snapshot") if isinstance(native, dict) else None
    return isinstance(snapshot, dict) and "scan" in snapshot


def _partitioning_column(options: dict[str, Any]) -> str | None:
    partitioning = options.get("partitioning")
    if isinstance(partitioning, dict) and partitioning.get("column"):
        return str(partitioning["column"])
    return None


__all__ = ["resolve_mssql_source_scan_decision"]
