"""File-IO facade for data product cost governance commands."""

from __future__ import annotations

import json
from collections.abc import Mapping
from importlib import import_module
from pathlib import Path
from typing import Any

import yaml


class DataProductCostFacade:
    """Thin CLI facade; cost decisions live in readiness modules."""

    def plan(self, *, manifest_path: str, bundle_path: str | None = None) -> dict[str, Any]:
        return (
            _cost()
            .CostGovernancePlanner()
            .plan(
                manifest=_read_mapping(manifest_path),
                bundle=_read_optional(bundle_path),
            )
        )

    def evaluate(
        self,
        *,
        plan_path: str,
        registry_path: str | None = None,
        runtime_artifact_path: str | None = None,
        target_connection_path: str | None = None,
    ) -> dict[str, Any]:
        target = _target_cost_evidence(target_connection_path)
        return (
            _cost()
            .CostBudgetEvaluator()
            .evaluate(
                plan=_read_mapping(plan_path),
                runtime_artifact=_read_optional(runtime_artifact_path),
                registry_records=_registry_records(registry_path),
                target_metrics=target["metrics"],
                target_blockers=target["blockers"],
                target_warnings=target["warnings"],
            )
        )

    def gate(self, *, evaluation_path: str, profile: str = "prod_strict") -> dict[str, Any]:
        return _cost().CostGovernanceGate().evaluate(evaluation=_read_mapping(evaluation_path), profile=profile)

    def forecast(self, *, evaluation_path: str, history_path: str | None = None) -> dict[str, Any]:
        return (
            _cost_rendering()
            .CostForecastEvaluator()
            .forecast(
                evaluation=_read_mapping(evaluation_path),
                history=_read_optional(history_path),
            )
        )

    def report(self, *, gate_path: str, forecast_path: str | None = None) -> dict[str, Any]:
        return (
            _cost_rendering()
            .CostGovernanceRenderer()
            .report(
                gate=_read_mapping(gate_path),
                forecast=_read_optional(forecast_path),
            )
        )


def _read_optional(path: str | None) -> dict[str, Any]:
    return _read_mapping(path) if path else {}


def _read_mapping(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    source = Path(path)
    text = source.read_text(encoding="utf-8")
    raw = json.loads(text) if source.suffix.lower() == ".json" else yaml.safe_load(text)
    if not isinstance(raw, Mapping):
        raise ValueError(f"{path} must contain an object")
    return dict(raw)


def _registry_records(path: str | None) -> tuple[Mapping[str, Any], ...]:
    if not path:
        return ()
    if path.endswith((".sqlite", ".sqlite3", ".db")):
        return ()
    raw = _read_mapping(path)
    raw_records = raw.get("records")
    records = raw_records if isinstance(raw_records, list) else []
    return tuple(item for item in records if isinstance(item, Mapping))


def _target_cost_evidence(path: str | None) -> dict[str, Any]:
    if not path:
        return {"metrics": {}, "blockers": (), "warnings": ()}
    connection = _read_mapping(path)
    target_type = str(
        connection.get("type") or connection.get("sink_type") or connection.get("provider") or "unknown"
    ).lower()
    if target_type != "clickhouse":
        return {
            "metrics": {},
            "blockers": (f"data_product_cost.unsupported_target:{target_type}",),
            "warnings": (),
        }
    result = _clickhouse_cost_probe().ClickHouseCostProbe().collect(connection)
    return {
        "metrics": dict(result.get("metrics", {})),
        "blockers": tuple(str(item) for item in result.get("blockers", ()) if str(item)),
        "warnings": tuple(str(item) for item in result.get("warnings", ()) if str(item)),
    }


def _cost() -> Any:
    return import_module("dpone.readiness.data_product_cost")


def _cost_rendering() -> Any:
    return import_module("dpone.readiness.data_product_cost_rendering")


def _clickhouse_cost_probe() -> Any:
    return import_module("dpone.services.data_product_cost_clickhouse")


__all__ = ["DataProductCostFacade"]
