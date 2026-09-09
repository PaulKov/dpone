"""Credential-free live conformance adapters."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.ops.routes.conformance_live_models import RouteConformanceLiveConfig, RouteConformanceLiveStep
from dpone.ops.routes.conformance_models import (
    RouteConformanceColumn,
    RouteConformanceDataset,
    RouteConformanceSnapshot,
)
from dpone.ops.routes.models import RouteKey


class InMemoryRouteConformanceLiveAdapter:
    """In-memory adapter that exercises live runner orchestration without credentials."""

    def __init__(self, *, drift_mode: str = "none") -> None:
        self._drift_mode = drift_mode
        self._seeded: RouteConformanceDataset | None = None

    def seed_source(
        self,
        *,
        route: RouteKey,
        dataset: RouteConformanceDataset,
        config: RouteConformanceLiveConfig,
    ) -> RouteConformanceLiveStep:
        del route, config
        self._seeded = dataset
        return RouteConformanceLiveStep(
            name="seed_source",
            status="verified",
            summary=f"seeded deterministic source rows={len(dataset.rows)}",
            rows=len(dataset.rows),
        )

    def apply_schema_evolution(
        self,
        *,
        route: RouteKey,
        dataset: RouteConformanceDataset,
        config: RouteConformanceLiveConfig,
    ) -> RouteConformanceLiveStep:
        del route
        if not config.require_schema_evolution and not dataset.profile.include_schema_evolution:
            return RouteConformanceLiveStep(
                name="apply_schema_evolution",
                status="verified",
                summary="schema evolution not requested",
                rows=len(self._seeded.rows) if self._seeded else len(dataset.rows),
            )
        source = self._seeded or dataset
        column = RouteConformanceColumn(
            name="evolved_score",
            logical_type="decimal",
            physical_contract="decimal(38,10)",
            nullable=True,
            ordinal=len(source.columns),
        )
        rows = [
            {
                **dict(row),
                column.name: f"{(index + 1) / 10000:.10f}",
            }
            for index, row in enumerate(source.rows)
        ]
        self._seeded = source.with_rows_and_columns(rows, (*source.columns, column))
        return RouteConformanceLiveStep(
            name="apply_schema_evolution",
            status="verified",
            summary="applied deterministic nullable decimal column",
            rows=len(rows),
        )

    def execute_route(
        self,
        *,
        route: RouteKey,
        dataset: RouteConformanceDataset,
        config: RouteConformanceLiveConfig,
    ) -> RouteConformanceLiveStep:
        del route, dataset, config
        return RouteConformanceLiveStep(
            name="execute_route",
            status="verified",
            summary="in-memory route execution completed",
            rows=len(self._seeded.rows) if self._seeded else 0,
        )

    def read_source_snapshot(
        self,
        *,
        route: RouteKey,
        dataset: RouteConformanceDataset,
        config: RouteConformanceLiveConfig,
    ) -> RouteConformanceSnapshot:
        del route, config
        return (self._seeded or dataset).to_snapshot("live_source")

    def read_sink_snapshot(
        self,
        *,
        route: RouteKey,
        dataset: RouteConformanceDataset,
        config: RouteConformanceLiveConfig,
    ) -> RouteConformanceSnapshot:
        del route
        source = self._seeded or dataset
        mode = config.drift_mode if config.drift_mode != "none" else self._drift_mode
        rows = [dict(row) for row in source.rows]
        columns = list(source.columns)
        if mode == "value" and rows:
            rows[min(3, len(rows) - 1)]["text_001"] = "changed downstream value"
        elif mode == "row_count" and rows:
            rows = rows[:-1]
        elif mode == "contract":
            columns = [_contract_drift(column) for column in columns]
        return source.with_rows_and_columns(rows, columns).to_snapshot("live_sink")


def _contract_drift(column: RouteConformanceColumn) -> RouteConformanceColumn:
    if column.name == "text_001":
        return column.with_physical_contract("nvarchar(255)")
    return column


def default_live_adapter_registry() -> Mapping[str, Any]:
    vendor_live = _vendor_live_adapter()
    return {
        "in_memory": InMemoryRouteConformanceLiveAdapter(),
        "vendor_live": vendor_live,
        "docker": vendor_live,
    }


def _vendor_live_adapter() -> Any:
    from dpone.ops.routes.conformance_vendor_sql import DockerVendorLiveRouteConformanceAdapterFactory

    return DockerVendorLiveRouteConformanceAdapterFactory.from_env()


__all__ = ["InMemoryRouteConformanceLiveAdapter", "default_live_adapter_registry"]
