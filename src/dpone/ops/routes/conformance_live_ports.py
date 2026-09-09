"""Ports for live Route Conformance Lab adapters."""

from __future__ import annotations

from typing import Protocol

from dpone.ops.routes.conformance_live_models import RouteConformanceLiveConfig, RouteConformanceLiveStep
from dpone.ops.routes.conformance_models import RouteConformanceDataset, RouteConformanceSnapshot
from dpone.ops.routes.models import RouteKey


class RouteConformanceSourceSeeder(Protocol):
    """Seed the live source with deterministic conformance data."""

    def seed_source(
        self,
        *,
        route: RouteKey,
        dataset: RouteConformanceDataset,
        config: RouteConformanceLiveConfig,
    ) -> RouteConformanceLiveStep: ...


class RouteConformanceRouteExecutor(Protocol):
    """Execute the route path after source seeding."""

    def execute_route(
        self,
        *,
        route: RouteKey,
        dataset: RouteConformanceDataset,
        config: RouteConformanceLiveConfig,
    ) -> RouteConformanceLiveStep: ...


class RouteConformanceSnapshotReader(Protocol):
    """Read source and sink snapshots after route execution."""

    def read_source_snapshot(
        self,
        *,
        route: RouteKey,
        dataset: RouteConformanceDataset,
        config: RouteConformanceLiveConfig,
    ) -> RouteConformanceSnapshot: ...

    def read_sink_snapshot(
        self,
        *,
        route: RouteKey,
        dataset: RouteConformanceDataset,
        config: RouteConformanceLiveConfig,
    ) -> RouteConformanceSnapshot: ...


class RouteConformanceSchemaEvolutionApplier(Protocol):
    """Apply a route-generic schema evolution plan before verification."""

    def apply_schema_evolution(
        self,
        *,
        route: RouteKey,
        dataset: RouteConformanceDataset,
        config: RouteConformanceLiveConfig,
    ) -> RouteConformanceLiveStep: ...


class RouteConformanceLiveAdapter(
    RouteConformanceSourceSeeder,
    RouteConformanceSchemaEvolutionApplier,
    RouteConformanceRouteExecutor,
    RouteConformanceSnapshotReader,
    Protocol,
):
    """Complete live adapter contract for one runner backend."""


__all__ = [
    "RouteConformanceLiveAdapter",
    "RouteConformanceRouteExecutor",
    "RouteConformanceSchemaEvolutionApplier",
    "RouteConformanceSnapshotReader",
    "RouteConformanceSourceSeeder",
]
