"""Route conformance service factories."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.ops.route_conformance import RouteConformanceService
from dpone.ops.route_conformance_live import RouteConformanceLiveService


@dataclass(frozen=True, slots=True)
class RouteConformanceOpsCatalog:
    """Factory catalog for route conformance and release-gate commands."""

    @classmethod
    def default(cls) -> RouteConformanceOpsCatalog:
        return cls()

    def route_conformance(self) -> RouteConformanceService:
        return RouteConformanceService()

    def route_conformance_live(self) -> RouteConformanceLiveService:
        return RouteConformanceLiveService()


__all__ = ["RouteConformanceOpsCatalog"]
