"""Route onboarding service factories."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.ops.connection_doctor import ConnectionDoctorService
from dpone.ops.route_bootstrap import RouteBootstrapService
from dpone.ops.route_doctor import RouteDoctorService
from dpone.ops.source_discovery import SourceDiscoveryService


@dataclass(frozen=True, slots=True)
class RouteOnboardingOpsCatalog:
    """Factory catalog for route onboarding and self-service doctor commands."""

    @classmethod
    def default(cls) -> RouteOnboardingOpsCatalog:
        return cls()

    def connection_doctor(self) -> ConnectionDoctorService:
        return ConnectionDoctorService()

    def source_discovery(self) -> SourceDiscoveryService:
        return SourceDiscoveryService()

    def route_bootstrap(self) -> RouteBootstrapService:
        return RouteBootstrapService()

    def route_doctor(self) -> RouteDoctorService:
        return RouteDoctorService()


__all__ = ["RouteOnboardingOpsCatalog"]
