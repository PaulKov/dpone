"""Readiness assessment service factories."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.ops.industrial_readiness import DEFAULT_INDUSTRIAL_DOMAINS, IndustrialReadinessService
from dpone.ops.production_maturity import DEFAULT_REQUIRED_DOMAINS, ProductionMaturityService
from dpone.ops.route_data_quality import RouteDataQualityService
from dpone.ops.route_readiness import RouteReadinessService
from dpone.ops.route_reconciliation_repair import RouteReconciliationRepairService
from dpone.ops.route_schema_evolution import RouteSchemaEvolutionService


@dataclass(frozen=True, slots=True)
class ReadinessAssessmentCatalog:
    """Factories for route and production readiness assessment services."""

    @classmethod
    def default(cls) -> ReadinessAssessmentCatalog:
        return cls()

    @property
    def default_required_domains(self) -> tuple[str, ...]:
        return tuple(DEFAULT_REQUIRED_DOMAINS)

    @property
    def default_industrial_domains(self) -> tuple[str, ...]:
        return tuple(DEFAULT_INDUSTRIAL_DOMAINS)

    def industrial_readiness(self) -> IndustrialReadinessService:
        return IndustrialReadinessService()

    def production_maturity(self) -> ProductionMaturityService:
        return ProductionMaturityService()

    def route_readiness(self) -> RouteReadinessService:
        return RouteReadinessService()

    def route_schema_evolution(self) -> RouteSchemaEvolutionService:
        return RouteSchemaEvolutionService()

    def route_reconciliation_repair(self) -> RouteReconciliationRepairService:
        return RouteReconciliationRepairService()

    def route_data_quality(self) -> RouteDataQualityService:
        return RouteDataQualityService()


__all__ = ["ReadinessAssessmentCatalog"]
