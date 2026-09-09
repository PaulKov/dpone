"""Core certification and connector marketplace service factories."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.ops.certification import CertificationHarnessService
from dpone.ops.certification_history import CertificationHistoryService
from dpone.ops.connector_badges import ConnectorBadgeService
from dpone.ops.marketplace import ConnectorMarketplaceService


@dataclass(frozen=True, slots=True)
class CoreCertificationCatalog:
    """Factory catalog for connector certification and marketplace services."""

    @classmethod
    def default(cls) -> CoreCertificationCatalog:
        return cls()

    def certification_harness(self) -> CertificationHarnessService:
        return CertificationHarnessService()

    def certification_history(self) -> CertificationHistoryService:
        return CertificationHistoryService()

    def connector_badges(self) -> ConnectorBadgeService:
        return ConnectorBadgeService()

    def connector_marketplace(self) -> ConnectorMarketplaceService:
        return ConnectorMarketplaceService.default()


__all__ = ["CoreCertificationCatalog"]
