"""Release and governance operational service composition root."""

from __future__ import annotations

from dataclasses import dataclass, field

from dpone.ops.catalog_readiness import ReadinessOpsCatalog
from dpone.ops.catalog_release_gates import ReleaseGateCatalog
from dpone.ops.catalog_release_governance import ReleaseGovernanceCatalog
from dpone.ops.catalog_release_risk import ReleaseRiskCatalog
from dpone.ops.catalog_release_sections import (
    ReleaseCdcOps,
    ReleaseGateOps,
    ReleaseGovernanceOps,
    ReleaseReadinessOps,
    ReleaseRiskOps,
)
from dpone.ops.catalog_route_conformance import RouteConformanceOpsCatalog
from dpone.ops.catalog_route_onboarding import RouteOnboardingOpsCatalog


@dataclass(frozen=True, slots=True)
class ReleaseOpsCatalog(
    ReleaseGateOps,
    ReleaseGovernanceOps,
    ReleaseReadinessOps,
    ReleaseRiskOps,
    ReleaseCdcOps,
):
    """Public release catalog facade assembled from small service sections."""

    gates: ReleaseGateCatalog = field(default_factory=ReleaseGateCatalog.default)
    governance: ReleaseGovernanceCatalog = field(default_factory=ReleaseGovernanceCatalog.default)
    conformance: RouteConformanceOpsCatalog = field(default_factory=RouteConformanceOpsCatalog.default)
    onboarding: RouteOnboardingOpsCatalog = field(default_factory=RouteOnboardingOpsCatalog.default)
    readiness: ReadinessOpsCatalog = field(default_factory=ReadinessOpsCatalog.default)
    risk: ReleaseRiskCatalog = field(default_factory=ReleaseRiskCatalog.default)

    @classmethod
    def default(cls) -> ReleaseOpsCatalog:
        return cls()


__all__ = ["ReleaseOpsCatalog"]
