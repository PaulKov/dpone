"""Release risk and incident service factories."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.ops.diff import OpsDiffService
from dpone.ops.incident import OpsIncidentPackService
from dpone.ops.security import OpsSecurityAuditService
from dpone.ops.slo import OpsSloService


@dataclass(frozen=True, slots=True)
class ReleaseRiskCatalog:
    """Factory catalog for diff, incident, security and SLO services."""

    @classmethod
    def default(cls) -> ReleaseRiskCatalog:
        return cls()

    def diff(self) -> OpsDiffService:
        return OpsDiffService()

    def incident_pack(self) -> OpsIncidentPackService:
        return OpsIncidentPackService()

    def security_audit(self) -> OpsSecurityAuditService:
        return OpsSecurityAuditService()

    def slo(self) -> OpsSloService:
        return OpsSloService()


__all__ = ["ReleaseRiskCatalog"]
