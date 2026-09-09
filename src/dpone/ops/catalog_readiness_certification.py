"""Route certification and release readiness service factories."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.ops.route_certify import RouteCertificationService
from dpone.ops.route_certify_release import RouteCertificationReleaseService
from dpone.ops.route_certify_release_finalizer import RouteCertificationReleaseFinalizerService
from dpone.ops.route_live_certification import RouteLiveCertificationService
from dpone.ops.route_rc_executor import RouteReleaseCandidateExecutorService
from dpone.ops.route_rc_orchestrator import RouteReleaseCandidateOrchestratorService
from dpone.ops.route_release_gate import RouteReleaseGateService
from dpone.ops.route_run_supervisor import RouteRunSupervisorService
from dpone.ops.routes.certification import RouteCertificationPackService


@dataclass(frozen=True, slots=True)
class ReadinessCertificationCatalog:
    """Factories for route certification, release gates and run supervision."""

    @classmethod
    def default(cls) -> ReadinessCertificationCatalog:
        return cls()

    def route_live_certification(self) -> RouteLiveCertificationService:
        return RouteLiveCertificationService()

    def route_rc_orchestrator(self) -> RouteReleaseCandidateOrchestratorService:
        return RouteReleaseCandidateOrchestratorService()

    def route_rc_executor(self) -> RouteReleaseCandidateExecutorService:
        return RouteReleaseCandidateExecutorService()

    def route_release_gate(self) -> RouteReleaseGateService:
        return RouteReleaseGateService()

    def route_run_supervisor(self) -> RouteRunSupervisorService:
        return RouteRunSupervisorService()

    def route_certification_pack(self) -> RouteCertificationPackService:
        return RouteCertificationPackService()

    def route_certify(self) -> RouteCertificationService:
        return RouteCertificationService()

    def route_certify_release(self) -> RouteCertificationReleaseService:
        return RouteCertificationReleaseService()

    def route_release_finalize(self) -> RouteCertificationReleaseFinalizerService:
        return RouteCertificationReleaseFinalizerService()


__all__ = ["ReadinessCertificationCatalog"]
