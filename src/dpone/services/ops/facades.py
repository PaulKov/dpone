from __future__ import annotations

from dpone.ops.certification_automation import CertificationAutomationPlanService
from dpone.ops.certification_suite import CertificationSuiteService
from dpone.ops.evidence_chain import EvidenceChainService
from dpone.ops.integration_matrix_report import IntegrationMatrixReportService
from dpone.ops.object_storage_retention import ObjectStorageOpsService
from dpone.ops.release_summary import ReleaseSummaryService
from dpone.ops.release_verify import ReleaseVerificationService
from dpone.ops.route_capability_certification import (
    RouteCapabilityCertificationRequest,
    RouteCapabilityCertificationService,
)
from dpone.ops.route_capability_certification_runner import RunManifestRouteCertificationRunner
from dpone.ops.route_certification_matrix import (
    RouteCertificationMatrixError,
    RouteCertificationMatrixRequest,
    RouteCertificationMatrixService,
)
from dpone.ops.self_service_certification import (
    SelfServiceCertificationError,
    SelfServiceCertificationRequest,
    SelfServiceCertificationService,
)


def build_route_capability_certification_service() -> RouteCapabilityCertificationService:
    """Build the route capability certification service behind the ops facade."""

    return RouteCapabilityCertificationService(route_runner=RunManifestRouteCertificationRunner())


__all__ = [
    "CertificationAutomationPlanService",
    "CertificationSuiteService",
    "EvidenceChainService",
    "IntegrationMatrixReportService",
    "ObjectStorageOpsService",
    "ReleaseSummaryService",
    "ReleaseVerificationService",
    "RouteCapabilityCertificationRequest",
    "RouteCapabilityCertificationService",
    "RouteCertificationMatrixError",
    "RouteCertificationMatrixRequest",
    "RouteCertificationMatrixService",
    "SelfServiceCertificationError",
    "SelfServiceCertificationRequest",
    "SelfServiceCertificationService",
    "build_route_capability_certification_service",
]
