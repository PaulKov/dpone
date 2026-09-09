"""Artifact and certification operational service factories."""

from __future__ import annotations

from dataclasses import dataclass, field

from dpone.ops.catalog_artifacts_certification import ArtifactCertificationCatalog
from dpone.ops.catalog_artifacts_credentials import ArtifactCredentialCatalog
from dpone.ops.catalog_artifacts_documentation import ArtifactDocumentationCatalog
from dpone.ops.catalog_artifacts_lineage import ArtifactLineageCatalog
from dpone.ops.catalog_artifacts_runtime import ArtifactRuntimeCatalog
from dpone.ops.catalog_evidence import EvidenceOpsCatalog
from dpone.ops.catalog_protocols import (
    BuildService,
    CertifyService,
    CheckService,
    EvaluateService,
    ExportService,
    PlanService,
    PublishService,
    ReconcileService,
    RecordService,
    RenderService,
)


@dataclass(frozen=True, slots=True)
class ArtifactOpsCatalog:
    """Factory catalog for artifact, evidence-pack and certification services."""

    certification: ArtifactCertificationCatalog = field(default_factory=ArtifactCertificationCatalog.default)
    credentials: ArtifactCredentialCatalog = field(default_factory=ArtifactCredentialCatalog.default)
    documentation: ArtifactDocumentationCatalog = field(default_factory=ArtifactDocumentationCatalog.default)
    evidence: EvidenceOpsCatalog = field(default_factory=EvidenceOpsCatalog.default)
    lineage: ArtifactLineageCatalog = field(default_factory=ArtifactLineageCatalog.default)
    runtime: ArtifactRuntimeCatalog = field(default_factory=ArtifactRuntimeCatalog.default)

    @classmethod
    def default(cls) -> ArtifactOpsCatalog:
        return cls()

    @property
    def default_required_release_evidence(self) -> tuple[str, ...]:
        return self.evidence.default_required_release_evidence

    def benchmark_baseline(self) -> EvaluateService:
        return self.certification.benchmark_baseline()

    def benchmark_slo_gate(self) -> EvaluateService:
        return self.certification.benchmark_slo_gate()

    def catalog_publication(self) -> PublishService:
        return self.lineage.catalog_publication()

    def connector_certification_pack(self) -> BuildService:
        return self.certification.connector_certification_pack()

    def dbt_lineage(self) -> ExportService:
        return self.lineage.dbt_lineage()

    def deployment_profiles(self) -> RenderService:
        return self.documentation.deployment_profiles()

    def docs_publish_pack(self) -> BuildService:
        return self.documentation.docs_publish_pack()

    def live_certification(self) -> BuildService:
        return self.certification.live_certification()

    def managed_credentials_readiness(self) -> CheckService:
        return self.credentials.managed_credentials_readiness()

    def live_state_reconciliation(self) -> CertifyService:
        return self.evidence.live_state_reconciliation()

    def manifest_bundle(self) -> BuildService:
        return self.documentation.manifest_bundle()

    def observability_pack(self) -> BuildService:
        return self.runtime.observability_pack()

    def openlineage_export(self) -> ExportService:
        return self.lineage.openlineage_export()

    def performance_certification(self) -> CertifyService:
        return self.certification.performance_certification()

    def pre_release_checklist(self) -> BuildService:
        return self.evidence.pre_release_checklist()

    def reconciliation(self) -> ReconcileService:
        return self.runtime.reconciliation()

    def release_evidence_pack(self) -> BuildService:
        return self.evidence.release_evidence_pack()

    def run_registry(self) -> RecordService:
        return self.lineage.run_registry()

    def runbook_pack(self) -> BuildService:
        return self.documentation.runbook_pack()

    def runtime_recovery(self) -> PlanService:
        return self.runtime.runtime_recovery()

    def staging_evidence(self) -> BuildService:
        return self.runtime.staging_evidence()
