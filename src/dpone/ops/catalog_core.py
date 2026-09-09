"""Core operational service factories."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from dpone.ops.catalog_core_certification import CoreCertificationCatalog
from dpone.ops.catalog_core_contracts import CoreContractCatalog
from dpone.ops.catalog_core_evidence import CoreEvidenceCatalog
from dpone.ops.catalog_core_package import CorePackageCatalog
from dpone.ops.catalog_core_rollback import CoreRollbackCatalog
from dpone.ops.catalog_protocols import (
    AppendService,
    BuildService,
    CatalogService,
    EvaluateService,
    ExecuteService,
    GenerateService,
    LoadPackageServicePort,
    RecordService,
    ReplayStoreService,
    RunMockContractService,
)


@dataclass(frozen=True, slots=True)
class CoreOpsCatalog:
    """Factory catalog for core ops command services."""

    certification: CoreCertificationCatalog = field(default_factory=CoreCertificationCatalog.default)
    contracts: CoreContractCatalog = field(default_factory=CoreContractCatalog.default)
    evidence: CoreEvidenceCatalog = field(default_factory=CoreEvidenceCatalog.default)
    package: CorePackageCatalog = field(default_factory=CorePackageCatalog.default)
    rollback: CoreRollbackCatalog = field(default_factory=CoreRollbackCatalog.default)

    @classmethod
    def default(cls) -> CoreOpsCatalog:
        return cls()

    def artifact_index(self) -> BuildService:
        return self.evidence.artifact_index()

    def certification_harness(self) -> RunMockContractService:
        return self.certification.certification_harness()

    def certification_history(self) -> RecordService:
        return self.certification.certification_history()

    def connector_badges(self) -> GenerateService:
        return self.certification.connector_badges()

    def connector_marketplace(self) -> CatalogService:
        return self.certification.connector_marketplace()

    def data_contracts(self) -> EvaluateService:
        return self.contracts.data_contracts()

    def evidence_bundle(self) -> BuildService:
        return self.evidence.evidence_bundle()

    def evidence_chain(self) -> AppendService:
        return self.evidence.evidence_chain()

    def load_packages(self, root: str) -> LoadPackageServicePort:
        return self.package.load_packages(root)

    def quarantine(self, root: str) -> ReplayStoreService:
        return self.package.quarantine(root)

    def rollback_plan(self, **kwargs: Any) -> Any:
        return self.rollback.rollback_plan(**kwargs)

    def rollback_plan_from_payload(self, payload: Mapping[str, Any]) -> Any:
        return self.rollback.rollback_plan_from_payload(payload)

    def rollback_plan_service(self) -> Any:
        return self.rollback.rollback_plan_service()

    def rollback_execution(self) -> ExecuteService:
        return self.rollback.rollback_execution()
