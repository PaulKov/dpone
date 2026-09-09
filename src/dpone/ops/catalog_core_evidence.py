"""Core artifact-index and evidence service factories."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.ops.artifact_index import ArtifactIndexService
from dpone.ops.evidence import EvidenceBundleService
from dpone.ops.evidence_chain import EvidenceChainService


@dataclass(frozen=True, slots=True)
class CoreEvidenceCatalog:
    """Factory catalog for artifact index, evidence bundles and evidence chains."""

    @classmethod
    def default(cls) -> CoreEvidenceCatalog:
        return cls()

    def artifact_index(self) -> ArtifactIndexService:
        return ArtifactIndexService()

    def evidence_bundle(self) -> EvidenceBundleService:
        return EvidenceBundleService()

    def evidence_chain(self) -> EvidenceChainService:
        return EvidenceChainService()


__all__ = ["CoreEvidenceCatalog"]
