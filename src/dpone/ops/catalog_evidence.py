"""Evidence and release-certification operational service factories."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.ops.live_state_reconciliation import LiveStateReconciliationCertificationService
from dpone.ops.pre_release_checklist import PreReleaseChecklistService
from dpone.ops.release_evidence_pack import DEFAULT_REQUIRED_RELEASE_EVIDENCE, ReleaseEvidencePackService


@dataclass(frozen=True, slots=True)
class EvidenceOpsCatalog:
    """Factory catalog for release evidence and certification services."""

    @classmethod
    def default(cls) -> EvidenceOpsCatalog:
        return cls()

    @property
    def default_required_release_evidence(self) -> tuple[str, ...]:
        return tuple(DEFAULT_REQUIRED_RELEASE_EVIDENCE)

    def live_state_reconciliation(self) -> LiveStateReconciliationCertificationService:
        return LiveStateReconciliationCertificationService()

    def pre_release_checklist(self) -> PreReleaseChecklistService:
        return PreReleaseChecklistService()

    def release_evidence_pack(self) -> ReleaseEvidencePackService:
        return ReleaseEvidencePackService()


__all__ = ["EvidenceOpsCatalog"]
