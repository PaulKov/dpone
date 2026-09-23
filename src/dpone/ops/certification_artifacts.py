"""Compatibility facade for certification artifact policy and readers."""

from dpone.ops.certification_artifacts_impl import (
    CertificationArtifactReader as CertificationArtifactReader,
)
from dpone.ops.certification_artifacts_impl import (
    CertificationEvidenceItem as CertificationEvidenceItem,
)
from dpone.ops.certification_artifacts_impl import (
    CertificationTrustDecision as CertificationTrustDecision,
)
from dpone.ops.certification_artifacts_impl import (
    artifact_payload_passed as artifact_payload_passed,
)
from dpone.ops.certification_artifacts_impl import (
    artifact_requires_certification_trust as artifact_requires_certification_trust,
)
from dpone.ops.certification_artifacts_impl import (
    certification_trust as certification_trust,
)
from dpone.ops.certification_artifacts_impl import (
    production_artifact_payload_passed as production_artifact_payload_passed,
)

__all__ = [
    "CertificationArtifactReader",
    "CertificationEvidenceItem",
    "CertificationTrustDecision",
    "artifact_payload_passed",
    "artifact_requires_certification_trust",
    "certification_trust",
    "production_artifact_payload_passed",
]
