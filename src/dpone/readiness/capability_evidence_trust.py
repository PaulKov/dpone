"""Revalidate route proofs before readiness discovery trusts a matrix row."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from dpone.contracts.capability_discovery import (
    CapabilityIssue,
    RouteCertificationCandidate,
)
from dpone.manifest.confined_files import ConfinedFileError, project_relative_path
from dpone.ops.route_certification_matrix_evidence import RouteCertificationEvidenceReader


def load_trusted_proofs(
    root: Path,
    evidence_dirs: Sequence[Path],
    *,
    candidates: Sequence[RouteCertificationCandidate],
    expected_commit: str,
    evaluated_at: datetime,
    max_age_hours: int,
) -> tuple[dict[tuple[str, str], Mapping[str, Any]], tuple[CapabilityIssue, ...]]:
    """Read canonical proof directories and return exact matrix-comparison DTOs."""

    reader = RouteCertificationEvidenceReader()
    proofs: dict[tuple[str, str], Mapping[str, Any]] = {}
    issues: list[CapabilityIssue] = []
    for index, configured in enumerate(evidence_dirs):
        try:
            relative = project_relative_path(root, configured)
        except ConfinedFileError:
            issues.append(
                _proof_issue(
                    "DPONE_CAPABILITY_EVIDENCE_DIRECTORY_UNSAFE",
                    str(index),
                    "Certification evidence directory must stay inside the project root.",
                )
            )
            continue
        result = reader.read(
            root / relative,
            candidates=candidates,
            expected_commit=expected_commit,
            evaluated_at=evaluated_at,
            max_age_hours=max_age_hours,
        )
        if result.issue is not None or result.proof is None or result.route_id is None:
            issues.append(
                _proof_issue(
                    "DPONE_CAPABILITY_EVIDENCE_PROOF_INVALID",
                    str(index),
                    "Certification evidence failed canonical proof validation.",
                )
            )
            continue
        key = (result.route_id, result.proof.evidence_set)
        if key in proofs:
            issues.append(
                _proof_issue(
                    "DPONE_CAPABILITY_EVIDENCE_PROOF_DUPLICATE",
                    result.proof.evidence_set,
                    "Certification evidence contains a duplicate proof identity.",
                )
            )
            continue
        proofs[key] = result.proof.to_dict()
    return proofs, tuple(issues)


def has_independent_production_proofs(
    proofs: tuple[Mapping[str, Any], ...],
) -> bool:
    """Return whether enterprise promotion has two signer/deployment identities."""

    deployments = {proof.get("deployment_id") for proof in proofs}
    signers = {proof.get("signer_identity") for proof in proofs}
    return len(deployments) >= 2 and len(signers) >= 2


def _proof_issue(code: str, entity_id: str, message: str) -> CapabilityIssue:
    return CapabilityIssue(
        code=code,
        entity_kind="route_certification_evidence",
        entity_id=entity_id,
        message=message,
    )


__all__ = ["has_independent_production_proofs", "load_trusted_proofs"]
