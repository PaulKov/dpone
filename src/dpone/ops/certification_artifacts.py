"""Certification artifact reading and normalization."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from dpone.contracts.certification_trust import (
    CertificationTrustDecision,
    certification_trust,
    is_local_only_evidence,
)
from dpone.ops.checksums import sha256_file

_STRICT_CERTIFICATION_NAMES = frozenset({"certification", "certification_report"})
_CERTIFICATION_EVIDENCE_SLOTS = frozenset(
    {
        "cdc_apply_certification",
        "certification",
        "certification_report",
        "certification_suite",
        "connector_certification",
        "integration_matrix_certification",
        "live_state_reconciliation",
        "native_transfer_certification",
        "performance_certification",
        "route_certification_pack",
        "route_live_evidence_bundle",
        "strategy_bundle",
        "strategy_certification_bundle",
    }
)


def artifact_requires_certification_trust(
    name: str,
    payload: Mapping[str, Any],
) -> bool:
    """Identify certification evidence without changing generic artifact semantics."""

    normalized_name = name.strip().lower().replace("-", "_")
    schema = str(payload.get("schema_version") or payload.get("schema") or "").lower()
    profile = str(payload.get("profile") or "").lower()
    return (
        normalized_name in _CERTIFICATION_EVIDENCE_SLOTS
        or "certification" in normalized_name
        or "certification" in schema
        or "evidence_status" in payload
        or profile in {"mock_contract", "vendor_live"}
        or "certified_transports" in payload
    )


def artifact_payload_passed(
    payload: Mapping[str, Any],
    *,
    name: str | None = None,
    require_certification_status: bool = False,
) -> bool:
    """Evaluate generic evidence while honoring strict certification status."""

    if _has_contradictory_failures(payload):
        return False
    requires_trust = require_certification_status or (
        name is not None and artifact_requires_certification_trust(name, payload)
    )
    if requires_trust or "evidence_status" in payload:
        return certification_trust(payload).passed
    if payload.get("status") in {"regression", "failed", "blocked", "rollback_required"}:
        return False
    if "passed" in payload:
        return payload["passed"] is True
    blockers = payload.get("blockers", [])
    violations = payload.get("violations", [])
    findings = payload.get("findings", [])
    return not bool(blockers or violations or findings)


def production_artifact_payload_passed(
    payload: Mapping[str, Any],
    *,
    name: str,
) -> bool:
    """Evaluate evidence at a generic production decision boundary.

    Behavioral certification results may be collected by evidence packs, but
    a generic release gate or environment promotion must not upgrade caller-
    supplied JSON to production authority. Even a ``VERIFIED`` field is only a
    claim here; route-specific cryptographic verification is the canonical
    authority path.
    """

    if (
        is_local_only_evidence(payload)
        or artifact_requires_certification_trust(name, payload)
        or "local_behavior_passed" in payload
        or "production_certification" in payload
        or "local_evidence_status" in payload
        or str(payload.get("environment_class") or "").strip().upper() == "LOCAL_DOCKER"
    ):
        return False
    return artifact_payload_passed(payload, name=name) and not _has_contradictory_failures(payload)


def _has_contradictory_failures(payload: Mapping[str, Any]) -> bool:
    if any(bool(payload.get(key)) for key in ("blockers", "violations", "findings", "errors", "failed_stages")):
        return True
    status = payload.get("status")
    return isinstance(status, str) and status.strip().lower() in {
        "blocked",
        "failed",
        "regression",
        "rollback_required",
    }


@dataclass(frozen=True, slots=True)
class CertificationEvidenceItem:
    name: str
    path: str
    sha256: str
    passed: bool
    required: bool
    missing: bool
    summary: str
    evidence_status: str | None = None
    release_id: str | None = None
    identity_status: str | None = None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


class CertificationArtifactReader:
    """Reads suite evidence artifacts through one small, reusable policy."""

    def read(
        self,
        *,
        name: str,
        path: str | Path | None,
        required: bool,
        expected_release_id: str | None = None,
    ) -> CertificationEvidenceItem | None:
        certification_required = name in _STRICT_CERTIFICATION_NAMES
        if path is None and not required:
            return None
        artifact_path = Path(path or "")
        if not artifact_path.exists():
            return CertificationEvidenceItem(
                name=name,
                path=str(artifact_path),
                sha256="0" * 64,
                passed=False,
                required=required,
                missing=True,
                summary="artifact missing",
                evidence_status="UNVERIFIED" if certification_required else None,
                identity_status="MISSING" if expected_release_id is not None else None,
            )
        payload = self._payload(artifact_path)
        trust = certification_trust(payload) if certification_required or "evidence_status" in payload else None
        artifact_release_id = payload.get("release_id")
        release_identity = artifact_release_id if isinstance(artifact_release_id, str) else None
        identity_status = _release_identity_status(
            release_identity,
            expected_release_id=expected_release_id,
        )
        identity_matches = identity_status in {None, "MATCHED"}
        return CertificationEvidenceItem(
            name=name,
            path=str(artifact_path),
            sha256=sha256_file(artifact_path),
            passed=identity_matches
            and artifact_payload_passed(
                payload,
                name=name,
                require_certification_status=certification_required,
            ),
            required=required,
            missing=False,
            summary=_identity_summary(identity_status, release_identity) or self._summary(payload),
            evidence_status=trust.evidence_status if trust is not None else None,
            release_id=release_identity,
            identity_status=identity_status,
        )

    @staticmethod
    def case_count(path: str | Path) -> int:
        payload = CertificationArtifactReader._payload(Path(path))
        results = payload.get("results", [])
        return len(results) if isinstance(results, list) else 0

    @staticmethod
    def _payload(path: Path) -> Mapping[str, Any]:
        if not path.exists() or path.suffix.lower() != ".json":
            return {}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}
        return payload if isinstance(payload, Mapping) else {}

    @staticmethod
    def _passed(payload: Mapping[str, Any]) -> bool:
        return artifact_payload_passed(payload)

    @staticmethod
    def _summary(payload: Mapping[str, Any]) -> str:
        if "evidence_status" in payload:
            return f"evidence_status={certification_trust(payload).evidence_status}"
        for key in ("blockers", "violations", "findings", "results", "items", "events", "strategy_rows"):
            value = payload.get(key)
            if isinstance(value, list | tuple):
                return f"{key}={len(value)}"
        if "status" in payload:
            return f"status={payload['status']}"
        return "passed" if artifact_payload_passed(payload) else "failed"


def _release_identity_status(
    release_identity: str | None,
    *,
    expected_release_id: str | None,
) -> str | None:
    if expected_release_id is None:
        return None
    if release_identity is None:
        return "MISSING"
    return "MATCHED" if release_identity == expected_release_id else "MISMATCHED"


def _identity_summary(identity_status: str | None, release_identity: str | None) -> str:
    if identity_status == "MISSING":
        return "release_id is required for a release-bound certification suite"
    if identity_status == "MISMATCHED":
        return f"release_id mismatch: artifact={release_identity}"
    return ""


__all__ = [
    "CertificationArtifactReader",
    "CertificationEvidenceItem",
    "CertificationTrustDecision",
    "artifact_payload_passed",
    "artifact_requires_certification_trust",
    "certification_trust",
    "production_artifact_payload_passed",
]
