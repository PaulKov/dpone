"""Bounded local evidence reader for route certification matrix publication."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from dpone.contracts.airflow_deployment import is_canonical_sha256_digest, release_id
from dpone.contracts.capability_discovery import RouteCertificationCandidateProtocol
from dpone.contracts.route_attestation import (
    RouteAttestationExpectedSubject,
    parse_aware_datetime,
    route_attestation_id,
    sha256_bytes,
)
from dpone.ops.route_certification_matrix_bundle import validate_certification_bundle
from dpone.ops.route_certification_matrix_crypto import (
    ProductionAttestationCryptoGate,
    require_production_attestation_crypto,
)
from dpone.ops.route_certification_matrix_models import (
    RouteCertificationDimensions,
    RouteCertificationMatrixIssue,
    RouteCertificationProof,
)
from dpone.readiness.route_attestation_files import RouteAttestationFileError, read_strict_json_mapping

_MAX_EVIDENCE_BYTES = 4 * 1024 * 1024
_RELEASE_FILE = "release-set.json"
_BUNDLE_FILE = "route_certification_bundle.json"
_ATTESTATION_FILE = "route-attestation.json"
_VERIFICATION_FILE = "route-attestation-verification.json"


@dataclass(frozen=True, slots=True)
class RouteCertificationEvidenceReadResult:
    route_id: str | None
    proof: RouteCertificationProof | None
    issue: RouteCertificationMatrixIssue | None


class RouteCertificationEvidenceReader:
    """Read one explicit proof set without directory discovery.

    Consistency checks remain local-only. Production promotion additionally
    requires cryptographic re-verification through ``crypto_gate`` (default:
    Sigstore/cosign artifacts beside the attestation).
    """

    def __init__(self, *, crypto_gate: ProductionAttestationCryptoGate | None = None) -> None:
        self._crypto_gate = crypto_gate or require_production_attestation_crypto

    def read(
        self,
        directory: Path,
        *,
        candidates: Sequence[RouteCertificationCandidateProtocol],
        expected_commit: str,
        evaluated_at: datetime,
        max_age_hours: int,
    ) -> RouteCertificationEvidenceReadResult:
        evidence_set = _evidence_set_label(directory)
        if directory.is_symlink() or not directory.is_dir():
            return _unmatched_issue(evidence_set, "route_matrix.evidence_directory_unsafe")
        bundle_path = directory / _BUNDLE_FILE
        try:
            bundle, bundle_bytes = _read(bundle_path, label="route certification bundle")
        except RouteCertificationFileViolation as exc:
            return _unmatched_issue(evidence_set, exc.code)
        candidate = _candidate_for_bundle(bundle, candidates=candidates)
        if candidate is None:
            return _unmatched_issue(evidence_set, "route_matrix.route_unknown_or_ambiguous")

        blockers: list[str] = []
        release_payload: dict[str, Any] = {}
        release_bytes = b""
        try:
            release_payload, release_bytes = _read(directory / _RELEASE_FILE, label="release-set")
        except RouteCertificationFileViolation as exc:
            blockers.append(exc.code)
        release_identity = _validate_release(release_payload, expected_commit=expected_commit, blockers=blockers)
        blockers.extend(
            validate_certification_bundle(
                bundle,
                bundle_directory=directory,
                candidate=candidate,
                release_identity=release_identity,
                expected_commit=expected_commit,
                evaluated_at=evaluated_at,
                max_age_hours=max_age_hours,
            )
        )

        attestation_path = directory / _ATTESTATION_FILE
        verification_path = directory / _VERIFICATION_FILE
        production_attempted = _present(attestation_path) or _present(verification_path)
        attestation_bytes = b""
        verification_bytes = b""
        deployment_id: str | None = None
        environment: str | None = None
        signer_identity: str | None = None
        expires_at: str | None = None
        if production_attempted:
            production = _validate_production_proof(
                attestation_path=attestation_path,
                verification_path=verification_path,
                bundle_bytes=bundle_bytes,
                candidate=candidate,
                release_identity=release_identity,
                evaluated_at=evaluated_at,
                blockers=blockers,
            )
            attestation_bytes = production.attestation_bytes
            verification_bytes = production.verification_bytes
            deployment_id = production.deployment_id
            environment = production.environment
            signer_identity = production.signer_identity
            expires_at = production.expires_at
            expected_subject = production.expected_subject
            if expected_subject is not None:
                crypto_blocker = self._crypto_gate(
                    directory=directory,
                    attestation_path=attestation_path,
                    certification_bundle_path=bundle_path,
                    expected=expected_subject,
                )
                if crypto_blocker is not None:
                    blockers.append(crypto_blocker)
            elif not blockers:
                blockers.append("route_matrix.attestation_crypto_subject_incomplete")

        failed = bool(blockers)
        level = "experimental" if failed else ("production-certified" if production_attempted else "route-certified")
        proof = RouteCertificationProof(
            evidence_set=evidence_set,
            evidence_status="FAIL" if failed else "PASS",
            certification_level=level,
            release_id=release_identity,
            deployment_id=deployment_id,
            environment=environment,
            signer_identity=signer_identity,
            expires_at=expires_at,
            release_set_sha256=sha256_bytes(release_bytes) if release_bytes else None,
            certification_bundle_sha256=sha256_bytes(bundle_bytes),
            attestation_sha256=sha256_bytes(attestation_bytes) if attestation_bytes else None,
            verification_sha256=sha256_bytes(verification_bytes) if verification_bytes else None,
            production_attempted=production_attempted,
            blockers=tuple(dict.fromkeys(blockers)),
        )
        return RouteCertificationEvidenceReadResult(candidate.certification_id, proof, None)


@dataclass(frozen=True, slots=True)
class _ProductionProof:
    attestation_bytes: bytes
    verification_bytes: bytes
    deployment_id: str | None
    environment: str | None
    signer_identity: str | None
    expires_at: str | None
    expected_subject: RouteAttestationExpectedSubject | None


class RouteCertificationFileViolation(ValueError):
    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def _read(path: Path, *, label: str) -> tuple[dict[str, Any], bytes]:
    if not _present(path):
        raise RouteCertificationFileViolation("route_matrix.evidence_file_missing")
    try:
        return read_strict_json_mapping(path, max_bytes=_MAX_EVIDENCE_BYTES, label=label)
    except RouteAttestationFileError as exc:
        raise RouteCertificationFileViolation("route_matrix.evidence_file_unsafe") from exc


def _validate_release(payload: Mapping[str, Any], *, expected_commit: str, blockers: list[str]) -> str | None:
    if not payload:
        return None
    identity = payload.get("release_id")
    if payload.get("schema") != "dpone.release-set.v1" or not is_canonical_sha256_digest(identity):
        blockers.append("route_matrix.release_schema_invalid")
        return str(identity) if isinstance(identity, str) else None
    if release_id(payload) != identity:
        blockers.append("route_matrix.release_digest_mismatch")
    provenance = payload.get("provenance")
    source_commit = provenance.get("source_commit") if isinstance(provenance, Mapping) else None
    if source_commit != expected_commit:
        blockers.append("route_matrix.release_commit_mismatch")
    return str(identity)


def _validate_production_proof(
    *,
    attestation_path: Path,
    verification_path: Path,
    bundle_bytes: bytes,
    candidate: RouteCertificationCandidateProtocol,
    release_identity: str | None,
    evaluated_at: datetime,
    blockers: list[str],
) -> _ProductionProof:
    if not _present(attestation_path) or not _present(verification_path):
        blockers.append("route_matrix.production_proof_incomplete")
        return _ProductionProof(b"", b"", None, None, None, None, None)
    try:
        attestation, attestation_bytes = _read(attestation_path, label="route attestation")
        verification, verification_bytes = _read(verification_path, label="route attestation verification")
    except RouteCertificationFileViolation as exc:
        blockers.append(exc.code)
        return _ProductionProof(b"", b"", None, None, None, None, None)
    claims = attestation.get("claims")
    route = claims.get("route") if isinstance(claims, Mapping) else None
    certification = claims.get("certification") if isinstance(claims, Mapping) else None
    subject = claims.get("subject") if isinstance(claims, Mapping) else None
    validity = claims.get("validity") if isinstance(claims, Mapping) else None
    signer = verification.get("signer")
    expected_route = _attested_route(candidate)
    attestation_id = attestation.get("attestation_id")
    if (
        attestation.get("schema") != "dpone.route-attestation.v1"
        or not isinstance(claims, dict)
        or not isinstance(route, Mapping)
        or dict(route) != expected_route
        or attestation_id != route_attestation_id(claims)
    ):
        blockers.append("route_matrix.attestation_route_invalid")
    expected_attestation_sha = sha256_bytes(attestation_bytes)
    expected_bundle_sha = sha256_bytes(bundle_bytes)
    if (
        not isinstance(certification, Mapping)
        or certification.get("bundle_sha256") != expected_bundle_sha
        or certification.get("profile") != "vendor_live"
        or certification.get("level") != "certified"
    ):
        blockers.append("route_matrix.attestation_certification_invalid")
    if (
        verification.get("schema") != "dpone.route-attestation-verification.v1"
        or verification.get("decision") != "verified"
        or verification.get("attestation_id") != attestation_id
        or verification.get("attestation_sha256") != expected_attestation_sha
        or verification.get("certification_bundle_sha256") != expected_bundle_sha
        or verification.get("route_id") != candidate.certification_id
        or verification.get("errors") != []
    ):
        blockers.append("route_matrix.attestation_verification_invalid")
    deployment_id = _text(subject, "deployment_id")
    environment = _text(subject, "environment")
    signer_identity = _text(signer, "certificate_identity")
    expires_at = _text(validity, "expires_at")
    if (
        not is_canonical_sha256_digest(deployment_id)
        or release_identity is None
        or _text(subject, "release_id") != release_identity
        or verification.get("release_id") != release_identity
        or verification.get("deployment_id") != deployment_id
        or environment != "production"
        or verification.get("environment") != environment
        or verification.get("authorization_profile") != _text(subject, "authorization_profile")
        or _text(signer, "backend") != "cosign_keyless_v1"
        or not _text(signer, "certificate_oidc_issuer")
        or not signer_identity
    ):
        blockers.append("route_matrix.production_subject_invalid")
    _validate_validity(validity, verification=verification, evaluated_at=evaluated_at, blockers=blockers)
    runtime_image = _text(subject, "runtime_image_digest")
    authorization_profile = _text(subject, "authorization_profile")
    expected_subject = None
    if (
        release_identity is not None
        and deployment_id is not None
        and environment is not None
        and runtime_image
        and authorization_profile
    ):
        expected_subject = RouteAttestationExpectedSubject(
            route_id=candidate.certification_id,
            release_id=release_identity,
            deployment_id=deployment_id,
            environment=environment,
            runtime_image_digest=runtime_image,
            authorization_profile=authorization_profile,
        )
    return _ProductionProof(
        attestation_bytes,
        verification_bytes,
        deployment_id,
        environment,
        signer_identity,
        expires_at,
        expected_subject,
    )


def _validate_validity(
    validity: object,
    *,
    verification: Mapping[str, Any],
    evaluated_at: datetime,
    blockers: list[str],
) -> None:
    if not isinstance(validity, Mapping):
        blockers.append("route_matrix.attestation_validity_invalid")
        return
    start = parse_aware_datetime(validity.get("not_before"))
    end = parse_aware_datetime(validity.get("expires_at"))
    receipt_validity = verification.get("validity")
    receipt_start = _text(receipt_validity, "not_before")
    receipt_end = _text(receipt_validity, "expires_at")
    if (
        start is None
        or end is None
        or start >= end
        or not start <= evaluated_at < end
        or receipt_start != validity.get("not_before")
        or receipt_end != validity.get("expires_at")
    ):
        blockers.append("route_matrix.attestation_validity_invalid")


def _candidate_for_bundle(
    bundle: Mapping[str, Any], *, candidates: Sequence[RouteCertificationCandidateProtocol]
) -> RouteCertificationCandidateProtocol | None:
    route = bundle.get("route")
    if not isinstance(route, Mapping):
        return None
    key = tuple(_normalized(route.get(field)) for field in ("source", "sink", "strategy"))
    claim = bundle.get("matrix_claim")
    claim_route = claim.get("route") if isinstance(claim, Mapping) else None
    claimed_id = claim_route.get("route_id") if isinstance(claim_route, Mapping) else None
    matches = [
        candidate
        for candidate in candidates
        if candidate.key == key and (claimed_id is None or candidate.certification_id == claimed_id)
    ]
    return matches[0] if len(matches) == 1 else None


def _attested_route(candidate: RouteCertificationCandidateProtocol) -> dict[str, str]:
    dimensions = RouteCertificationDimensions(
        candidate.source,
        candidate.sink,
        candidate.strategy,
        candidate.transport,
        candidate.schema_evolution,
        candidate.airflow_runtime_mode,
    )
    return {"route_id": candidate.certification_id, **dimensions.to_dict(), "sampling_mode": candidate.sampling_mode}


def _unmatched_issue(evidence_set: str, code: str) -> RouteCertificationEvidenceReadResult:
    return RouteCertificationEvidenceReadResult(
        route_id=None,
        proof=None,
        issue=RouteCertificationMatrixIssue(code, evidence_set, _message(code)),
    )


def _message(code: str) -> str:
    messages = {
        "route_matrix.evidence_directory_unsafe": "Evidence directory is missing, unsafe, or a symlink.",
        "route_matrix.evidence_file_unsafe": "Evidence file is missing, unsafe, oversized, or invalid JSON.",
        "route_matrix.evidence_file_missing": "Required evidence file is missing.",
        "route_matrix.route_unknown_or_ambiguous": "Evidence route is unknown or maps to multiple variants.",
    }
    return messages.get(code, "Route certification evidence is invalid.")


def _evidence_set_label(directory: Path) -> str:
    label = directory.name.strip()
    return label[:128] or "evidence"


def _present(path: Path) -> bool:
    return path.exists() or path.is_symlink()


def _text(value: object, key: str) -> str | None:
    if not isinstance(value, Mapping):
        return None
    item = value.get(key)
    return str(item) if isinstance(item, str) and item else None


def _normalized(value: object) -> str:
    return str(value or "").strip().lower().replace("-", "_")


__all__ = ["RouteCertificationEvidenceReadResult", "RouteCertificationEvidenceReader"]
