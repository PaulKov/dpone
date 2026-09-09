"""Build and verify signed Airflow deployment artifact statements."""

from __future__ import annotations

import datetime as dt
import json
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any, Protocol

from dpone.contracts.airflow_artifact_attestation import (
    AirflowArtifactAttestation,
    AirflowArtifactAttestationVerification,
    AirflowArtifactExpectedSubject,
    AirflowArtifactObservedSubject,
    sha256_bytes,
)
from dpone.contracts.blob_signature import BlobSignatureVerification

if TYPE_CHECKING:
    from dpone.contracts.airflow_deployment_trust_policy import (
        AirflowDeploymentTrustPolicy,
        CosignPublicKeyPolicy,
    )

_MAX_EVIDENCE_BYTES = 1024 * 1024
_UTC = dt.timezone(dt.timedelta(0))


class AirflowArtifactSignatureVerifier(Protocol):
    def verify_blob(
        self,
        *,
        blob: bytes,
        sigstore_bundle: bytes,
        public_key: bytes,
        policy: CosignPublicKeyPolicy,
    ) -> BlobSignatureVerification: ...


class AirflowArtifactAttestationBuilder:
    """Derive deterministic claims from one exact publication commitment."""

    def build(
        self,
        *,
        release_bytes: bytes,
        deployment_bytes: bytes,
        airflow_index_bytes: bytes,
        release_id: str,
        deployment_id: str,
        environment: str,
        artifact_registry_ref: str,
        registry_scope_id: str,
        publication_evidence: bytes,
        source_project: str,
        source_ref: str,
        source_git_sha: str,
        issued_at: dt.datetime,
    ) -> AirflowArtifactAttestation:
        evidence = _publication_evidence(publication_evidence)
        _require_equal(evidence, "release_id", release_id)
        _require_equal(evidence, "deployment_id", deployment_id)
        _require_equal(evidence, "environment", environment)
        _require_equal(evidence, "artifact_registry_ref", artifact_registry_ref)
        commitment = _mapping(evidence.get("publication_commitment"), "publication commitment")
        if (
            commitment.get("schema") != "dpone.airflow-publication-commitment.v1"
            or commitment.get("verification_mode") != "remote_readback_sha256"
            or commitment.get("projection_verified") is not True
        ):
            raise ValueError("publication commitment is not an exact remote read-back receipt")
        _require_equal(commitment, "registry_scope_id", registry_scope_id)

        _require_descriptor(commitment, "release", release_bytes)
        _require_descriptor(commitment, "deployment", deployment_bytes)
        _require_descriptor(commitment, "airflow_index", airflow_index_bytes)
        deployment = _strict_json(deployment_bytes, "deployment")
        runtime_image_digest = deployment.get("runtime_image_digest")
        if not isinstance(runtime_image_digest, str):
            raise ValueError("deployment runtime image digest is missing")
        if issued_at.tzinfo is None or issued_at.utcoffset() is None:
            raise ValueError("attestation issued_at must be offset-aware")
        subject = AirflowArtifactExpectedSubject(
            release_id=release_id,
            deployment_id=deployment_id,
            environment=environment,
            artifact_registry_ref=artifact_registry_ref,
            registry_scope_id=registry_scope_id,
            release_set_sha256=sha256_bytes(release_bytes),
            deployment_sha256=sha256_bytes(deployment_bytes),
            airflow_index_sha256=sha256_bytes(airflow_index_bytes),
            runtime_image_digest=runtime_image_digest,
        )
        return AirflowArtifactAttestation.create(
            {
                "subject": subject.to_dict(),
                "source": {
                    "project": source_project,
                    "ref": source_ref,
                    "git_sha": source_git_sha,
                },
                "publication": {
                    "evidence_sha256": sha256_bytes(publication_evidence),
                    "verification_mode": "remote_readback_sha256",
                },
                "issued_at": _utc_text(issued_at),
            }
        )


class AirflowArtifactAttestationVerificationService:
    """Verify signature infrastructure, policy, and exact deployment subject."""

    def __init__(
        self,
        *,
        signature_verifier: AirflowArtifactSignatureVerifier,
        clock: Callable[[], dt.datetime] | None = None,
    ) -> None:
        self._signature_verifier = signature_verifier
        self._clock = clock or (lambda: dt.datetime.now(_UTC))

    def verify(
        self,
        *,
        attestation: bytes,
        sigstore_bundle: bytes,
        policy: AirflowDeploymentTrustPolicy,
        public_keys: Mapping[str, bytes],
        expected: AirflowArtifactExpectedSubject | AirflowArtifactObservedSubject,
    ) -> AirflowArtifactAttestationVerification:
        now = self._aware_now()
        try:
            statement = AirflowArtifactAttestation.from_bytes(attestation)
        except ValueError:
            return _receipt(
                "invalid",
                "DPONE_ARTIFACT_ATTESTATION_INVALID",
                "artifact attestation bytes are invalid",
                policy=policy,
                now=now,
            )
        if statement.attestation_id in policy.revoked_attestation_ids:
            return _receipt(
                "invalid",
                "DPONE_ARTIFACT_ATTESTATION_REVOKED",
                "artifact attestation is revoked",
                policy=policy,
                now=now,
                statement=statement,
            )
        key_error = _public_key_error(policy, public_keys)
        if key_error is not None:
            return _receipt(
                "unverified",
                "DPONE_ARTIFACT_ATTESTATION_KEY_MISMATCH",
                key_error,
                policy=policy,
                now=now,
                statement=statement,
            )
        signature = self._verify_signature(
            statement=statement,
            attestation=attestation,
            sigstore_bundle=sigstore_bundle,
            policy=policy,
            public_keys=public_keys,
        )
        if signature[0] is None:
            result = signature[1]
            return _receipt(
                result.status,
                (
                    "DPONE_ARTIFACT_ATTESTATION_VERIFIER_UNAVAILABLE"
                    if result.status == "unverified"
                    else "DPONE_ARTIFACT_ATTESTATION_SIGNATURE_INVALID"
                ),
                "artifact attestation signature could not be verified",
                policy=policy,
                now=now,
                statement=statement,
                verifier_version=result.verifier_version,
            )
        key_id, result = signature
        key = next(item for item in policy.trusted_public_keys if item.key_id == key_id)
        claims = statement.claims
        subject = claims["subject"]
        subject_matches = (
            expected.matches(subject)
            if isinstance(expected, AirflowArtifactObservedSubject)
            else subject == expected.to_dict()
        )
        if not subject_matches:
            return _receipt(
                "invalid",
                "DPONE_ARTIFACT_ATTESTATION_SUBJECT_MISMATCH",
                "artifact attestation does not match the exact deployment subject",
                policy=policy,
                now=now,
                statement=statement,
                key=key,
                verifier_version=result.verifier_version,
            )
        source = claims["source"]
        if source["project"] not in policy.allowed_source_projects or source["ref"] not in policy.allowed_source_refs:
            return _receipt(
                "invalid",
                "DPONE_ARTIFACT_ATTESTATION_SOURCE_NOT_ALLOWED",
                "artifact attestation source is not allowed by policy",
                policy=policy,
                now=now,
                statement=statement,
                key=key,
                verifier_version=result.verifier_version,
            )
        if (
            expected.environment not in policy.allowed_environments
            or expected.artifact_registry_ref not in policy.allowed_artifact_registry_refs
            or expected.registry_scope_id not in policy.allowed_registry_scope_ids
        ):
            return _receipt(
                "invalid",
                "DPONE_ARTIFACT_ATTESTATION_POLICY_DENIED",
                "deployment environment or artifact registry is not allowed by policy",
                policy=policy,
                now=now,
                statement=statement,
                key=key,
                verifier_version=result.verifier_version,
            )
        return _receipt(
            "verified",
            "DPONE_ARTIFACT_ATTESTATION_VERIFIED",
            "artifact attestation is valid for this deployment",
            policy=policy,
            now=now,
            statement=statement,
            key=key,
            verifier_version=result.verifier_version,
        )

    def _verify_signature(
        self,
        *,
        statement: AirflowArtifactAttestation,
        attestation: bytes,
        sigstore_bundle: bytes,
        policy: AirflowDeploymentTrustPolicy,
        public_keys: Mapping[str, bytes],
    ) -> tuple[str | None, BlobSignatureVerification]:
        unavailable: BlobSignatureVerification | None = None
        for key in policy.trusted_public_keys:
            if key.key_id in policy.revoked_public_key_ids:
                continue
            result = self._signature_verifier.verify_blob(
                blob=attestation,
                sigstore_bundle=sigstore_bundle,
                public_key=public_keys[key.key_id],
                policy=policy.cosign,
            )
            if result.status == "verified":
                return key.key_id, result
            if result.status == "unverified":
                unavailable = result
        return None, unavailable or BlobSignatureVerification.invalid()

    def _aware_now(self) -> dt.datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("attestation clock must return an offset-aware datetime")
        return value.astimezone(_UTC)


def _public_key_error(policy: AirflowDeploymentTrustPolicy, values: Mapping[str, bytes]) -> str | None:
    if set(values) != {item.key_id for item in policy.trusted_public_keys}:
        return "trusted public key set is incomplete or contains unknown keys"
    for item in policy.trusted_public_keys:
        if sha256_bytes(values[item.key_id]) != item.sha256:
            return "trusted public key bytes do not match the pinned digest"
    return None


def _receipt(
    decision: str,
    code: str,
    message: str,
    *,
    policy: AirflowDeploymentTrustPolicy,
    now: dt.datetime,
    statement: AirflowArtifactAttestation | None = None,
    key: object | None = None,
    verifier_version: str | None = None,
) -> AirflowArtifactAttestationVerification:
    return AirflowArtifactAttestationVerification(
        decision=decision,
        code=code,
        message=message,
        attestation_id=None if statement is None else statement.attestation_id,
        policy_fingerprint=policy.fingerprint,
        public_key_id=getattr(key, "key_id", None),
        public_key_sha256=getattr(key, "sha256", None),
        verifier_version=verifier_version,
        verified_at=_utc_text(now),
    )


def _publication_evidence(raw: bytes) -> dict[str, Any]:
    if len(raw) > _MAX_EVIDENCE_BYTES:
        raise ValueError("publication evidence exceeds the size limit")
    payload = _strict_json(raw, "publication evidence")
    if payload.get("schema") != "dpone.airflow-artifact-publish.v2":
        raise ValueError("publication evidence must use the exact v2 schema")
    if payload.get("status") not in {"published", "already_published"}:
        raise ValueError("publication evidence is not successful")
    return payload


def _require_descriptor(commitment: Mapping[str, Any], name: str, raw: bytes) -> None:
    descriptor = _mapping(commitment.get(name), name)
    if descriptor.get("sha256") != sha256_bytes(raw) or descriptor.get("bytes") != len(raw):
        raise ValueError(f"publication commitment {name} does not match local bytes")


def _require_equal(value: Mapping[str, Any], field: str, expected: str) -> None:
    if value.get(field) != expected:
        raise ValueError(f"publication evidence {field} does not match")


def _strict_json(raw: bytes, label: str) -> dict[str, Any]:
    def unique(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            if key in result:
                raise ValueError(f"{label} contains duplicate keys")
            result[key] = value
        return result

    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=unique)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"{label} must be valid UTF-8 JSON") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be an object")
    return value


def _mapping(value: object, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{label} must be an object")
    return value


def _utc_text(value: dt.datetime) -> str:
    return value.astimezone(_UTC).isoformat().replace("+00:00", "Z")


__all__ = [
    "AirflowArtifactAttestationBuilder",
    "AirflowArtifactAttestationVerificationService",
    "AirflowArtifactSignatureVerifier",
]
