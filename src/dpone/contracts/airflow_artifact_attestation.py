"""Dependency-free contracts for signed Airflow deployment artifacts."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from dpone.contracts.airflow_deployment import canonical_fingerprint, is_canonical_sha256_digest

ATTESTATION_SCHEMA = "dpone.airflow-artifact-attestation.v1"
VERIFICATION_SCHEMA = "dpone.airflow-artifact-attestation-verification.v1"

_GIT_SHA = re.compile(r"^[0-9a-f]{40,64}$")


def sha256_bytes(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


@dataclass(frozen=True, slots=True)
class AirflowArtifactExpectedSubject:
    """Exact immutable deployment values that signed claims must equal."""

    release_id: str
    deployment_id: str
    environment: str
    artifact_registry_ref: str
    registry_scope_id: str
    release_set_sha256: str
    deployment_sha256: str
    airflow_index_sha256: str
    runtime_image_digest: str

    def __post_init__(self) -> None:
        for name in (
            "release_id",
            "deployment_id",
            "registry_scope_id",
            "release_set_sha256",
            "deployment_sha256",
            "airflow_index_sha256",
            "runtime_image_digest",
        ):
            _digest(getattr(self, name), name)
        _text(self.environment, "environment", maximum=63)
        _text(self.artifact_registry_ref, "artifact_registry_ref", maximum=128)

    def to_dict(self) -> dict[str, str]:
        return {
            "release_id": self.release_id,
            "deployment_id": self.deployment_id,
            "environment": self.environment,
            "artifact_registry_ref": self.artifact_registry_ref,
            "registry_scope_id": self.registry_scope_id,
            "release_set_sha256": self.release_set_sha256,
            "deployment_sha256": self.deployment_sha256,
            "airflow_index_sha256": self.airflow_index_sha256,
            "runtime_image_digest": self.runtime_image_digest,
        }


@dataclass(frozen=True, slots=True)
class AirflowArtifactObservedSubject:
    """Deployment values independently observed at one verification boundary."""

    release_id: str
    deployment_id: str
    environment: str
    artifact_registry_ref: str
    registry_scope_id: str
    release_set_sha256: str
    deployment_sha256: str
    runtime_image_digest: str
    airflow_index_sha256: str | None = None

    def __post_init__(self) -> None:
        for name in (
            "release_id",
            "deployment_id",
            "registry_scope_id",
            "release_set_sha256",
            "deployment_sha256",
            "runtime_image_digest",
        ):
            _digest(getattr(self, name), name)
        if self.airflow_index_sha256 is not None:
            _digest(self.airflow_index_sha256, "airflow_index_sha256")
        _text(self.environment, "environment", maximum=63)
        _text(self.artifact_registry_ref, "artifact_registry_ref", maximum=128)

    @classmethod
    def from_expected(
        cls,
        subject: AirflowArtifactExpectedSubject,
    ) -> AirflowArtifactObservedSubject:
        return cls(**subject.to_dict())

    @property
    def observed_claims(self) -> tuple[str, ...]:
        return tuple(name for name, value in self.to_dict().items() if value is not None)

    @property
    def unobserved_claims(self) -> tuple[str, ...]:
        return () if self.airflow_index_sha256 is not None else ("airflow_index_sha256",)

    def to_dict(self) -> dict[str, str | None]:
        return {
            "release_id": self.release_id,
            "deployment_id": self.deployment_id,
            "environment": self.environment,
            "artifact_registry_ref": self.artifact_registry_ref,
            "registry_scope_id": self.registry_scope_id,
            "release_set_sha256": self.release_set_sha256,
            "deployment_sha256": self.deployment_sha256,
            "airflow_index_sha256": self.airflow_index_sha256,
            "runtime_image_digest": self.runtime_image_digest,
        }

    def matches(self, signed_subject: Mapping[str, Any]) -> bool:
        return all(value is None or signed_subject.get(name) == value for name, value in self.to_dict().items())


@dataclass(frozen=True, slots=True)
class AirflowArtifactAttestation:
    """Canonical unsigned statement that an external authority signs."""

    attestation_id: str
    claims: dict[str, Any]
    schema: str = ATTESTATION_SCHEMA

    @classmethod
    def create(cls, claims: Mapping[str, Any]) -> AirflowArtifactAttestation:
        canonical = _claims(claims)
        return cls(attestation_id=canonical_fingerprint(canonical), claims=canonical)

    @classmethod
    def from_bytes(cls, raw: bytes) -> AirflowArtifactAttestation:
        payload = _strict_json(raw, "artifact attestation")
        if set(payload) != {"schema", "attestation_id", "claims"}:
            raise ValueError("artifact attestation fields are invalid")
        model = cls.create(_mapping(payload.get("claims"), "claims"))
        if payload.get("schema") != ATTESTATION_SCHEMA:
            raise ValueError("artifact attestation schema is invalid")
        if payload.get("attestation_id") != model.attestation_id:
            raise ValueError("artifact attestation identity is invalid")
        if raw != model.to_bytes():
            raise ValueError("artifact attestation must use canonical JSON bytes")
        return model

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "attestation_id": self.attestation_id,
            "claims": self.claims,
        }

    def to_bytes(self) -> bytes:
        return _canonical_bytes(self.to_dict())


@dataclass(frozen=True, slots=True)
class AirflowArtifactAttestationPackage:
    """Bounded statement and signature bundle fetched from an artifact registry."""

    attestation: bytes
    sigstore_bundle: bytes

    def __post_init__(self) -> None:
        if not self.attestation or len(self.attestation) > 256 * 1024:
            raise ValueError("artifact attestation statement size is invalid")
        if not self.sigstore_bundle or len(self.sigstore_bundle) > 2 * 1024 * 1024:
            raise ValueError("artifact attestation bundle size is invalid")
        AirflowArtifactAttestation.from_bytes(self.attestation)


@dataclass(frozen=True, slots=True)
class AirflowArtifactAttestationVerification:
    decision: str
    code: str
    message: str
    attestation_id: str | None
    policy_fingerprint: str
    public_key_id: str | None
    public_key_sha256: str | None
    verifier_version: str | None
    verified_at: str

    @property
    def is_verified(self) -> bool:
        return self.decision == "verified"

    @property
    def decision_sha256(self) -> str:
        """Return a deterministic decision identity independent of observation time."""

        return canonical_fingerprint(
            {
                "schema": "dpone.airflow-artifact-attestation-decision.v1",
                "decision": self.decision,
                "code": self.code,
                "attestation_id": self.attestation_id,
                "policy_fingerprint": self.policy_fingerprint,
                "public_key_id": self.public_key_id,
                "public_key_sha256": self.public_key_sha256,
                "verifier_version": self.verifier_version,
            }
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": VERIFICATION_SCHEMA,
            "decision": self.decision,
            "code": self.code,
            "message": self.message,
            "decision_sha256": self.decision_sha256,
            "attestation_id": self.attestation_id,
            "policy_fingerprint": self.policy_fingerprint,
            "public_key_id": self.public_key_id,
            "public_key_sha256": self.public_key_sha256,
            "verifier_version": self.verifier_version,
            "verified_at": self.verified_at,
            "errors": [] if self.is_verified else [{"code": self.code, "message": self.message}],
        }


def _claims(value: Mapping[str, Any]) -> dict[str, Any]:
    payload = _exact_mapping(value, {"subject", "source", "publication", "issued_at"}, "attestation claims")
    subject = AirflowArtifactExpectedSubject(**_exact_text_mapping(payload["subject"], _SUBJECT_FIELDS, "subject"))
    source = _exact_text_mapping(payload["source"], {"project", "ref", "git_sha"}, "source")
    if not _GIT_SHA.fullmatch(source["git_sha"]):
        raise ValueError("attestation source git_sha is invalid")
    publication = _exact_text_mapping(
        payload["publication"],
        {"evidence_sha256", "verification_mode"},
        "publication",
    )
    _digest(publication["evidence_sha256"], "publication evidence_sha256")
    if publication["verification_mode"] != "remote_readback_sha256":
        raise ValueError("attestation publication verification_mode is invalid")
    issued_at = _text(payload["issued_at"], "issued_at", maximum=40)
    return {
        "subject": subject.to_dict(),
        "source": source,
        "publication": publication,
        "issued_at": issued_at,
    }


_SUBJECT_FIELDS = {
    "release_id",
    "deployment_id",
    "environment",
    "artifact_registry_ref",
    "registry_scope_id",
    "release_set_sha256",
    "deployment_sha256",
    "airflow_index_sha256",
    "runtime_image_digest",
}


def _strict_json(raw: bytes, label: str) -> dict[str, Any]:
    def unique(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, item in items:
            if key in result:
                raise ValueError(f"{label} contains duplicate JSON keys")
            result[key] = item
        return result

    try:
        payload = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=unique,
            parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)),
        )
    except UnicodeDecodeError as exc:
        raise ValueError(f"{label} must be UTF-8 JSON") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"{label} must be valid JSON") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"{label} must be a JSON object")
    return payload


def _canonical_bytes(value: Mapping[str, Any]) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")


def _mapping(value: object, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{label} must be an object")
    return value


def _exact_mapping(value: object, fields: set[str], label: str) -> Mapping[str, Any]:
    payload = _mapping(value, label)
    if set(payload) != fields:
        raise ValueError(f"{label} fields are invalid")
    return payload


def _exact_text_mapping(value: object, fields: set[str], label: str) -> dict[str, str]:
    payload = _exact_mapping(value, fields, label)
    return {field: _text(payload[field], f"{label}.{field}", maximum=512) for field in fields}


def _text(value: object, label: str, *, maximum: int) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > maximum
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        raise ValueError(f"{label} is invalid")
    return value


def _digest(value: object, label: str) -> str:
    if not is_canonical_sha256_digest(value):
        raise ValueError(f"{label} must be a canonical sha256 digest")
    return str(value)


__all__ = [
    "ATTESTATION_SCHEMA",
    "VERIFICATION_SCHEMA",
    "AirflowArtifactAttestation",
    "AirflowArtifactAttestationPackage",
    "AirflowArtifactAttestationVerification",
    "AirflowArtifactExpectedSubject",
    "sha256_bytes",
]
