"""Trusted policy contracts for Airflow deployment artifact signatures."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from dpone.contracts.airflow_deployment import canonical_fingerprint, is_canonical_sha256_digest

POLICY_SCHEMA = "dpone.airflow-deployment-trust-policy.v1"
_KEY_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")
_KEY_FILE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,252}$")
_MAX_KEYS = 8


@dataclass(frozen=True, slots=True)
class TrustedPublicKey:
    key_id: str
    file: str
    sha256: str

    @classmethod
    def from_entry(cls, key_id: object, value: object) -> TrustedPublicKey:
        payload = _exact_mapping(value, {"file", "sha256"}, "trusted public key")
        key_id = _text(key_id, "trusted public key key_id", maximum=128)
        file = _text(payload["file"], "trusted public key file", maximum=253)
        if not _KEY_ID.fullmatch(key_id) or not _KEY_FILE.fullmatch(file):
            raise ValueError("trusted public key identity is invalid")
        return cls(key_id=key_id, file=file, sha256=_digest(payload["sha256"], "trusted public key sha256"))

    def to_dict(self) -> dict[str, str]:
        return {"file": self.file, "sha256": self.sha256}


@dataclass(frozen=True, slots=True)
class CosignPublicKeyPolicy:
    minimum_version: str
    maximum_version_exclusive: str
    timeout_seconds: int

    @classmethod
    def from_mapping(cls, value: object) -> CosignPublicKeyPolicy:
        payload = _exact_mapping(
            value,
            {"minimum_version", "maximum_version_exclusive", "timeout_seconds"},
            "cosign policy",
        )
        timeout = payload["timeout_seconds"]
        if isinstance(timeout, bool) or not isinstance(timeout, int) or not 1 <= timeout <= 60:
            raise ValueError("cosign timeout_seconds must be between 1 and 60")
        return cls(
            minimum_version=_text(payload["minimum_version"], "cosign minimum_version", maximum=32),
            maximum_version_exclusive=_text(
                payload["maximum_version_exclusive"],
                "cosign maximum_version_exclusive",
                maximum=32,
            ),
            timeout_seconds=timeout,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "minimum_version": self.minimum_version,
            "maximum_version_exclusive": self.maximum_version_exclusive,
            "timeout_seconds": self.timeout_seconds,
        }


@dataclass(frozen=True, slots=True)
class AirflowDeploymentTrustPolicy:
    """Trusted production expectations loaded outside release artifacts."""

    trusted_public_keys: tuple[TrustedPublicKey, ...]
    cosign: CosignPublicKeyPolicy
    allowed_environments: tuple[str, ...]
    allowed_artifact_registry_refs: tuple[str, ...]
    allowed_registry_scope_ids: tuple[str, ...]
    allowed_source_projects: tuple[str, ...]
    allowed_source_refs: tuple[str, ...]
    revoked_attestation_ids: tuple[str, ...]
    revoked_public_key_ids: tuple[str, ...]
    schema: str = POLICY_SCHEMA
    trust_tier: str = "production"
    attestations: str = "required_for_prod"
    backend: str = "cosign_public_key_v1"

    @classmethod
    def from_bytes(cls, raw: bytes) -> AirflowDeploymentTrustPolicy:
        model = cls.from_json_bytes(raw)
        if raw != model.to_bytes():
            raise ValueError("artifact trust policy must use canonical JSON bytes")
        return model

    @classmethod
    def from_json_bytes(cls, raw: bytes) -> AirflowDeploymentTrustPolicy:
        """Parse human-authored JSON before writing canonical policy bytes."""

        return cls.from_mapping(_strict_json(raw))

    @classmethod
    def from_mapping(cls, value: object) -> AirflowDeploymentTrustPolicy:
        fields = {
            "schema",
            "trust_tier",
            "attestations",
            "backend",
            "trusted_public_keys",
            "cosign",
            "allowed_environments",
            "allowed_artifact_registry_refs",
            "allowed_registry_scope_ids",
            "allowed_source_projects",
            "allowed_source_refs",
            "revoked_attestation_ids",
            "revoked_public_key_ids",
        }
        payload = _exact_mapping(value, fields, "artifact trust policy")
        if (
            payload["schema"] != POLICY_SCHEMA
            or payload["trust_tier"] != "production"
            or payload["attestations"] != "required_for_prod"
            or payload["backend"] != "cosign_public_key_v1"
        ):
            raise ValueError("artifact trust policy authority is invalid")
        raw_keys = payload["trusted_public_keys"]
        if not isinstance(raw_keys, Mapping) or not 1 <= len(raw_keys) <= _MAX_KEYS:
            raise ValueError("artifact trust policy requires a bounded public key set")
        keys = tuple(TrustedPublicKey.from_entry(key_id, item) for key_id, item in raw_keys.items())
        policy = cls(
            trusted_public_keys=keys,
            cosign=CosignPublicKeyPolicy.from_mapping(payload["cosign"]),
            allowed_environments=_text_tuple(payload["allowed_environments"], "allowed_environments"),
            allowed_artifact_registry_refs=_text_tuple(
                payload["allowed_artifact_registry_refs"], "allowed_artifact_registry_refs"
            ),
            allowed_registry_scope_ids=_digest_tuple(
                payload["allowed_registry_scope_ids"],
                "allowed_registry_scope_ids",
                require_non_empty=True,
            ),
            allowed_source_projects=_text_tuple(payload["allowed_source_projects"], "allowed_source_projects"),
            allowed_source_refs=_text_tuple(payload["allowed_source_refs"], "allowed_source_refs"),
            revoked_attestation_ids=_digest_tuple(payload["revoked_attestation_ids"], "revoked_attestation_ids"),
            revoked_public_key_ids=_text_tuple(
                payload["revoked_public_key_ids"], "revoked_public_key_ids", allow_empty=True
            ),
        )
        return policy

    @property
    def fingerprint(self) -> str:
        return canonical_fingerprint(self.to_dict())

    def to_bytes(self) -> bytes:
        return _canonical_bytes(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "trust_tier": self.trust_tier,
            "attestations": self.attestations,
            "backend": self.backend,
            "trusted_public_keys": {item.key_id: item.to_dict() for item in self.trusted_public_keys},
            "cosign": self.cosign.to_dict(),
            "allowed_environments": list(self.allowed_environments),
            "allowed_artifact_registry_refs": list(self.allowed_artifact_registry_refs),
            "allowed_registry_scope_ids": list(self.allowed_registry_scope_ids),
            "allowed_source_projects": list(self.allowed_source_projects),
            "allowed_source_refs": list(self.allowed_source_refs),
            "revoked_attestation_ids": list(self.revoked_attestation_ids),
            "revoked_public_key_ids": list(self.revoked_public_key_ids),
        }


def _strict_json(raw: bytes) -> dict[str, Any]:
    def unique(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            if key in result:
                raise ValueError("artifact trust policy contains duplicate JSON keys")
            result[key] = value
        return result

    try:
        payload = json.loads(raw.decode("utf-8"), object_pairs_hook=unique)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("artifact trust policy must be valid UTF-8 JSON") from exc
    if not isinstance(payload, dict):
        raise ValueError("artifact trust policy must be a JSON object")
    return payload


def _canonical_bytes(value: Mapping[str, Any]) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()


def _exact_mapping(value: object, fields: set[str], label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != fields:
        raise ValueError(f"{label} fields are invalid")
    return value


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


def _text_tuple(value: object, label: str, *, allow_empty: bool = False) -> tuple[str, ...]:
    if not isinstance(value, list) or (not value and not allow_empty) or len(value) > 64:
        raise ValueError(f"{label} must be a bounded array")
    items = tuple(_text(item, label, maximum=512) for item in value)
    if len(set(items)) != len(items):
        raise ValueError(f"{label} must contain unique values")
    return items


def _digest_tuple(
    value: object,
    label: str,
    *,
    require_non_empty: bool = False,
) -> tuple[str, ...]:
    if not isinstance(value, list) or (require_non_empty and not value) or len(value) > 256:
        raise ValueError(f"{label} must be a bounded array")
    items = tuple(_digest(item, label) for item in value)
    if len(set(items)) != len(items):
        raise ValueError(f"{label} must contain unique values")
    return items


__all__ = [
    "POLICY_SCHEMA",
    "AirflowDeploymentTrustPolicy",
    "CosignPublicKeyPolicy",
    "TrustedPublicKey",
]
