"""Closed trust contracts for an externally signed SqlClient companion."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from dpone.contracts.airflow_deployment_trust_policy import CosignPublicKeyPolicy

_DIGEST = re.compile(r"[0-9a-f]{64}")
_IDENTITY = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:/@+-]{0,255}")
_ERROR = "mssql_native.sqlclient_companion_trust_invalid"


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def canonical_bytes(value: Mapping[str, Any]) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()


@dataclass(frozen=True, slots=True)
class SqlClientCompanionTrustPolicy:
    """Deployment-owned exact allow-list; release content cannot grant trust."""

    trust_tier: str
    environment: str
    key_id: str
    public_key_sha256: str
    companion_schema: str
    companion_version: str
    platform: str
    dpone_exact_version: str
    dpone_distribution_sha256: str
    dpone_compatibility: str
    cosign_exact_version: str
    cosign_executable_sha256: str
    source_identity: str
    release_identity: str
    release_manifest_sha256: str
    archive_sha256: str
    deployment_build_sha256: str
    revoked_key_ids: tuple[str, ...]
    revoked_release_identities: tuple[str, ...]
    revoked_deployment_builds: tuple[str, ...]
    cosign: CosignPublicKeyPolicy
    schema: str = "dpone.mssql-sqlclient.companion-trust-policy.v1"

    @classmethod
    def from_bytes(cls, raw: bytes) -> SqlClientCompanionTrustPolicy:
        value = _strict_object(raw)
        if raw != canonical_bytes(value):
            raise ValueError(_ERROR)
        fields = {
            "schema",
            "trust_tier",
            "environment",
            "key_id",
            "public_key_sha256",
            "companion_schema",
            "companion_version",
            "platform",
            "dpone_exact_version",
            "dpone_distribution_sha256",
            "dpone_compatibility",
            "cosign_exact_version",
            "cosign_executable_sha256",
            "source_identity",
            "release_identity",
            "release_manifest_sha256",
            "archive_sha256",
            "deployment_build_sha256",
            "revoked_key_ids",
            "revoked_release_identities",
            "revoked_deployment_builds",
            "cosign",
        }
        if set(value) != fields or value["schema"] != "dpone.mssql-sqlclient.companion-trust-policy.v1":
            raise ValueError(_ERROR)
        policy = cls(
            trust_tier=_enum(value["trust_tier"], {"qualification", "production"}),
            environment=_identity(value["environment"]),
            key_id=_identity(value["key_id"]),
            public_key_sha256=_digest(value["public_key_sha256"]),
            companion_schema=_identity(value["companion_schema"]),
            companion_version=_version(value["companion_version"]),
            platform=_enum(value["platform"], {"linux_arm64"}),
            dpone_exact_version=_version(value["dpone_exact_version"]),
            dpone_distribution_sha256=_digest(value["dpone_distribution_sha256"]),
            dpone_compatibility=_compatibility(value["dpone_compatibility"]),
            cosign_exact_version=_cosign_version(value["cosign_exact_version"]),
            cosign_executable_sha256=_digest(value["cosign_executable_sha256"]),
            source_identity=_digest(value["source_identity"]),
            release_identity=_identity(value["release_identity"]),
            release_manifest_sha256=_digest(value["release_manifest_sha256"]),
            archive_sha256=_digest(value["archive_sha256"]),
            deployment_build_sha256=_digest(value["deployment_build_sha256"]),
            revoked_key_ids=_identities(value["revoked_key_ids"]),
            revoked_release_identities=_identities(value["revoked_release_identities"]),
            revoked_deployment_builds=_digests(value["revoked_deployment_builds"]),
            cosign=CosignPublicKeyPolicy.from_mapping(value["cosign"]),
        )
        policy.assert_authorized()
        return policy

    @property
    def digest(self) -> str:
        return sha256_bytes(self.to_bytes())

    def to_bytes(self) -> bytes:
        return canonical_bytes(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "trust_tier": self.trust_tier,
            "environment": self.environment,
            "key_id": self.key_id,
            "public_key_sha256": self.public_key_sha256,
            "companion_schema": self.companion_schema,
            "companion_version": self.companion_version,
            "platform": self.platform,
            "dpone_exact_version": self.dpone_exact_version,
            "dpone_distribution_sha256": self.dpone_distribution_sha256,
            "dpone_compatibility": self.dpone_compatibility,
            "cosign_exact_version": self.cosign_exact_version,
            "cosign_executable_sha256": self.cosign_executable_sha256,
            "source_identity": self.source_identity,
            "release_identity": self.release_identity,
            "release_manifest_sha256": self.release_manifest_sha256,
            "archive_sha256": self.archive_sha256,
            "deployment_build_sha256": self.deployment_build_sha256,
            "revoked_key_ids": list(self.revoked_key_ids),
            "revoked_release_identities": list(self.revoked_release_identities),
            "revoked_deployment_builds": list(self.revoked_deployment_builds),
            "cosign": self.cosign.to_dict(),
        }

    def assert_authorized(self) -> None:
        if (
            self.key_id in self.revoked_key_ids
            or self.release_identity in self.revoked_release_identities
            or self.deployment_build_sha256 in self.revoked_deployment_builds
            or self.cosign.minimum_version != "3.0.4"
            or self.cosign.maximum_version_exclusive != "4.0.0"
            or self.cosign_exact_version != "3.0.4"
        ):
            raise ValueError(_ERROR)


@dataclass(frozen=True, slots=True)
class SqlClientCompanionVerificationReceipt:
    """Canonical evidence only; it is never an admission authority."""

    trust_tier: str
    environment: str
    key_id: str
    public_key_sha256: str
    policy_sha256: str
    archive_sha256: str
    sigstore_bundle_sha256: str
    signature_input_sha256: str
    release_manifest_sha256: str
    tree_inventory_sha256: str
    deployment_build_sha256: str
    source_identity: str
    release_identity: str
    verifier_identity: str
    verifier_version: str
    status: str = "VERIFIED"
    schema: str = "dpone.mssql-sqlclient.companion-verification.v1"

    def __post_init__(self) -> None:
        values = {name: getattr(self, name) for name in self.__dataclass_fields__}
        for name, value in values.items():
            if name in {"schema", "status"}:
                continue
            _digest(value) if name.endswith("sha256") else _identity(value)
        if (
            values["trust_tier"] not in {"qualification", "production"}
            or values["verifier_identity"] != "cosign_public_key_v1"
            or re.fullmatch(r"3\.[0-9]+\.[0-9]+(?:[-+].*)?", values["verifier_version"]) is None
            or values["release_identity"] != f"sha256:{values['release_manifest_sha256']}"
            or self.status != "VERIFIED"
            or self.schema != "dpone.mssql-sqlclient.companion-verification.v1"
        ):
            raise ValueError(_ERROR)

    def to_bytes(self) -> bytes:
        return canonical_bytes({name: getattr(self, name) for name in self.__dataclass_fields__})

    @property
    def digest(self) -> str:
        return sha256_bytes(self.to_bytes())


def _strict_object(raw: bytes) -> dict[str, Any]:
    def unique(rows: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in rows:
            if key in result:
                raise ValueError(_ERROR)
            result[key] = value
        return result

    try:
        value = json.loads(raw.decode(), object_pairs_hook=unique)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(_ERROR) from error
    if type(value) is not dict:
        raise ValueError(_ERROR)
    return value


def _identity(value: object) -> str:
    if type(value) is not str or _IDENTITY.fullmatch(value) is None:
        raise ValueError(_ERROR)
    return value


def _enum(value: object, allowed: set[str]) -> str:
    text = _identity(value)
    if text not in allowed:
        raise ValueError(_ERROR)
    return text


def _version(value: object) -> str:
    text = _identity(value)
    if re.fullmatch(r"0\.83\.[0-9]+", text) is None:
        raise ValueError(_ERROR)
    return text


def _compatibility(value: object) -> str:
    if type(value) is not str or value != ">=0.83.0,<0.84.0":
        raise ValueError(_ERROR)
    return value


def _cosign_version(value: object) -> str:
    if type(value) is not str or value != "3.0.4":
        raise ValueError(_ERROR)
    return value


def _digest(value: object) -> str:
    if type(value) is not str or _DIGEST.fullmatch(value) is None:
        raise ValueError(_ERROR)
    return value


def _identities(value: object) -> tuple[str, ...]:
    if type(value) is not list or len(value) > 64:
        raise ValueError(_ERROR)
    result = tuple(_identity(item) for item in value)
    if len(set(result)) != len(result):
        raise ValueError(_ERROR)
    return result


def _digests(value: object) -> tuple[str, ...]:
    if type(value) is not list or len(value) > 64:
        raise ValueError(_ERROR)
    result = tuple(_digest(item) for item in value)
    if len(set(result)) != len(result):
        raise ValueError(_ERROR)
    return result


__all__ = ["SqlClientCompanionTrustPolicy", "SqlClientCompanionVerificationReceipt", "canonical_bytes", "sha256_bytes"]
