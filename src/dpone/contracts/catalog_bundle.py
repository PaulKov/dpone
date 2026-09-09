"""Pure contracts and deterministic identities for signed catalog bundles."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from dpone.contracts.airflow_deployment import canonical_fingerprint, is_canonical_sha256_digest

BUNDLE_SCHEMA = "dpone.catalog-bundle.v1"
TRUST_POLICY_SCHEMA = "dpone.catalog-trust-policy.v1"
VERIFICATION_SCHEMA = "dpone.catalog-bundle-verification.v1"
BUNDLE_KINDS = frozenset({"recipe_catalog", "connection_registry"})
MAX_PAYLOAD_FILES = 1001
MAX_FILE_BYTES = 4 * 1024 * 1024
MAX_TOTAL_BYTES = 64 * 1024 * 1024
MAX_CONTROL_TOKENS = 32_000
MAX_CONTROL_NODES = 16_000


class CatalogBundleError(ValueError):
    """A catalog bundle violated a stable public contract."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True, slots=True)
class CatalogBundleArtifact:
    logical_id: str
    path: str
    sha256: str
    size_bytes: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "logical_id": self.logical_id,
            "path": self.path,
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
        }


@dataclass(frozen=True, slots=True)
class CatalogBundleBuildRequest:
    project_root: str
    kind: str
    source: str
    bundle_root: str
    publisher_id: str
    environment: str | None = None


@dataclass(frozen=True, slots=True)
class CatalogBundleBuildResult:
    bundle_id: str
    bundle_dir: str
    manifest_path: str
    artifacts: int
    status: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": "dpone.catalog-bundle-build.v1",
            "bundle_id": self.bundle_id,
            "bundle_dir": self.bundle_dir,
            "manifest_path": self.manifest_path,
            "artifacts": self.artifacts,
            "status": self.status,
        }


@dataclass(frozen=True, slots=True)
class CatalogBundleVerifyRequest:
    bundle_dir: str
    sigstore_bundle: str
    policy: str
    trusted_root: str
    output: str


@dataclass(frozen=True, slots=True)
class CatalogBundleVerification:
    decision: str
    code: str
    bundle_id: str | None
    bundle_manifest_sha256: str
    kind: str | None
    publisher_id: str | None
    environment: str | None
    policy_fingerprint: str
    verified_artifacts: int
    verifier_version: str | None
    verified_at: str
    errors: tuple[dict[str, str], ...] = ()

    @property
    def is_verified(self) -> bool:
        return self.decision == "verified"

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": VERIFICATION_SCHEMA,
            "decision": self.decision,
            "code": self.code,
            "bundle_id": self.bundle_id,
            "bundle_manifest_sha256": self.bundle_manifest_sha256,
            "kind": self.kind,
            "publisher_id": self.publisher_id,
            "environment": self.environment,
            "policy_fingerprint": self.policy_fingerprint,
            "verified_artifacts": self.verified_artifacts,
            "verifier_version": self.verifier_version,
            "verified_at": self.verified_at,
            "errors": [dict(item) for item in self.errors],
        }


def sha256_bytes(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


def bundle_id(payload: Mapping[str, Any]) -> str:
    """Hash semantic manifest fields without the self-reference."""

    normalized = dict(payload)
    normalized.pop("bundle_id", None)
    artifacts = normalized.get("artifacts")
    if isinstance(artifacts, list):
        normalized["artifacts"] = sorted(
            artifacts,
            key=lambda item: str(item.get("logical_id", "")) if isinstance(item, Mapping) else "",
        )
    return canonical_fingerprint(normalized)


def policy_fingerprint(payload: Mapping[str, Any]) -> str:
    return canonical_fingerprint(payload)


def canonical_json_bytes(payload: Mapping[str, Any]) -> bytes:
    return (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")


def require_digest(value: object, *, label: str) -> str:
    if not is_canonical_sha256_digest(value):
        raise CatalogBundleError("DPONE_CATALOG_BUNDLE_INTEGRITY_FAILED", f"{label} must be a SHA-256 digest.")
    return str(value)


__all__ = [
    "BUNDLE_KINDS",
    "BUNDLE_SCHEMA",
    "CatalogBundleArtifact",
    "CatalogBundleBuildRequest",
    "CatalogBundleBuildResult",
    "CatalogBundleError",
    "CatalogBundleVerifyRequest",
    "CatalogBundleVerification",
    "MAX_FILE_BYTES",
    "MAX_CONTROL_NODES",
    "MAX_CONTROL_TOKENS",
    "MAX_PAYLOAD_FILES",
    "MAX_TOTAL_BYTES",
    "TRUST_POLICY_SCHEMA",
    "VERIFICATION_SCHEMA",
    "bundle_id",
    "canonical_json_bytes",
    "policy_fingerprint",
    "require_digest",
    "sha256_bytes",
]
