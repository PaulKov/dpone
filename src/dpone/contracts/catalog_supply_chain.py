"""Cohesive contract facade for signed catalogs and extension evidence."""

from __future__ import annotations

from dpone.contracts.blob_signature import CosignVerificationPolicy
from dpone.contracts.catalog_bundle import (
    BUNDLE_KINDS,
    BUNDLE_SCHEMA,
    MAX_CONTROL_NODES,
    MAX_CONTROL_TOKENS,
    MAX_FILE_BYTES,
    MAX_PAYLOAD_FILES,
    MAX_TOTAL_BYTES,
    TRUST_POLICY_SCHEMA,
    CatalogBundleArtifact,
    CatalogBundleBuildRequest,
    CatalogBundleBuildResult,
    CatalogBundleError,
    CatalogBundleVerification,
    CatalogBundleVerifyRequest,
    bundle_id,
    canonical_json_bytes,
    policy_fingerprint,
    sha256_bytes,
)
from dpone.contracts.credential_security import FORBIDDEN_SECRET_KEYS
from dpone.contracts.extension_conformance import (
    PROFILE_CHECKS,
    REQUEST_SCHEMA,
    ExtensionCheckResult,
    ExtensionConformanceError,
    ExtensionConformanceReport,
    conformance_id,
)

__all__ = [
    "BUNDLE_KINDS",
    "BUNDLE_SCHEMA",
    "CatalogBundleArtifact",
    "CatalogBundleBuildRequest",
    "CatalogBundleBuildResult",
    "CatalogBundleError",
    "CatalogBundleVerification",
    "CatalogBundleVerifyRequest",
    "CosignVerificationPolicy",
    "ExtensionCheckResult",
    "ExtensionConformanceError",
    "ExtensionConformanceReport",
    "FORBIDDEN_SECRET_KEYS",
    "MAX_CONTROL_NODES",
    "MAX_CONTROL_TOKENS",
    "MAX_FILE_BYTES",
    "MAX_PAYLOAD_FILES",
    "MAX_TOTAL_BYTES",
    "PROFILE_CHECKS",
    "REQUEST_SCHEMA",
    "TRUST_POLICY_SCHEMA",
    "bundle_id",
    "canonical_json_bytes",
    "conformance_id",
    "policy_fingerprint",
    "sha256_bytes",
]
