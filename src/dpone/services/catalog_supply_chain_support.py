"""Shared boundary helpers for signed catalog application services."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.contracts.catalog_supply_chain import (
    BUNDLE_KINDS,
    BUNDLE_SCHEMA,
    FORBIDDEN_SECRET_KEYS,
    MAX_CONTROL_NODES,
    MAX_CONTROL_TOKENS,
    MAX_FILE_BYTES,
    MAX_PAYLOAD_FILES,
    MAX_TOTAL_BYTES,
    PROFILE_CHECKS,
    REQUEST_SCHEMA,
    TRUST_POLICY_SCHEMA,
    CatalogBundleArtifact,
    CatalogBundleBuildRequest,
    CatalogBundleBuildResult,
    CatalogBundleError,
    CatalogBundleVerification,
    CatalogBundleVerifyRequest,
    CosignVerificationPolicy,
    ExtensionCheckResult,
    ExtensionConformanceError,
    ExtensionConformanceReport,
    bundle_id,
    canonical_json_bytes,
    conformance_id,
    policy_fingerprint,
    sha256_bytes,
)
from dpone.gitops.schema_validation import GitOpsSchemaValidator
from dpone.manifest.catalog_bundle_support import (
    RecipeBundleFile,
    RecipeBundleSupportError,
    collect_recipe_bundle_files,
    parse_bounded_json_mapping,
    parse_bounded_mapping,
    read_project_file,
    safe_project_relative,
    validate_recipe_bundle,
)
from dpone.ports.blob_signature import BlobSignatureVerifier


def validate_registered_schema(payload: Mapping[str, Any], expected_kind: str) -> bool:
    """Return whether a registered v1 schema accepts the payload."""

    issues = GitOpsSchemaValidator().validate(payload, expected_kind=expected_kind)
    if expected_kind == BUNDLE_SCHEMA and payload.get("schema") == BUNDLE_SCHEMA:
        issues = tuple(issue for issue in issues if not (issue.code == "schema_kind_mismatch" and issue.path == "kind"))
    return not issues


__all__ = [
    "BUNDLE_KINDS",
    "BUNDLE_SCHEMA",
    "BlobSignatureVerifier",
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
    "RecipeBundleFile",
    "RecipeBundleSupportError",
    "TRUST_POLICY_SCHEMA",
    "bundle_id",
    "canonical_json_bytes",
    "collect_recipe_bundle_files",
    "parse_bounded_json_mapping",
    "conformance_id",
    "parse_bounded_mapping",
    "policy_fingerprint",
    "read_project_file",
    "safe_project_relative",
    "sha256_bytes",
    "validate_recipe_bundle",
    "validate_registered_schema",
]
