"""Canonical protected artifact-authority identity for semantic refresh V2."""

from __future__ import annotations

from collections.abc import Mapping

from dpone.contracts.semantic_refresh_evidence_common import (
    SemanticRefreshContractError,
    require_closed_mapping,
    require_digest,
    require_positive_int,
    require_text,
    require_utc_timestamp,
    semantic_refresh_sha256,
)

ARTIFACT_AUTHORITY_IDENTITY_SCHEMA = "dpone.semantic-refresh-artifact-authority-identity.v1"
_FIELDS = frozenset(
    {
        "provider",
        "provider_profile",
        "endpoint_authority_id",
        "bucket_or_container_authority_id",
        "kms_key_authority_id",
        "capability_evidence_sha256",
        "writer_scope",
        "artifact_prefix",
        "encryption_policy_sha256",
        "retention_policy_id",
        "retention_policy_sha256",
        "retention_days",
        "retention_issued_at",
        "retention_until",
        "max_artifact_bytes",
    }
)
_TEXT_FIELDS = frozenset(
    {
        "provider_profile",
        "endpoint_authority_id",
        "bucket_or_container_authority_id",
        "kms_key_authority_id",
        "writer_scope",
        "artifact_prefix",
        "retention_policy_id",
    }
)
_DIGEST_FIELDS = frozenset({"capability_evidence_sha256", "encryption_policy_sha256", "retention_policy_sha256"})


def semantic_refresh_artifact_authority_sha256(value: object) -> str:
    """Digest one closed protected authority mapping using the sole canonical preimage."""

    raw = require_closed_mapping(value, "artifact_authority", required=_FIELDS)
    canonical: dict[str, object] = {"provider": require_text(raw.get("provider"), "provider")}
    if canonical["provider"] != "s3":
        raise SemanticRefreshContractError("semantic-refresh artifact provider must equal s3")
    for field in _TEXT_FIELDS:
        canonical[field] = require_text(raw.get(field), field)
    artifact_prefix = canonical["artifact_prefix"]
    assert isinstance(artifact_prefix, str)
    if artifact_prefix.startswith("/") or ".." in artifact_prefix.split("/"):
        raise SemanticRefreshContractError("artifact_prefix must be relative and confined")
    for field in _DIGEST_FIELDS:
        canonical[field] = require_digest(raw.get(field), field)
    canonical["retention_days"] = require_positive_int(raw.get("retention_days"), "retention_days")
    canonical["retention_issued_at"] = require_utc_timestamp(raw.get("retention_issued_at"), "retention_issued_at")
    canonical["retention_until"] = require_utc_timestamp(raw.get("retention_until"), "retention_until")
    canonical["max_artifact_bytes"] = require_positive_int(raw.get("max_artifact_bytes"), "max_artifact_bytes")
    return semantic_refresh_sha256({"schema": ARTIFACT_AUTHORITY_IDENTITY_SCHEMA, **canonical})


def semantic_refresh_artifact_authority_preimage(value: object) -> Mapping[str, object]:
    """Expose the deterministic schema-tagged preimage for audit/debug tooling."""

    raw = require_closed_mapping(value, "artifact_authority", required=_FIELDS)
    semantic_refresh_artifact_authority_sha256(raw)
    return {"schema": ARTIFACT_AUTHORITY_IDENTITY_SCHEMA, **dict(raw)}


__all__ = [
    "ARTIFACT_AUTHORITY_IDENTITY_SCHEMA",
    "semantic_refresh_artifact_authority_preimage",
    "semantic_refresh_artifact_authority_sha256",
]
