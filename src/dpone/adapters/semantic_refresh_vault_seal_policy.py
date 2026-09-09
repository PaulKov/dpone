"""Version-pinned Vault resolver for semantic-refresh seal policy authority."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol

from dpone.adapters.semantic_refresh_vault_authority import (
    SemanticRefreshVaultAuthorityError,
    VaultSemanticRefreshAuthorityIndexReader,
)
from dpone.contracts.semantic_refresh_core import semantic_refresh_sha256
from dpone.ports.semantic_refresh_seal_policy import (
    SemanticRefreshSealPolicyAuthority,
    SemanticRefreshSealPolicySubject,
)

_SUBJECT_SCHEMA = "dpone.semantic-refresh-seal-policy-subject.v1"
_AUTHORITY_SCHEMA = "dpone.semantic-refresh-seal-policy-authority.v1"
_LOCATOR_SCHEMA = "dpone.semantic-refresh-seal-policy-locator.v1"


class _VaultKvClient(Protocol):
    def get_secret(self, *, mount_point: str, path: str) -> Mapping[str, Any]: ...


class VaultSemanticRefreshSealPolicyAuthority:
    """Load one policy document only when the pinned index authorizes it."""

    def __init__(
        self,
        *,
        client: _VaultKvClient,
        index: VaultSemanticRefreshAuthorityIndexReader,
        mount_point: str,
        path_prefix: str,
        expected_version: int = 1,
    ) -> None:
        if not mount_point or not path_prefix or path_prefix.endswith("/"):
            raise ValueError("Vault seal-policy mount and prefix are required")
        if isinstance(expected_version, bool) or expected_version <= 0:
            raise ValueError("Vault seal-policy version must be positive")
        self._client = client
        self._index = index
        self._mount_point = mount_point
        self._path_prefix = path_prefix
        self._expected_version = expected_version

    def load(self, subject: SemanticRefreshSealPolicySubject) -> SemanticRefreshSealPolicyAuthority:
        """Return an exact policy whose document, version, subject, and index agree."""

        if not isinstance(subject, SemanticRefreshSealPolicySubject):
            raise TypeError("seal policy subject must be typed")
        try:
            raw = self._client.get_secret(
                mount_point=self._mount_point,
                path=f"{self._path_prefix}/{semantic_refresh_seal_policy_locator(subject)}",
            )
            values = _without_metadata(raw, expected_version=self._expected_version)
            authority = _authority(values)
            if authority.subject != subject:
                raise SemanticRefreshVaultAuthorityError("Vault seal policy subject differs")
            if not self._index.authorizes_seal_policy(authority.seal_policy_authority_sha256):
                raise SemanticRefreshVaultAuthorityError("Vault seal policy is absent from the pinned index")
            if values != authority.to_dict():
                raise SemanticRefreshVaultAuthorityError("Vault seal policy document is noncanonical")
            return authority
        except SemanticRefreshVaultAuthorityError:
            raise
        except Exception as exc:
            raise SemanticRefreshVaultAuthorityError("protected Vault seal policy is unavailable") from exc


def semantic_refresh_vault_seal_policy_document(
    authority: SemanticRefreshSealPolicyAuthority,
) -> dict[str, object]:
    """Return the canonical create-once Vault payload, excluding KV metadata."""

    if not isinstance(authority, SemanticRefreshSealPolicyAuthority):
        raise TypeError("seal policy authority must be typed")
    return authority.to_dict()


def semantic_refresh_seal_policy_locator(subject: SemanticRefreshSealPolicySubject) -> str:
    """Derive a non-secret stable path from deployment and model identity."""

    return semantic_refresh_sha256(
        {
            "deployment_id": subject.deployment_id,
            "model_unique_id": subject.model_unique_id,
            "release_id": subject.release_id,
            "schema": _LOCATOR_SCHEMA,
        }
    )[7:]


def _authority(raw: Mapping[str, object]) -> SemanticRefreshSealPolicyAuthority:
    expected = {
        "clickhouse_input_mapping_sha256",
        "codec_mapping_certification_sha256",
        "issuer_attestation_sha256",
        "issuer_authority",
        "issuer_signature_sha256",
        "schema",
        "seal_policy_authority_sha256",
        "seal_policy_sha256",
        "subject",
    }
    if set(raw) != expected or raw.get("schema") != _AUTHORITY_SCHEMA:
        raise SemanticRefreshVaultAuthorityError("Vault seal policy fields are not closed")
    subject_raw = raw.get("subject")
    if not isinstance(subject_raw, Mapping):
        raise SemanticRefreshVaultAuthorityError("Vault seal policy subject is invalid")
    subject = _subject(subject_raw)
    return SemanticRefreshSealPolicyAuthority(
        subject=subject,
        clickhouse_input_mapping_sha256=_string(raw, "clickhouse_input_mapping_sha256"),
        codec_mapping_certification_sha256=_string(raw, "codec_mapping_certification_sha256"),
        seal_policy_sha256=_string(raw, "seal_policy_sha256"),
        issuer_authority=_string(raw, "issuer_authority"),
        issuer_attestation_sha256=_string(raw, "issuer_attestation_sha256"),
        issuer_signature_sha256=_string(raw, "issuer_signature_sha256"),
        seal_policy_authority_sha256=_string(raw, "seal_policy_authority_sha256"),
    )


def _subject(raw: Mapping[str, object]) -> SemanticRefreshSealPolicySubject:
    required = {
        "artifact_authority_sha256",
        "ddl_freeze_assurance_receipt_sha256",
        "deployment_id",
        "effective_key_mapping_sha256",
        "event_time_source_type",
        "model_unique_id",
        "ordered_writable_schema_sha256",
        "parquet_schema_mapping_sha256",
        "release_id",
        "route_certification_receipt_sha256",
        "schema",
        "serializer_sha256",
        "writer_exclusivity_assurance_receipt_sha256",
    }
    optional = {"utc_semantics_assurance_receipt_sha256"}
    if not required.issubset(raw) or not set(raw).issubset(required | optional) or raw.get("schema") != _SUBJECT_SCHEMA:
        raise SemanticRefreshVaultAuthorityError("Vault seal policy subject fields are not closed")
    utc = raw.get("utc_semantics_assurance_receipt_sha256")
    return SemanticRefreshSealPolicySubject(
        release_id=_string(raw, "release_id"),
        deployment_id=_string(raw, "deployment_id"),
        model_unique_id=_string(raw, "model_unique_id"),
        event_time_source_type=_string(raw, "event_time_source_type"),
        effective_key_mapping_sha256=_string(raw, "effective_key_mapping_sha256"),
        ordered_writable_schema_sha256=_string(raw, "ordered_writable_schema_sha256"),
        route_certification_receipt_sha256=_string(raw, "route_certification_receipt_sha256"),
        writer_exclusivity_assurance_receipt_sha256=_string(
            raw,
            "writer_exclusivity_assurance_receipt_sha256",
        ),
        ddl_freeze_assurance_receipt_sha256=_string(raw, "ddl_freeze_assurance_receipt_sha256"),
        artifact_authority_sha256=_string(raw, "artifact_authority_sha256"),
        serializer_sha256=_string(raw, "serializer_sha256"),
        parquet_schema_mapping_sha256=_string(raw, "parquet_schema_mapping_sha256"),
        utc_semantics_assurance_receipt_sha256=(None if utc is None else str(utc)),
    )


def _without_metadata(raw: Mapping[str, object], *, expected_version: int) -> dict[str, object]:
    metadata = raw.get("_metadata")
    if not isinstance(metadata, Mapping) or metadata.get("version") != expected_version:
        raise SemanticRefreshVaultAuthorityError("Vault seal policy version differs")
    return {str(key): value for key, value in raw.items() if key != "_metadata"}


def _string(raw: Mapping[str, object], field_name: str) -> str:
    value = raw.get(field_name)
    if not isinstance(value, str):
        raise SemanticRefreshVaultAuthorityError(f"Vault seal policy {field_name} is invalid")
    return value


__all__ = [
    "VaultSemanticRefreshSealPolicyAuthority",
    "semantic_refresh_seal_policy_locator",
    "semantic_refresh_vault_seal_policy_document",
]
