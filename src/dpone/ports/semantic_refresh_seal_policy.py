"""Protected policy boundary for issuing semantic-refresh seal receipts."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from dpone.contracts.semantic_refresh_artifact_authority_identity import (
    semantic_refresh_artifact_authority_sha256 as _canonical_artifact_authority_sha256,
)
from dpone.contracts.semantic_refresh_core import semantic_refresh_sha256
from dpone.ports.semantic_refresh_mssql_authority import (
    MssqlProtectedArtifactAuthority,
    MssqlProtectedWritableColumn,
)

_DIGEST_PREFIX = "sha256:"
_SUBJECT_SCHEMA = "dpone.semantic-refresh-seal-policy-subject.v1"
_AUTHORITY_SCHEMA = "dpone.semantic-refresh-seal-policy-authority.v1"


@dataclass(frozen=True, slots=True)
class SemanticRefreshSealPolicySubject:
    """Deployment/model projection that an independent policy must authorize."""

    release_id: str
    deployment_id: str
    model_unique_id: str
    event_time_source_type: str
    effective_key_mapping_sha256: str
    ordered_writable_schema_sha256: str
    route_certification_receipt_sha256: str
    writer_exclusivity_assurance_receipt_sha256: str
    ddl_freeze_assurance_receipt_sha256: str
    artifact_authority_sha256: str
    serializer_sha256: str
    parquet_schema_mapping_sha256: str
    utc_semantics_assurance_receipt_sha256: str | None = None

    def __post_init__(self) -> None:
        _text(self.model_unique_id, "model_unique_id")
        if self.event_time_source_type not in {"date", "datetime2(6)"}:
            raise ValueError("seal policy event-time source type is unsupported")
        for field_name in (
            "release_id",
            "deployment_id",
            "effective_key_mapping_sha256",
            "ordered_writable_schema_sha256",
            "route_certification_receipt_sha256",
            "writer_exclusivity_assurance_receipt_sha256",
            "ddl_freeze_assurance_receipt_sha256",
            "artifact_authority_sha256",
            "serializer_sha256",
            "parquet_schema_mapping_sha256",
        ):
            _digest(getattr(self, field_name), field_name)
        if self.event_time_source_type == "datetime2(6)":
            _digest(
                self.utc_semantics_assurance_receipt_sha256,
                "utc_semantics_assurance_receipt_sha256",
            )
        elif self.utc_semantics_assurance_receipt_sha256 is not None:
            raise ValueError("date seal policy subject forbids UTC assurance")

    def to_dict(self) -> dict[str, object]:
        result: dict[str, object] = {
            "artifact_authority_sha256": self.artifact_authority_sha256,
            "ddl_freeze_assurance_receipt_sha256": self.ddl_freeze_assurance_receipt_sha256,
            "deployment_id": self.deployment_id,
            "effective_key_mapping_sha256": self.effective_key_mapping_sha256,
            "event_time_source_type": self.event_time_source_type,
            "model_unique_id": self.model_unique_id,
            "ordered_writable_schema_sha256": self.ordered_writable_schema_sha256,
            "parquet_schema_mapping_sha256": self.parquet_schema_mapping_sha256,
            "release_id": self.release_id,
            "route_certification_receipt_sha256": self.route_certification_receipt_sha256,
            "schema": _SUBJECT_SCHEMA,
            "serializer_sha256": self.serializer_sha256,
            "writer_exclusivity_assurance_receipt_sha256": (self.writer_exclusivity_assurance_receipt_sha256),
        }
        if self.utc_semantics_assurance_receipt_sha256 is not None:
            result["utc_semantics_assurance_receipt_sha256"] = self.utc_semantics_assurance_receipt_sha256
        return result


@dataclass(frozen=True, slots=True)
class SemanticRefreshSealPolicyAuthority:
    """Signed protected policy values used to issue one model's receipts."""

    subject: SemanticRefreshSealPolicySubject
    clickhouse_input_mapping_sha256: str
    codec_mapping_certification_sha256: str
    seal_policy_sha256: str
    issuer_authority: str
    issuer_attestation_sha256: str
    issuer_signature_sha256: str
    seal_policy_authority_sha256: str

    def __post_init__(self) -> None:
        if not isinstance(self.subject, SemanticRefreshSealPolicySubject):
            raise TypeError("seal policy authority subject must be typed")
        for field_name in (
            "clickhouse_input_mapping_sha256",
            "codec_mapping_certification_sha256",
            "seal_policy_sha256",
            "issuer_attestation_sha256",
            "issuer_signature_sha256",
        ):
            _digest(getattr(self, field_name), field_name)
        _text(self.issuer_authority, "issuer_authority")
        expected = semantic_refresh_sha256(self._unsigned())
        if self.seal_policy_authority_sha256 != expected:
            raise ValueError("seal policy authority digest differs from its content")

    @classmethod
    def build(
        cls,
        *,
        subject: SemanticRefreshSealPolicySubject,
        clickhouse_input_mapping_sha256: str,
        codec_mapping_certification_sha256: str,
        seal_policy_sha256: str,
        issuer_authority: str,
        issuer_attestation_sha256: str,
        issuer_signature_sha256: str,
    ) -> SemanticRefreshSealPolicyAuthority:
        unsigned = {
            "clickhouse_input_mapping_sha256": clickhouse_input_mapping_sha256,
            "codec_mapping_certification_sha256": codec_mapping_certification_sha256,
            "issuer_attestation_sha256": issuer_attestation_sha256,
            "issuer_authority": issuer_authority,
            "issuer_signature_sha256": issuer_signature_sha256,
            "schema": _AUTHORITY_SCHEMA,
            "seal_policy_sha256": seal_policy_sha256,
            "subject": subject.to_dict(),
        }
        return cls(
            subject=subject,
            clickhouse_input_mapping_sha256=clickhouse_input_mapping_sha256,
            codec_mapping_certification_sha256=codec_mapping_certification_sha256,
            seal_policy_sha256=seal_policy_sha256,
            issuer_authority=issuer_authority,
            issuer_attestation_sha256=issuer_attestation_sha256,
            issuer_signature_sha256=issuer_signature_sha256,
            seal_policy_authority_sha256=semantic_refresh_sha256(unsigned),
        )

    def _unsigned(self) -> dict[str, object]:
        return {
            "clickhouse_input_mapping_sha256": self.clickhouse_input_mapping_sha256,
            "codec_mapping_certification_sha256": self.codec_mapping_certification_sha256,
            "issuer_attestation_sha256": self.issuer_attestation_sha256,
            "issuer_authority": self.issuer_authority,
            "issuer_signature_sha256": self.issuer_signature_sha256,
            "schema": _AUTHORITY_SCHEMA,
            "seal_policy_sha256": self.seal_policy_sha256,
            "subject": self.subject.to_dict(),
        }

    def to_dict(self) -> dict[str, object]:
        return {
            **self._unsigned(),
            "seal_policy_authority_sha256": self.seal_policy_authority_sha256,
        }


class SemanticRefreshSealPolicyAuthorityPort(Protocol):
    """Resolve a policy only from independent protected state."""

    def load(self, subject: SemanticRefreshSealPolicySubject) -> SemanticRefreshSealPolicyAuthority:
        """Return the exact signed authority or fail closed."""


class SemanticRefreshSealCodecAuthorityPort(Protocol):
    """Expose code-derived byte and schema identities for the pinned codec."""

    @property
    def serializer_sha256(self) -> str:
        """Return the digest of the complete deterministic writer configuration."""

    def parquet_schema_mapping_sha256(
        self,
        columns: tuple[MssqlProtectedWritableColumn, ...],
    ) -> str:
        """Return the exact ordered SQL Server-to-Parquet mapping digest."""


def semantic_refresh_artifact_authority_sha256(
    authority: MssqlProtectedArtifactAuthority,
) -> str:
    """Digest the exact protected provider/prefix/encryption/retention closure."""

    if not isinstance(authority, MssqlProtectedArtifactAuthority):
        raise TypeError("artifact authority must be a protected typed authority")
    return _canonical_artifact_authority_sha256(
        {
            "artifact_prefix": authority.artifact_prefix,
            "bucket_or_container_authority_id": authority.bucket_or_container_authority_id,
            "capability_evidence_sha256": authority.capability_evidence_sha256,
            "endpoint_authority_id": authority.endpoint_authority_id,
            "encryption_policy_sha256": authority.encryption_policy_sha256,
            "kms_key_authority_id": authority.kms_key_authority_id,
            "max_artifact_bytes": authority.max_artifact_bytes,
            "provider": authority.provider,
            "provider_profile": authority.provider_profile,
            "retention_days": authority.retention_days,
            "retention_issued_at": authority.retention_issued_at,
            "retention_policy_id": authority.retention_policy_id,
            "retention_policy_sha256": authority.retention_policy_sha256,
            "retention_until": authority.retention_until,
            "writer_scope": authority.writer_scope,
        }
    )


def _digest(value: object, field_name: str) -> str:
    if (
        not isinstance(value, str)
        or not value.startswith(_DIGEST_PREFIX)
        or len(value) != 71
        or any(character not in "0123456789abcdef" for character in value[7:])
    ):
        raise ValueError(f"{field_name} must be a canonical lowercase sha256 digest")
    return value


def _text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be non-empty text")
    return value


__all__ = [
    "SemanticRefreshSealPolicyAuthority",
    "SemanticRefreshSealPolicyAuthorityPort",
    "SemanticRefreshSealCodecAuthorityPort",
    "SemanticRefreshSealPolicySubject",
    "semantic_refresh_artifact_authority_sha256",
]
