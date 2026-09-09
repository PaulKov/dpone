"""Time-bounded trusted runtime-assurance authority for semantic refresh V2."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import ClassVar

from dpone.contracts.semantic_refresh_document import (
    SemanticRefreshContractError,
    SemanticRefreshDocumentCodec,
    require_closed_mapping,
    require_digest,
    require_enum,
    require_positive_int,
    require_text,
    semantic_refresh_sha256,
    validate_digest,
    validate_schema,
)
from dpone.contracts.semantic_refresh_evidence_common import (
    parse_utc_timestamp,
    require_utc_interval,
    require_utc_timestamp,
)
from dpone.contracts.semantic_refresh_runtime_assurance_subject import (
    RuntimeAssuranceKind,
    RuntimeAssuranceSubjectType,
    SemanticRefreshRuntimeAssuranceSubject,
)

RUNTIME_ASSURANCE_RECEIPT_SCHEMA = "dpone.semantic-refresh-runtime-assurance-receipt.v1"
_DIGEST_FIELD = "runtime_assurance_receipt_sha256"
_REQUIRED = frozenset(
    {
        "schema",
        _DIGEST_FIELD,
        "status",
        "subject",
        "producer_version",
        "transformation_version",
        "acl_policy_version",
        "runtime_assurance_policy_sha256",
        "approver_authority",
        "approver_attestation_sha256",
        "approver_signature_sha256",
    }
)
_TIMING = frozenset({"effective_from", "expires_at"})
_ACTIVE = _TIMING | {"evidence_sha256"}
_WRITER_DIGESTS = frozenset(
    {
        "engine_acl_proof_sha256",
        "platform_allowlist_sha256",
        "external_job_inventory_sha256",
        "organizational_control_sha256",
    }
)
_WRITER_STATUSES = frozenset(
    {
        "engine_acl_proof_status",
        "platform_allowlist_status",
        "external_job_inventory_status",
        "organizational_control_status",
    }
)
_REVOCATION = frozenset({"revoked_receipt_sha256", "revoked_at", "revocation_reason"})
_OPTIONAL = _ACTIVE | _WRITER_DIGESTS | _WRITER_STATUSES | _REVOCATION | {"ddl_epoch"}


class RuntimeAssuranceStatus(str, Enum):  # noqa: UP042
    """A receipt is either currently certifiable or an immutable revocation."""

    CERTIFIED = "CERTIFIED"
    REVOKED = "REVOKED"


@dataclass(frozen=True, slots=True)
class SemanticRefreshRuntimeAssuranceReceipt(SemanticRefreshDocumentCodec):
    """Signed current assurance or signed revocation for one exact subject."""

    subject: SemanticRefreshRuntimeAssuranceSubject
    status: RuntimeAssuranceStatus
    producer_version: str
    transformation_version: str
    acl_policy_version: str
    runtime_assurance_policy_sha256: str
    approver_authority: str
    approver_attestation_sha256: str
    approver_signature_sha256: str
    runtime_assurance_receipt_sha256: str
    effective_from: str | None = None
    expires_at: str | None = None
    evidence_sha256: str | None = None
    engine_acl_proof_sha256: str | None = None
    engine_acl_proof_status: str | None = None
    platform_allowlist_sha256: str | None = None
    platform_allowlist_status: str | None = None
    external_job_inventory_sha256: str | None = None
    external_job_inventory_status: str | None = None
    organizational_control_sha256: str | None = None
    organizational_control_status: str | None = None
    revoked_receipt_sha256: str | None = None
    revoked_at: str | None = None
    revocation_reason: str | None = None
    ddl_epoch: int | None = None
    schema: str = RUNTIME_ASSURANCE_RECEIPT_SCHEMA

    schema_id: ClassVar[str] = RUNTIME_ASSURANCE_RECEIPT_SCHEMA
    digest_field: ClassVar[str] = _DIGEST_FIELD

    def __post_init__(self) -> None:
        validate_schema(self.schema, self.schema_id)
        if not isinstance(self.subject, SemanticRefreshRuntimeAssuranceSubject):
            raise SemanticRefreshContractError("subject must be typed runtime assurance coordinates")
        if not isinstance(self.status, RuntimeAssuranceStatus):
            raise SemanticRefreshContractError("status is unsupported")
        for field in ("producer_version", "transformation_version", "acl_policy_version", "approver_authority"):
            require_text(getattr(self, field), field)
        for field in (
            "runtime_assurance_policy_sha256",
            "approver_attestation_sha256",
            "approver_signature_sha256",
        ):
            require_digest(getattr(self, field), field)
        self._validate_branch()
        validate_digest(self._unsigned(), self.runtime_assurance_receipt_sha256, self.digest_field)

    def _validate_branch(self) -> None:
        active = tuple(getattr(self, field) for field in _ACTIVE)
        writer = tuple(getattr(self, field) for field in _WRITER_DIGESTS | _WRITER_STATUSES)
        revoked = tuple(getattr(self, field) for field in _REVOCATION)
        if self.status is RuntimeAssuranceStatus.REVOKED:
            if (
                not all(value is not None for value in revoked)
                or any(value is not None for value in active + writer)
                or self.ddl_epoch is not None
            ):
                raise SemanticRefreshContractError("runtime assurance revocation branch is incomplete or mixed")
            require_digest(self.revoked_receipt_sha256, "revoked_receipt_sha256")
            require_utc_timestamp(self.revoked_at, "revoked_at")
            require_text(self.revocation_reason, "revocation_reason")
            return
        if not all(value is not None for value in active) or any(value is not None for value in revoked):
            raise SemanticRefreshContractError(
                "certified runtime assurance requires an exact validity window and evidence"
            )
        require_utc_interval(self.effective_from, self.expires_at, "effective_from", "expires_at")
        require_digest(self.evidence_sha256, "evidence_sha256")
        if self.subject.assurance_kind is RuntimeAssuranceKind.WRITER_EXCLUSIVITY:
            if not all(value is not None for value in writer):
                raise SemanticRefreshContractError("writer exclusivity assurance requires every positive control proof")
            for field in _WRITER_DIGESTS:
                require_digest(getattr(self, field), field)
            expected = {
                "engine_acl_proof_status": "PASS",
                "platform_allowlist_status": "PASS",
                "external_job_inventory_status": "PASS",
                "organizational_control_status": "CERTIFIED",
            }
            if any(getattr(self, field) != value for field, value in expected.items()):
                raise SemanticRefreshContractError(
                    "writer exclusivity controls must be PASS and organizationally CERTIFIED"
                )
        elif any(value is not None for value in writer):
            raise SemanticRefreshContractError("writer exclusivity controls are forbidden for another assurance kind")
        if self.subject.assurance_kind is RuntimeAssuranceKind.DDL_FREEZE:
            require_positive_int(self.ddl_epoch, "ddl_epoch")
        elif self.ddl_epoch is not None:
            raise SemanticRefreshContractError("ddl_epoch is forbidden for another assurance kind")

    @classmethod
    def build(
        cls, *, subject: SemanticRefreshRuntimeAssuranceSubject, **values: object
    ) -> SemanticRefreshRuntimeAssuranceReceipt:
        """Build one certified active receipt; writer controls remain mandatory as a complete set."""

        writer_values = {
            "engine_acl_proof_sha256": values.get("engine_acl_proof_sha256"),
            "platform_allowlist_sha256": values.get("platform_allowlist_sha256"),
            "external_job_inventory_sha256": values.get("external_job_inventory_sha256"),
            "organizational_control_sha256": values.get("organizational_control_sha256"),
        }
        unsigned: dict[str, object] = {
            "acl_policy_version": values["acl_policy_version"],
            "approver_attestation_sha256": values["approver_attestation_sha256"],
            "approver_authority": values["approver_authority"],
            "approver_signature_sha256": values["approver_signature_sha256"],
            "effective_from": values["effective_from"],
            "evidence_sha256": values["evidence_sha256"],
            "expires_at": values["expires_at"],
            "producer_version": values["producer_version"],
            "runtime_assurance_policy_sha256": values["runtime_assurance_policy_sha256"],
            "schema": cls.schema_id,
            "status": RuntimeAssuranceStatus.CERTIFIED.value,
            "subject": subject.to_dict(),
            "transformation_version": values["transformation_version"],
        }
        if any(value is not None for value in writer_values.values()):
            unsigned.update(writer_values)
            unsigned.update(
                {
                    "engine_acl_proof_status": "PASS",
                    "external_job_inventory_status": "PASS",
                    "organizational_control_status": "CERTIFIED",
                    "platform_allowlist_status": "PASS",
                }
            )
        if subject.assurance_kind is RuntimeAssuranceKind.DDL_FREEZE:
            unsigned["ddl_epoch"] = values["ddl_epoch"]
        return cls.from_mapping({**unsigned, cls.digest_field: semantic_refresh_sha256(unsigned)})

    @classmethod
    def build_revocation(
        cls,
        *,
        subject: SemanticRefreshRuntimeAssuranceSubject,
        revoked_receipt_sha256: str,
        revoked_at: str,
        revocation_reason: str,
        producer_version: str,
        transformation_version: str,
        acl_policy_version: str,
        runtime_assurance_policy_sha256: str,
        approver_authority: str,
        approver_attestation_sha256: str,
        approver_signature_sha256: str,
    ) -> SemanticRefreshRuntimeAssuranceReceipt:
        """Build a signed immutable revocation of one prior exact receipt."""

        unsigned: dict[str, object] = {
            "acl_policy_version": acl_policy_version,
            "approver_attestation_sha256": approver_attestation_sha256,
            "approver_authority": approver_authority,
            "approver_signature_sha256": approver_signature_sha256,
            "producer_version": producer_version,
            "revocation_reason": revocation_reason,
            "revoked_at": revoked_at,
            "revoked_receipt_sha256": revoked_receipt_sha256,
            "runtime_assurance_policy_sha256": runtime_assurance_policy_sha256,
            "schema": cls.schema_id,
            "status": RuntimeAssuranceStatus.REVOKED.value,
            "subject": subject.to_dict(),
            "transformation_version": transformation_version,
        }
        return cls.from_mapping({**unsigned, cls.digest_field: semantic_refresh_sha256(unsigned)})

    @classmethod
    def from_mapping(cls, value: object) -> SemanticRefreshRuntimeAssuranceReceipt:
        """Parse one closed certified or revoked receipt."""

        raw = require_closed_mapping(value, "runtime_assurance_receipt", required=_REQUIRED, optional=_OPTIONAL)
        if any(field in raw and raw[field] is None for field in _OPTIONAL):
            raise SemanticRefreshContractError("optional runtime assurance fields cannot be null")
        parsed: dict[str, object] = {
            "subject": SemanticRefreshRuntimeAssuranceSubject.from_mapping(raw.get("subject")),
            "status": require_enum(raw.get("status"), "status", RuntimeAssuranceStatus),
            "producer_version": require_text(raw.get("producer_version"), "producer_version"),
            "transformation_version": require_text(raw.get("transformation_version"), "transformation_version"),
            "acl_policy_version": require_text(raw.get("acl_policy_version"), "acl_policy_version"),
            "runtime_assurance_policy_sha256": require_digest(
                raw.get("runtime_assurance_policy_sha256"), "runtime_assurance_policy_sha256"
            ),
            "approver_authority": require_text(raw.get("approver_authority"), "approver_authority"),
            "approver_attestation_sha256": require_digest(
                raw.get("approver_attestation_sha256"), "approver_attestation_sha256"
            ),
            "approver_signature_sha256": require_digest(
                raw.get("approver_signature_sha256"), "approver_signature_sha256"
            ),
            _DIGEST_FIELD: require_digest(raw.get(_DIGEST_FIELD), _DIGEST_FIELD),
            "schema": validate_schema(raw.get("schema"), cls.schema_id),
        }
        for field in _TIMING | {"revoked_at"}:
            parsed[field] = require_utc_timestamp(raw[field], field) if field in raw else None
        for field in _WRITER_DIGESTS | {"evidence_sha256", "revoked_receipt_sha256"}:
            parsed[field] = require_digest(raw[field], field) if field in raw else None
        for field in _WRITER_STATUSES | {"revocation_reason"}:
            parsed[field] = require_text(raw[field], field) if field in raw else None
        parsed["ddl_epoch"] = require_positive_int(raw["ddl_epoch"], "ddl_epoch") if "ddl_epoch" in raw else None
        return cls(**parsed)  # type: ignore[arg-type]

    def authorizes(
        self,
        as_of: datetime,
        *,
        trusted_digest: str | None,
        expected_subject: SemanticRefreshRuntimeAssuranceSubject,
    ) -> bool:
        """Authorize a current exact subject after the resolver verifies signature/revocation and supplies trust."""

        if as_of.tzinfo is None or as_of.utcoffset() is None:
            raise SemanticRefreshContractError("as_of must be timezone-aware")
        if not isinstance(expected_subject, SemanticRefreshRuntimeAssuranceSubject):
            raise SemanticRefreshContractError("expected_subject must be typed runtime assurance coordinates")
        return bool(
            self.status is RuntimeAssuranceStatus.CERTIFIED
            and trusted_digest == self.runtime_assurance_receipt_sha256
            and expected_subject == self.subject
            and self.effective_from is not None
            and self.expires_at is not None
            and parse_utc_timestamp(self.effective_from, "effective_from") <= as_of
            and as_of < parse_utc_timestamp(self.expires_at, "expires_at")
        )

    def _unsigned(self) -> dict[str, object]:
        result: dict[str, object] = {
            "acl_policy_version": self.acl_policy_version,
            "approver_attestation_sha256": self.approver_attestation_sha256,
            "approver_authority": self.approver_authority,
            "approver_signature_sha256": self.approver_signature_sha256,
            "producer_version": self.producer_version,
            "runtime_assurance_policy_sha256": self.runtime_assurance_policy_sha256,
            "schema": self.schema,
            "status": self.status.value,
            "subject": self.subject.to_dict(),
            "transformation_version": self.transformation_version,
        }
        for field in sorted(_OPTIONAL):
            value = getattr(self, field)
            if value is not None:
                result[field] = value
        return result

    def to_dict(self) -> dict[str, object]:
        """Return the exact branch including its canonical receipt digest."""

        return {**self._unsigned(), self.digest_field: self.runtime_assurance_receipt_sha256}


__all__ = [
    "RUNTIME_ASSURANCE_RECEIPT_SCHEMA",
    "RuntimeAssuranceKind",
    "RuntimeAssuranceStatus",
    "RuntimeAssuranceSubjectType",
    "SemanticRefreshRuntimeAssuranceReceipt",
    "SemanticRefreshRuntimeAssuranceSubject",
]
