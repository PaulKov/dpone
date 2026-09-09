"""Exact environment capability and live-certification receipts."""

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
from dpone.contracts.semantic_refresh_route_coordinates import SemanticRefreshRouteCapabilityCoordinates

ROUTE_LIVE_CERTIFICATION_RECEIPT_SCHEMA = "dpone.semantic-refresh-route-live-certification-receipt.v1"
_DIGEST_FIELD = "route_certification_receipt_sha256"
_FIELDS = frozenset(
    {
        "schema",
        _DIGEST_FIELD,
        "coordinates",
        "status",
        "certification_policy_sha256",
        "issuer_authority",
        "issuer_attestation_sha256",
        "issuer_signature_sha256",
    }
)
_OPTIONAL_FIELDS = frozenset(
    {
        "tested_at",
        "effective_from",
        "expires_at",
        "live_environment_sha256",
        "live_evidence_sha256",
        "failure_matrix_sha256",
        "revoked_receipt_sha256",
        "revoked_at",
        "revocation_reason",
    }
)


class LiveCertificationStatus(str, Enum):  # noqa: UP042
    """Closed outcome vocabulary; unavailable live execution is never PASS."""

    PASS = "PASS"
    FAIL = "FAIL"
    UNVERIFIED = "UNVERIFIED"


@dataclass(frozen=True, slots=True)
class SemanticRefreshRouteLiveCertificationReceipt(SemanticRefreshDocumentCodec):
    """Immutable live result or revocation for one exact capability coordinate."""

    coordinates: SemanticRefreshRouteCapabilityCoordinates
    status: LiveCertificationStatus
    certification_policy_sha256: str
    issuer_authority: str
    issuer_attestation_sha256: str
    issuer_signature_sha256: str
    route_certification_receipt_sha256: str
    tested_at: str | None = None
    effective_from: str | None = None
    expires_at: str | None = None
    live_environment_sha256: str | None = None
    live_evidence_sha256: str | None = None
    failure_matrix_sha256: str | None = None
    revoked_receipt_sha256: str | None = None
    revoked_at: str | None = None
    revocation_reason: str | None = None
    schema: str = ROUTE_LIVE_CERTIFICATION_RECEIPT_SCHEMA

    schema_id: ClassVar[str] = ROUTE_LIVE_CERTIFICATION_RECEIPT_SCHEMA
    digest_field: ClassVar[str] = _DIGEST_FIELD

    def __post_init__(self) -> None:
        validate_schema(self.schema, self.schema_id)
        if not isinstance(self.coordinates, SemanticRefreshRouteCapabilityCoordinates):
            raise SemanticRefreshContractError("coordinates must be exact route capability coordinates")
        if not isinstance(self.status, LiveCertificationStatus):
            raise SemanticRefreshContractError("status is unsupported")
        require_digest(self.certification_policy_sha256, "certification_policy_sha256")
        require_text(self.issuer_authority, "issuer_authority")
        require_digest(self.issuer_attestation_sha256, "issuer_attestation_sha256")
        require_digest(self.issuer_signature_sha256, "issuer_signature_sha256")
        self._validate_branch()
        validate_digest(self._unsigned(), self.route_certification_receipt_sha256, self.digest_field)

    def _validate_branch(self) -> None:
        revocation = (self.revoked_receipt_sha256, self.revoked_at, self.revocation_reason)
        timing = (self.tested_at, self.effective_from, self.expires_at)
        live = (self.live_environment_sha256, self.live_evidence_sha256, self.failure_matrix_sha256)
        if any(value is not None for value in revocation):
            if not all(value is not None for value in revocation) or any(value is not None for value in timing + live):
                raise SemanticRefreshContractError("revocation coordinates are incomplete or mixed with live evidence")
            if self.status is not LiveCertificationStatus.UNVERIFIED:
                raise SemanticRefreshContractError("a revoked certification must be UNVERIFIED")
            require_digest(self.revoked_receipt_sha256, "revoked_receipt_sha256")
            require_utc_timestamp(self.revoked_at, "revoked_at")
            require_text(self.revocation_reason, "revocation_reason")
            return
        if not all(value is not None for value in timing):
            raise SemanticRefreshContractError("certification validity window is incomplete")
        require_utc_timestamp(self.tested_at, "tested_at")
        require_utc_interval(self.effective_from, self.expires_at, "effective_from", "expires_at")
        if parse_utc_timestamp(self.tested_at, "tested_at") > parse_utc_timestamp(
            self.effective_from, "effective_from"
        ):
            raise SemanticRefreshContractError("certification cannot become effective before live testing")
        if self.status is LiveCertificationStatus.UNVERIFIED:
            if any(value is not None for value in live):
                raise SemanticRefreshContractError("UNVERIFIED certification cannot contain live evidence")
            return
        if not all(value is not None for value in live):
            raise SemanticRefreshContractError("PASS/FAIL requires exact live evidence and failure matrix")
        for field in ("live_environment_sha256", "live_evidence_sha256", "failure_matrix_sha256"):
            require_digest(getattr(self, field), field)

    @classmethod
    def build(
        cls,
        *,
        coordinates: SemanticRefreshRouteCapabilityCoordinates,
        status: LiveCertificationStatus,
        tested_at: str,
        effective_from: str,
        expires_at: str,
        live_environment_sha256: str | None,
        live_evidence_sha256: str | None,
        failure_matrix_sha256: str | None,
        certification_policy_sha256: str,
        issuer_authority: str,
        issuer_attestation_sha256: str,
        issuer_signature_sha256: str,
    ) -> SemanticRefreshRouteLiveCertificationReceipt:
        """Build a timed live result. UNVERIFIED deliberately carries no live evidence."""

        if not isinstance(status, LiveCertificationStatus):
            raise SemanticRefreshContractError("status is unsupported")
        values: dict[str, object] = {
            "certification_policy_sha256": certification_policy_sha256,
            "coordinates": coordinates.to_dict(),
            "effective_from": effective_from,
            "expires_at": expires_at,
            "issuer_authority": issuer_authority,
            "issuer_attestation_sha256": issuer_attestation_sha256,
            "issuer_signature_sha256": issuer_signature_sha256,
            "schema": cls.schema_id,
            "status": status.value,
            "tested_at": tested_at,
        }
        optional = {
            "failure_matrix_sha256": failure_matrix_sha256,
            "live_environment_sha256": live_environment_sha256,
            "live_evidence_sha256": live_evidence_sha256,
        }
        values.update({key: value for key, value in optional.items() if value is not None})
        return cls(
            coordinates=coordinates,
            status=status,
            certification_policy_sha256=certification_policy_sha256,
            issuer_authority=issuer_authority,
            issuer_attestation_sha256=issuer_attestation_sha256,
            issuer_signature_sha256=issuer_signature_sha256,
            route_certification_receipt_sha256=semantic_refresh_sha256(values),
            tested_at=tested_at,
            effective_from=effective_from,
            expires_at=expires_at,
            live_environment_sha256=live_environment_sha256,
            live_evidence_sha256=live_evidence_sha256,
            failure_matrix_sha256=failure_matrix_sha256,
        )

    @classmethod
    def build_revocation(
        cls,
        *,
        coordinates: SemanticRefreshRouteCapabilityCoordinates,
        revoked_receipt_sha256: str,
        revoked_at: str,
        revocation_reason: str,
        certification_policy_sha256: str,
        issuer_authority: str,
        issuer_attestation_sha256: str,
        issuer_signature_sha256: str,
    ) -> SemanticRefreshRouteLiveCertificationReceipt:
        """Build an immutable fail-closed revocation of a prior receipt."""

        values = {
            "certification_policy_sha256": certification_policy_sha256,
            "coordinates": coordinates.to_dict(),
            "issuer_authority": issuer_authority,
            "issuer_attestation_sha256": issuer_attestation_sha256,
            "issuer_signature_sha256": issuer_signature_sha256,
            "revocation_reason": revocation_reason,
            "revoked_at": revoked_at,
            "revoked_receipt_sha256": revoked_receipt_sha256,
            "schema": cls.schema_id,
            "status": LiveCertificationStatus.UNVERIFIED.value,
        }
        return cls(
            coordinates=coordinates,
            status=LiveCertificationStatus.UNVERIFIED,
            certification_policy_sha256=certification_policy_sha256,
            issuer_authority=issuer_authority,
            issuer_attestation_sha256=issuer_attestation_sha256,
            issuer_signature_sha256=issuer_signature_sha256,
            route_certification_receipt_sha256=semantic_refresh_sha256(values),
            revoked_receipt_sha256=revoked_receipt_sha256,
            revoked_at=revoked_at,
            revocation_reason=revocation_reason,
        )

    @classmethod
    def from_mapping(cls, value: object) -> SemanticRefreshRouteLiveCertificationReceipt:
        """Parse one closed timed result or revocation branch."""

        raw = require_closed_mapping(
            value, "route_live_certification_receipt", required=_FIELDS, optional=_OPTIONAL_FIELDS
        )
        if any(field in raw and raw[field] is None for field in _OPTIONAL_FIELDS):
            raise SemanticRefreshContractError("optional certification fields cannot be null")
        optional = {field: raw.get(field) for field in _OPTIONAL_FIELDS}
        return cls(
            coordinates=SemanticRefreshRouteCapabilityCoordinates.from_mapping(raw.get("coordinates")),
            status=require_enum(raw.get("status"), "status", LiveCertificationStatus),
            certification_policy_sha256=require_digest(
                raw.get("certification_policy_sha256"), "certification_policy_sha256"
            ),
            issuer_authority=require_text(raw.get("issuer_authority"), "issuer_authority"),
            issuer_attestation_sha256=require_digest(raw.get("issuer_attestation_sha256"), "issuer_attestation_sha256"),
            issuer_signature_sha256=require_digest(raw.get("issuer_signature_sha256"), "issuer_signature_sha256"),
            route_certification_receipt_sha256=require_digest(raw.get(_DIGEST_FIELD), _DIGEST_FIELD),
            tested_at=_optional_timestamp(optional["tested_at"], "tested_at"),
            effective_from=_optional_timestamp(optional["effective_from"], "effective_from"),
            expires_at=_optional_timestamp(optional["expires_at"], "expires_at"),
            live_environment_sha256=_optional_digest(optional["live_environment_sha256"], "live_environment_sha256"),
            live_evidence_sha256=_optional_digest(optional["live_evidence_sha256"], "live_evidence_sha256"),
            failure_matrix_sha256=_optional_digest(optional["failure_matrix_sha256"], "failure_matrix_sha256"),
            revoked_receipt_sha256=_optional_digest(optional["revoked_receipt_sha256"], "revoked_receipt_sha256"),
            revoked_at=_optional_timestamp(optional["revoked_at"], "revoked_at"),
            revocation_reason=_optional_text(optional["revocation_reason"], "revocation_reason"),
            schema=validate_schema(raw.get("schema"), cls.schema_id),
        )

    def authorizes(self, as_of: datetime, *, trusted_receipt_sha256: str | None = None) -> bool:
        """Admit only a current PASS whose digest an external trust verifier accepted."""

        if as_of.tzinfo is None or as_of.utcoffset() is None:
            raise SemanticRefreshContractError("as_of must be timezone-aware")
        return bool(
            self.status is LiveCertificationStatus.PASS
            and trusted_receipt_sha256 == self.route_certification_receipt_sha256
            and self.revoked_receipt_sha256 is None
            and self.effective_from is not None
            and self.expires_at is not None
            and parse_utc_timestamp(self.effective_from, "effective_from") <= as_of
            and as_of < parse_utc_timestamp(self.expires_at, "expires_at")
        )

    def _unsigned(self) -> dict[str, object]:
        result: dict[str, object] = {
            "certification_policy_sha256": self.certification_policy_sha256,
            "coordinates": self.coordinates.to_dict(),
            "issuer_authority": self.issuer_authority,
            "issuer_attestation_sha256": self.issuer_attestation_sha256,
            "issuer_signature_sha256": self.issuer_signature_sha256,
            "schema": self.schema,
            "status": self.status.value,
        }
        for field in sorted(_OPTIONAL_FIELDS):
            value = getattr(self, field)
            if value is not None:
                result[field] = value
        return result

    def to_dict(self) -> dict[str, object]:
        """Return the exact branch mapping including its content digest."""

        return {**self._unsigned(), self.digest_field: self.route_certification_receipt_sha256}


def _optional_digest(value: object, field: str) -> str | None:
    return None if value is None else require_digest(value, field)


def _optional_timestamp(value: object, field: str) -> str | None:
    return None if value is None else require_utc_timestamp(value, field)


def _optional_text(value: object, field: str) -> str | None:
    return None if value is None else require_text(value, field)


__all__ = [
    "LiveCertificationStatus",
    "ROUTE_LIVE_CERTIFICATION_RECEIPT_SCHEMA",
    "SemanticRefreshRouteCapabilityCoordinates",
    "SemanticRefreshRouteLiveCertificationReceipt",
]
