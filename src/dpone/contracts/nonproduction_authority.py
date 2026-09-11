"""Externally pinned policy and comparison subjects for synthetic authority.

These pure contracts do not verify signatures or enroll physical participants.
The trusted runtime must reopen original bytes, verify them with its existing
cryptographic capability and supply independent policy/revocation/clock inputs.
Neither a signature-subject DTO nor a caller's PASS status grants permission.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

from dpone.contracts.nonproduction_scope import (
    PURPOSE,
    TRUST_TIER,
    NonproductionAuthorityError,
    NonproductionParticipant,
    NonproductionScope,
    canonical_document,
    digest,
    document_sha256,
    exact_fields,
    ordered,
    parse_document,
    repository,
    require_participants,
    sequence,
    text,
    uuid_text,
)
from dpone.contracts.runtime_artifact_attestation import MAX_ATTESTATION_BUNDLE_BYTES
from dpone.contracts.strict_json import strict_json_object

POLICY_SCHEMA = "dpone.nonproduction-authority-policy.v1"
GRANT_SCHEMA = "dpone.nonproduction-authority.v1"
_CEILINGS = {
    "max_validity_seconds": 86_400,
    "max_workloads": 64,
    "max_attempts": 128,
    "max_source_rows": 100_000,
    "max_source_bytes": 1_073_741_824,
    "max_attempt_seconds": 3600,
}


def require_signature_bundle(bundle: bytes) -> None:
    """Bound raw signature evidence before trust reads or verifier invocation."""
    if type(bundle) is not bytes or not 1 <= len(bundle) <= MAX_ATTESTATION_BUNDLE_BYTES:
        raise NonproductionAuthorityError("signature_bundle_budget") from None


def utc_timestamp(value: str) -> datetime:
    """Require canonical RFC3339 UTC seconds, then use Python's datetime parser."""
    if type(value) is not str or re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", value) is None:
        raise NonproductionAuthorityError("timestamp")
    try:
        return datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError:
        raise NonproductionAuthorityError("timestamp") from None


def require_validity(not_before: str, expires_at: str, maximum_seconds: int, *, now: datetime | None = None) -> None:
    """Validity is half-open: the exact expiry blocks all new issuance."""
    start, end = utc_timestamp(not_before), utc_timestamp(expires_at)
    if not 0 < (end - start).total_seconds() <= min(maximum_seconds, _CEILINGS["max_validity_seconds"]):
        raise NonproductionAuthorityError("validity")
    if now is not None:
        if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
            raise NonproductionAuthorityError("clock")
        if not start <= now.astimezone(timezone.utc) < end:  # noqa: UP017
            raise NonproductionAuthorityError("expired_or_not_yet_valid")


def require_epoch(value: int) -> None:
    if type(value) is not int or not 0 <= value <= 9_223_372_036_854_775_807:
        raise NonproductionAuthorityError("revocation_epoch")


@dataclass(frozen=True, slots=True)
class NonproductionLimits:
    """Signed ceilings; actual counters require atomic protected runtime enforcement."""

    max_validity_seconds: int
    max_workloads: int
    max_attempts: int
    max_source_rows: int
    max_source_bytes: int
    max_attempt_seconds: int

    def __post_init__(self) -> None:
        for key, value in asdict(self).items():
            if type(value) is not int or not 1 <= value <= _CEILINGS[key]:
                raise NonproductionAuthorityError("limits")

    @classmethod
    def from_dict(cls, value: object) -> NonproductionLimits:
        return cls(**exact_fields(value, set(_CEILINGS)))

    def require_usage(
        self, *, workloads: int, attempts: int, source_rows: int, source_bytes: int, attempt_seconds: int
    ) -> None:
        """Check a complete observed counter snapshot, without claiming atomicity.

        Source bytes are cumulative source bytes, not HTTP payload bytes. Runtime
        must reserve limits before reads and repeat checks while streaming.
        """
        self.__post_init__()
        observed = {
            "max_workloads": workloads,
            "max_attempts": attempts,
            "max_source_rows": source_rows,
            "max_source_bytes": source_bytes,
            "max_attempt_seconds": attempt_seconds,
        }
        for name, value in observed.items():
            if type(value) is not int or not 0 <= value <= getattr(self, name):
                raise NonproductionAuthorityError("budget_exceeded")


def effective_limits(
    policy: NonproductionLimits, grant: NonproductionLimits, workload: NonproductionLimits
) -> NonproductionLimits:
    """The smallest policy/grant/workload ceiling wins, with no implicit defaults."""
    for item in (policy, grant, workload):
        if type(item) is not NonproductionLimits:
            raise NonproductionAuthorityError("limits")
        item.__post_init__()
    return NonproductionLimits(
        **{key: min(getattr(item, key) for item in (policy, grant, workload)) for key in _CEILINGS}
    )


@dataclass(frozen=True, slots=True)
class NonproductionSigner:
    """Exact expectations pinned by external policy, not embedded trusted roots."""

    backend: str
    issuer: str
    identity: str
    trust_root_sha256: str

    def __post_init__(self) -> None:
        if text(self.backend) not in {"cosign_public_key_v1", "github_artifact_attestation_v1"}:
            raise NonproductionAuthorityError("signer_backend")
        text(self.issuer)
        text(self.identity)
        digest(self.trust_root_sha256)

    @classmethod
    def from_dict(cls, value: object) -> NonproductionSigner:
        return cls(**exact_fields(value, {"backend", "issuer", "identity", "trust_root_sha256"}))


@dataclass(frozen=True, slots=True)
class NonproductionSignatureSubject:
    """Documentary result from a trusted verifier capability, never permission.

    Constructing or matching this object proves no signature. Runtime must obtain
    it from actual original-byte verification using independently trusted roots;
    it must never deserialize a caller's verification report into authority.
    """

    document_schema: str
    phase: str
    document_sha256: str
    policy_sha256: str
    signer: NonproductionSigner

    def __post_init__(self) -> None:
        if self.document_schema != GRANT_SCHEMA or text(self.phase) not in {"qualification", "execution"}:
            raise NonproductionAuthorityError("signature_subject")
        digest(self.document_sha256)
        digest(self.policy_sha256)
        if type(self.signer) is not NonproductionSigner:
            raise NonproductionAuthorityError("signer")
        self.signer.__post_init__()


@dataclass(frozen=True, slots=True)
class NonproductionAuthorityPolicy:
    """One externally pinned synthetic environment and complete participant scope."""

    signer: NonproductionSigner
    environment_id: str
    enrollment_sha256: str
    participants: tuple[NonproductionParticipant, ...]
    source_repository: str
    not_before: str
    expires_at: str
    revocation_epoch: int
    revoked_grant_ids: tuple[str, ...]
    limits: NonproductionLimits
    schema: str = POLICY_SCHEMA
    purpose: str = PURPOSE
    trust_tier: str = TRUST_TIER

    def __post_init__(self) -> None:
        if (self.schema, self.purpose, self.trust_tier) != (POLICY_SCHEMA, PURPOSE, TRUST_TIER):
            raise NonproductionAuthorityError("policy_family")
        if type(self.signer) is not NonproductionSigner or type(self.limits) is not NonproductionLimits:
            raise NonproductionAuthorityError("policy_shape")
        self.signer.__post_init__()
        self.limits.__post_init__()
        uuid_text(self.environment_id)
        digest(self.enrollment_sha256)
        require_participants(self.participants)
        repository(self.source_repository)
        require_validity(self.not_before, self.expires_at, self.limits.max_validity_seconds)
        require_epoch(self.revocation_epoch)
        if type(self.revoked_grant_ids) is not tuple:
            raise NonproductionAuthorityError("revocations")
        for value in self.revoked_grant_ids:
            uuid_text(value, version4=True)
        ordered(self.revoked_grant_ids, maximum=8192, allow_empty=True)

    @classmethod
    def from_bytes(cls, raw: bytes, *, expected_sha256: str) -> NonproductionAuthorityPolicy:
        """Parse only the exact policy bytes selected outside release/grant inputs."""
        digest(expected_sha256)
        body = exact_fields(
            parse_document(raw, POLICY_SCHEMA),
            {
                "schema",
                "purpose",
                "trust_tier",
                "signer",
                "environment_id",
                "enrollment_sha256",
                "participants",
                "source_repository",
                "not_before",
                "expires_at",
                "revocation_epoch",
                "revoked_grant_ids",
                "limits",
            },
        )
        if document_sha256(body) != expected_sha256:
            raise NonproductionAuthorityError("policy_pin")
        return cls(
            **dict(
                body,
                signer=NonproductionSigner.from_dict(body["signer"]),
                limits=NonproductionLimits.from_dict(body["limits"]),
                revoked_grant_ids=tuple(sequence(body["revoked_grant_ids"], maximum=8192, allow_empty=True)),
                participants=tuple(
                    NonproductionParticipant.from_dict(row) for row in sequence(body["participants"], maximum=256)
                ),
            )
        )

    def to_dict(self) -> dict[str, Any]:
        self.__post_init__()
        return strict_json_object(canonical_document(asdict(self)))

    def to_bytes(self) -> bytes:
        return canonical_document(self.to_dict())

    @property
    def policy_sha256(self) -> str:
        return document_sha256(self.to_dict())

    def require_scope(self, scope: NonproductionScope) -> None:
        """Compare full signed scope with the external enrollment/policy snapshot."""
        self.__post_init__()
        if type(scope) is not NonproductionScope:
            raise NonproductionAuthorityError("scope")
        scope.__post_init__()
        if (
            scope.policy_sha256,
            scope.environment_id,
            scope.enrollment_sha256,
            scope.participants,
            scope.source_repository,
        ) != (
            self.policy_sha256,
            self.environment_id,
            self.enrollment_sha256,
            self.participants,
            self.source_repository,
        ):
            raise NonproductionAuthorityError("policy_scope")

    def require_current(self, *, now: datetime, current_revocation_epoch: int) -> None:
        """Compare an independent clock and revocation observation; never fetch them."""
        self.__post_init__()
        if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
            raise NonproductionAuthorityError("clock")
        require_epoch(current_revocation_epoch)
        require_validity(self.not_before, self.expires_at, self.limits.max_validity_seconds, now=now)
        if current_revocation_epoch != self.revocation_epoch:
            raise NonproductionAuthorityError("revoked")
