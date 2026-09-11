"""Immutable original grant documents and complete campaign membership policy.

These values describe stored data. Neither decoding a signature subject nor
constructing a registration authenticates a signature or grants source access.
The private coordinator must verify originals and independently reconstruct
scope before invoking protected storage. Existing production formats are intact.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
from dataclasses import asdict, dataclass, field
from datetime import datetime

from dpone.contracts.nonproduction_activation import NonproductionCompositionActivationRequest
from dpone.contracts.nonproduction_authority import (
    NonproductionAuthorityPolicy,
    NonproductionSignatureSubject,
    NonproductionSigner,
    require_validity,
    utc_timestamp,
)
from dpone.contracts.nonproduction_document import (
    MAX_DOCUMENT_BYTES,
    NonproductionAuthorityError,
    canonical_document,
    digest,
    exact_fields,
    ordered,
    text,
)
from dpone.contracts.nonproduction_grants import (
    NonproductionExecutionGrant,
    NonproductionQualificationGrant,
    parse_nonproduction_grant,
    validate_grant_subject,
)
from dpone.contracts.runtime_artifact_attestation import MAX_ATTESTATION_BUNDLE_BYTES
from dpone.contracts.strict_json import strict_json_object


def original_sha256(raw: bytes, *, maximum: int = MAX_DOCUMENT_BYTES) -> str:
    """Bound and hash exact original bytes; never normalize slash or Unicode text."""
    if type(raw) is not bytes or not 1 <= len(raw) <= maximum:
        raise NonproductionAuthorityError("registration_document")
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def signature_subject_bytes(subject: NonproductionSignatureSubject) -> bytes:
    """Canonical documentary comparison subject, not a signature-verification API."""
    if type(subject) is not NonproductionSignatureSubject:
        raise NonproductionAuthorityError("registration_subject")
    subject.__post_init__()
    return canonical_document(asdict(subject))


def _subject(raw: bytes) -> NonproductionSignatureSubject:
    original_sha256(raw)
    body = exact_fields(
        strict_json_object(raw),
        {
            "document_schema",
            "phase",
            "document_sha256",
            "policy_sha256",
            "signer",
        },
    )
    signer = NonproductionSigner(**exact_fields(body["signer"], {"backend", "issuer", "identity", "trust_root_sha256"}))
    value = NonproductionSignatureSubject(**dict(body, signer=signer))
    if signature_subject_bytes(value) != raw:
        raise NonproductionAuthorityError("registration_subject")
    return value


@dataclass(frozen=True, slots=True)
class NonproductionRegistrationOriginals:
    """Original bounded public documents; execution alone carries an NP request.

    The public signature bundle remains opaque, byte-for-byte, within the
    existing 8 MiB bound. It is not canonicalized or cryptographically verified.
    """

    grant_bytes: bytes = field(repr=False)
    bundle_bytes: bytes = field(repr=False)
    signature_subject_bytes: bytes = field(repr=False)
    request_bytes: bytes | None = field(repr=False)

    def __post_init__(self) -> None:
        try:
            original_sha256(self.bundle_bytes, maximum=MAX_ATTESTATION_BUNDLE_BYTES)
            grant, subject = self.grant, self.subject
            if (subject.document_sha256, subject.phase, subject.policy_sha256) != (
                grant.grant_sha256,
                grant.phase,
                grant.scope.policy_sha256,
            ):
                raise NonproductionAuthorityError("registration_subject")
            if type(grant) is NonproductionExecutionGrant:
                if not isinstance(self.request_bytes, bytes):
                    raise NonproductionAuthorityError("registration_request")
                original_sha256(self.request_bytes)
                request = NonproductionCompositionActivationRequest.from_bytes(self.request_bytes)
                request.require_execution_grant(grant)
            elif self.request_bytes is not None:
                raise NonproductionAuthorityError("registration_phase")
        except Exception:
            raise NonproductionAuthorityError("registration_originals") from None

    @property
    def grant(self) -> NonproductionExecutionGrant | NonproductionQualificationGrant:
        return parse_nonproduction_grant(self.grant_bytes)

    @property
    def subject(self) -> NonproductionSignatureSubject:
        return _subject(self.signature_subject_bytes)

    @property
    def grant_sha256(self) -> str:
        return original_sha256(self.grant_bytes)

    @property
    def bundle_sha256(self) -> str:
        return original_sha256(self.bundle_bytes, maximum=MAX_ATTESTATION_BUNDLE_BYTES)

    @property
    def request_sha256(self) -> str | None:
        return None if self.request_bytes is None else original_sha256(self.request_bytes)

    def require_policy(
        self, policy_bytes: bytes, policy_sha256: str, revocation_epoch: int, now: datetime
    ) -> NonproductionAuthorityPolicy:
        """Compare original subjects/time with independently supplied policy bytes.

        This is structural policy validation, not authentication or enrollment.
        Protected current and historical readers supply their own original policy
        revision; a caller-selected policy cannot establish storage authority.
        """
        policy = NonproductionAuthorityPolicy.from_bytes(policy_bytes, expected_sha256=policy_sha256)
        validate_grant_subject(
            self.grant,
            policy=policy,
            expected_policy_sha256=policy_sha256,
            expected_scope=self.grant.scope,
            signature_subject=self.subject,
            now=now,
            current_revocation_epoch=revocation_epoch,
        )
        return policy


@dataclass(frozen=True, slots=True)
class NonproductionGrantRegistration:
    """Historical receipt at one original trust revision and UTC second.

    It describes neither transaction commit nor fresh admission. The transaction
    owner must commit once and independently reread complete membership. Failure
    or lost ACK must never turn this record into a transferable executor permit.
    """

    originals: NonproductionRegistrationOriginals
    trust_revision: int
    registered_at: str

    def __post_init__(self) -> None:
        if type(self.originals) is not NonproductionRegistrationOriginals:
            raise NonproductionAuthorityError("registration_originals")
        self.originals.__post_init__()
        if type(self.trust_revision) is not int or not 1 <= self.trust_revision <= 2**63 - 1:
            raise NonproductionAuthorityError("registration_revision")
        grant = self.originals.grant
        require_validity(
            grant.not_before, grant.expires_at, grant.limits.max_validity_seconds, now=utc_timestamp(self.registered_at)
        )

    def require_historical_boundary(self, *, observed_at: datetime, maximum_revision: int) -> datetime:
        """Compare an already constructed record with independently observed ceilings.

        The caller must still reopen historical policy and retain its transaction.
        This neither authenticates current/historical trust nor authorizes execution.
        """
        instant = utc_timestamp(self.registered_at)
        if instant > observed_at or self.trust_revision > maximum_revision:
            raise NonproductionAuthorityError("registration_history")
        return instant


def workload_document(workload_id: str) -> bytes:
    """Stable complete-campaign member; pack or grant replacement cannot reset it."""
    return canonical_document({"workload_id": text(workload_id)})


def require_execution_memberships(
    grant: NonproductionExecutionGrant,
    policy: NonproductionAuthorityPolicy,
    existing: tuple[str, ...],
) -> tuple[str, ...]:
    """Check the complete retained union against every current membership ceiling.

    A newly signed larger current ceiling may add capacity, preserving all old
    members. This function changes no attempt bound, counter or qualification.
    """
    if type(grant) is not NonproductionExecutionGrant or type(policy) is not NonproductionAuthorityPolicy:
        raise NonproductionAuthorityError("registration_phase")
    grant.__post_init__()
    policy.require_scope(grant.scope)
    ordered(existing, maximum=64, allow_empty=True)
    for value in existing:
        text(value)
    result = tuple(sorted(set(existing).union(row.workload_id for row in grant.workloads)))
    ceiling = min(
        policy.limits.max_workloads, grant.limits.max_workloads, *(row.limits.max_workloads for row in grant.workloads)
    )
    if len(result) > ceiling:
        raise NonproductionAuthorityError("membership_budget")
    return result


def require_membership_history(
    members: tuple[tuple[str, bytes, str], ...], history: Iterable[NonproductionGrantRegistration]
) -> frozenset[str]:
    """Audit all original registrations with at most 64 membership/origin records.

    History must be complete and ordered by consumption key. The protected SQL
    reader supplies that stream; arbitrary caller rows are not durable evidence.
    Originals are released between iterations. Time is O(history), not bounded
    by the number of distinct members, and no historical grant is skipped.
    """
    if type(members) is not tuple or len(members) > 64:
        raise NonproductionAuthorityError("registration_membership_overflow")
    observed: dict[str, bytes] = {}
    origins: dict[str, set[str]] = {}
    for key, document, first in members:
        digest(first)
        if key in observed or key != original_sha256(document):
            raise NonproductionAuthorityError("registration_membership")
        observed[key] = document
        origins.setdefault(first, set()).add(key)
    previous = ""
    seen: set[str] = set()
    names: set[str] = set()
    for record in history:
        grant = record.originals.grant
        key = grant.consumption_subject_sha256
        if key <= previous:
            raise NonproductionAuthorityError("registration_pool_order")
        previous = key
        witnessed = set()
        if type(grant) is NonproductionExecutionGrant:
            for workload in grant.workloads:
                document = workload_document(workload.workload_id)
                workload_key = original_sha256(document)
                if observed.get(workload_key) != document:
                    raise NonproductionAuthorityError("registration_membership")
                witnessed.add(workload_key)
                names.add(workload.workload_id)
        if not origins.pop(key, set()).issubset(witnessed):
            raise NonproductionAuthorityError("registration_membership")
        seen.update(witnessed)
    if seen != set(observed) or origins:
        raise NonproductionAuthorityError("registration_membership")
    return frozenset(names)
