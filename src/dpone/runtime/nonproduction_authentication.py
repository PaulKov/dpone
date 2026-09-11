"""Authenticate closed synthetic grants before protected admission can proceed.

The trusted application root injects policy/revocation, clock and signature
capabilities. Original signatures are verified on every call; no report or
previous return value substitutes for verification. This service does not
consume a grant, establish physical enrollment, reserve counters or issue a
credential. Those independent protected operations must follow authentication.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from typing import TypeVar

from dpone.contracts.nonproduction_authority import (
    NonproductionAuthorityPolicy,
    NonproductionSignatureSubject,
    require_validity,
)
from dpone.contracts.nonproduction_grants import (
    NonproductionExecutionGrant,
    NonproductionQualificationGrant,
    NonproductionWorkload,
    validate_grant_subject,
)
from dpone.contracts.nonproduction_scope import NonproductionAuthorityError, NonproductionScope
from dpone.contracts.runtime_artifact_attestation import MAX_ATTESTATION_BUNDLE_BYTES
from dpone.ports.nonproduction_authentication import (
    NonproductionGrantSignatureVerifier,
    NonproductionTrustProvider,
    NonproductionTrustSnapshot,
)

_Grant = TypeVar("_Grant", NonproductionQualificationGrant, NonproductionExecutionGrant)


class NonproductionGrantAuthenticator:
    """Verify one exact phase and independently reconstructed expected subjects.

    The returned tuple is documentary data, not a transferable executor permit.
    Protected entrypoints must invoke this capability themselves, then consume
    the exact grant ID/phase/bytes under their own ledger transaction. A reread
    may authenticate again; this class cannot assert whether it was consumed.
    """

    def __init__(
        self,
        *,
        trust_provider: NonproductionTrustProvider,
        clock: Callable[[], datetime],
        verifier: NonproductionGrantSignatureVerifier,
    ) -> None:
        self._trust_provider, self._clock, self._verifier = trust_provider, clock, verifier

    def authenticate_qualification(
        self,
        *,
        grant_bytes: bytes,
        sigstore_bundle: bytes,
        expected_scope: NonproductionScope,
        qualification_run_id: str,
        fixture_plan_sha256: str,
        qualification_plan_sha256: str,
    ) -> tuple[NonproductionQualificationGrant, NonproductionSignatureSubject]:
        """Bind original qualification bytes to the independent run and plans."""
        grant = NonproductionQualificationGrant.from_bytes(grant_bytes)
        grant.require_qualification_subject(
            qualification_run_id=qualification_run_id,
            fixture_plan_sha256=fixture_plan_sha256,
            qualification_plan_sha256=qualification_plan_sha256,
        )
        return self._authenticate(grant, grant_bytes, sigstore_bundle, expected_scope)

    def authenticate_execution(
        self,
        *,
        grant_bytes: bytes,
        sigstore_bundle: bytes,
        expected_scope: NonproductionScope,
        qualified_set_sha256: str,
        native_release_id: str,
        parent_release_id: str,
        deployment_id: str,
        activation_id: str,
        workloads: tuple[NonproductionWorkload, ...],
    ) -> tuple[NonproductionExecutionGrant, NonproductionSignatureSubject]:
        """Bind original execution bytes to the full independent ancestor set."""
        grant = NonproductionExecutionGrant.from_bytes(grant_bytes)
        grant.require_execution_subject(
            qualified_set_sha256=qualified_set_sha256,
            native_release_id=native_release_id,
            parent_release_id=parent_release_id,
            deployment_id=deployment_id,
            activation_id=activation_id,
            workloads=workloads,
        )
        return self._authenticate(grant, grant_bytes, sigstore_bundle, expected_scope)

    def _authenticate(
        self,
        grant: _Grant,
        grant_bytes: bytes,
        bundle: bytes,
        expected_scope: NonproductionScope,
    ) -> tuple[_Grant, NonproductionSignatureSubject]:
        if type(bundle) is not bytes or not 1 <= len(bundle) <= MAX_ATTESTATION_BUNDLE_BYTES:
            raise NonproductionAuthorityError("signature_bundle_budget") from None
        before, policy, started = self._read_trust()
        policy.require_scope(grant.scope)
        if type(expected_scope) is not NonproductionScope or expected_scope != grant.scope:
            raise NonproductionAuthorityError("scope_subject") from None
        if grant.revocation_epoch != before.current_revocation_epoch or grant.grant_id in policy.revoked_grant_ids:
            raise NonproductionAuthorityError("revoked") from None
        require_validity(
            grant.not_before,
            grant.expires_at,
            min(policy.limits.max_validity_seconds, grant.limits.max_validity_seconds),
            now=started,
        )
        self._require_signer_trust(before, started)
        try:
            subject = self._verifier.verify(grant_bytes=grant_bytes, sigstore_bundle=bundle, trust=before, now=started)
        except Exception:
            raise NonproductionAuthorityError("signature_verification") from None
        after, refreshed_policy, finished = self._read_trust()
        if finished < started:
            raise NonproductionAuthorityError("clock") from None
        if after != before:
            raise NonproductionAuthorityError("trust_changed") from None
        self._require_signer_trust(after, finished)
        validate_grant_subject(
            grant,
            policy=refreshed_policy,
            expected_policy_sha256=after.policy_sha256,
            expected_scope=expected_scope,
            signature_subject=subject,
            now=finished,
            current_revocation_epoch=after.current_revocation_epoch,
        )
        return grant, subject

    def _read_trust(self) -> tuple[NonproductionTrustSnapshot, NonproductionAuthorityPolicy, datetime]:
        try:
            snapshot = self._trust_provider.read()
        except Exception:
            raise NonproductionAuthorityError("trust_source") from None
        try:
            now = self._clock()
            if type(now) is not datetime or now.tzinfo is None or now.utcoffset() is None:
                raise ValueError
            now = now.astimezone(timezone.utc)  # noqa: UP017
        except Exception:
            raise NonproductionAuthorityError("clock_source") from None
        if type(snapshot) is not NonproductionTrustSnapshot:
            raise NonproductionAuthorityError("trust_snapshot") from None
        snapshot.require_integrity()
        policy = NonproductionAuthorityPolicy.from_bytes(snapshot.policy_bytes, expected_sha256=snapshot.policy_sha256)
        policy.require_current(now=now, current_revocation_epoch=snapshot.current_revocation_epoch)
        return snapshot, policy, now

    def _require_signer_trust(self, snapshot: NonproductionTrustSnapshot, now: datetime) -> None:
        try:
            self._verifier.require_trust(trust=snapshot, now=now)
        except Exception:
            raise NonproductionAuthorityError("signer_trust") from None
