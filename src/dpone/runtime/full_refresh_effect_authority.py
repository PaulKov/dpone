"""Epoch-scoped external-effect authority for bounded full refreshes."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol
from uuid import UUID

from dpone.runtime.full_refresh_attempt import (
    AuthenticatedResourceReadback,
    EffectKind,
    EffectState,
    PlannedCreate,
    PlannedResource,
    ResourceState,
    assert_effect_transition,
    assert_resource_transition,
    require_aware_datetime,
    require_opaque_reference,
)

_CREATE_PAIRED_EDGES = frozenset(
    {
        (ResourceState.CREATE_PLANNED, EffectState.PLANNED, ResourceState.CREATE_GRANTED, EffectState.GRANTED),
        (ResourceState.CREATE_GRANTED, EffectState.GRANTED, ResourceState.CREATE_DISPATCHED, EffectState.DISPATCHED),
        (ResourceState.CREATE_GRANTED, EffectState.GRANTED, ResourceState.CREATE_NOT_APPLIED, EffectState.REVOKED),
        *(
            (ResourceState.CREATE_DISPATCHED, prior, resource_outcome, effect_outcome)
            for prior in (EffectState.DISPATCHED, EffectState.TERMINATED)
            for resource_outcome, effect_outcome in (
                (ResourceState.CREATE_OBSERVED, EffectState.APPLIED),
                (ResourceState.CREATE_NOT_APPLIED, EffectState.NOT_APPLIED),
                (ResourceState.CREATE_FOREIGN_IDENTITY, EffectState.UNKNOWN),
                (ResourceState.CREATE_UNKNOWN, EffectState.UNKNOWN),
            )
        ),
    }
)
_RECOVERY_EDGES = frozenset(
    {
        (ResourceState.CREATE_GRANTED, EffectState.GRANTED, ResourceState.CREATE_NOT_APPLIED, EffectState.REVOKED),
        (
            ResourceState.CREATE_DISPATCHED,
            EffectState.DISPATCHED,
            ResourceState.CREATE_NOT_APPLIED,
            EffectState.NOT_APPLIED,
        ),
    }
)


class UnsupportedEffectAuthorityError(RuntimeError):
    """Raised before mutation when required fencing capabilities are absent."""


@dataclass(frozen=True, slots=True)
class EffectAuthorityCapability:
    profile_id: str
    dynamic_credentials: bool
    revocation: bool
    termination_confirmation: bool
    authenticated_outcome_probe: bool
    reason: str | None = None

    @property
    def supported(self) -> bool:
        return all(
            (
                self.dynamic_credentials,
                self.revocation,
                self.termination_confirmation,
                self.authenticated_outcome_probe,
            )
        )

    def supports(self, profile: EffectAuthorityProfile) -> bool:
        return self.supported and self.profile_id == profile.profile_id


@dataclass(frozen=True, slots=True)
class EffectAuthorityProfile:
    profile_id: str
    source_authority: str
    target_authority: str

    def __post_init__(self) -> None:
        if not self.profile_id or not self.source_authority or not self.target_authority:
            raise ValueError("effect authority profile fields must be non-empty")


@dataclass(frozen=True, slots=True)
class EffectGrant:
    """Short-lived grant; only ``credential_version_ref`` is journal-safe."""

    effect_id: UUID
    lease_epoch: int
    effect_kind: EffectKind
    credential_version_ref: str
    grant_until: datetime
    credential: Any = field(repr=False, compare=False)

    def __post_init__(self) -> None:
        if self.lease_epoch < 1:
            raise ValueError("lease_epoch must be positive")
        require_opaque_reference(self.credential_version_ref, field_name="credential_version_ref")
        require_aware_datetime(self.grant_until, field_name="grant_until")


@dataclass(frozen=True, slots=True)
class CreateTransition:
    """Expected paired CREATE CAS edge plus journal-safe effect facts."""

    expected_resource_state: ResourceState
    next_resource_state: ResourceState
    expected_effect_state: EffectState
    next_effect_state: EffectState
    credential_version_ref: str | None = None
    grant_until: datetime | None = None
    external_query_id: str | None = None
    readback: AuthenticatedResourceReadback | None = None

    def validate(self, planned: PlannedCreate) -> str | None:
        assert_resource_transition(self.expected_resource_state, self.next_resource_state)
        assert_effect_transition(self.expected_effect_state, self.next_effect_state)
        edge = (
            self.expected_resource_state,
            self.expected_effect_state,
            self.next_resource_state,
            self.next_effect_state,
        )
        if edge not in _CREATE_PAIRED_EDGES:
            raise ValueError("invalid paired CREATE resource/effect transition")
        credential_ref = self.credential_version_ref or planned.credential_version_ref
        if credential_ref is not None:
            require_opaque_reference(credential_ref, field_name="credential_version_ref")
        if self.next_resource_state is ResourceState.CREATE_GRANTED and (
            not credential_ref or self.grant_until is None
        ):
            raise ValueError("CREATE grant requires opaque credential reference and expiry")
        if self.grant_until is not None:
            require_aware_datetime(self.grant_until, field_name="grant_until")
        if self.next_resource_state is ResourceState.CREATE_DISPATCHED and not self.external_query_id:
            raise ValueError("CREATE dispatch requires external_query_id")
        if self.next_resource_state is ResourceState.CREATE_OBSERVED and (
            self.readback is None
            or not self.readback.proves(
                planned.resource, effect_id=planned.effect_id, credential_version_ref=credential_ref or ""
            )
        ):
            raise ValueError("authenticated CREATE readback does not match planned resource and grant")
        return credential_ref


@dataclass(frozen=True, slots=True)
class EffectRecoveryReceipt:
    """Authenticated collector proof for a non-mutating expired-lease recovery."""

    attempt_id: UUID
    lease_epoch: int
    effect_id: UUID
    resource_id: UUID
    effect_kind: EffectKind
    expected_resource_state: ResourceState
    next_resource_state: ResourceState
    expected_effect_state: EffectState
    next_effect_state: EffectState
    authority_receipt_ref: str

    def __post_init__(self) -> None:
        require_opaque_reference(self.authority_receipt_ref, field_name="authority_receipt_ref")
        edge = (
            self.expected_resource_state,
            self.expected_effect_state,
            self.next_resource_state,
            self.next_effect_state,
        )
        if self.effect_kind is not EffectKind.CREATE or edge not in _RECOVERY_EDGES:
            raise ValueError("collector recovery may only revoke or record a final exact CREATE outcome")

    def proves(self, planned: PlannedCreate) -> bool:
        return (
            self.attempt_id == planned.attempt.identity.attempt_id
            and self.lease_epoch == planned.attempt.lease_epoch
            and self.effect_id == planned.effect_id
            and self.resource_id == planned.resource.resource_id
        )


class DynamicEffectBroker(Protocol):
    """Deployment-injected broker implementing revocable external authority."""

    def capabilities(self, exact_profile: EffectAuthorityProfile) -> EffectAuthorityCapability: ...

    def mint(
        self,
        *,
        attempt_id: UUID,
        epoch: int,
        effect_id: UUID,
        effect_kind: EffectKind,
        planned_resource: PlannedResource,
    ) -> EffectGrant: ...

    def revoke(self, grant: EffectGrant) -> None: ...

    def terminate_and_confirm(self, grant: EffectGrant) -> object: ...

    def probe_outcome(self, grant: EffectGrant, planned_resource: PlannedResource) -> object: ...


class EpochScopedEffectAuthority(Protocol):
    def admit(self, exact_profile: EffectAuthorityProfile) -> EffectAuthorityCapability: ...

    def mint(
        self,
        *,
        attempt_id: UUID,
        epoch: int,
        effect_id: UUID,
        effect_kind: EffectKind,
        planned_resource: PlannedResource,
    ) -> EffectGrant: ...

    def revoke(self, grant: EffectGrant) -> None: ...

    def terminate_and_confirm(self, grant: EffectGrant) -> object: ...

    def probe_outcome(self, grant: EffectGrant, planned_resource: PlannedResource) -> object: ...


class MssqlClickHouseEpochScopedEffectAuthority:
    """Fail-closed runtime adapter over an injected dynamic credential broker.

    No static connector credentials are accepted by this adapter.  Composition
    must inject a broker that can mint and revoke narrow credentials, confirm
    termination, and authenticate outcome probes for the exact route profile.
    """

    def __init__(self, broker: DynamicEffectBroker, exact_profile: EffectAuthorityProfile) -> None:
        self._broker = broker
        self._exact_profile = exact_profile

    def admit(self, exact_profile: EffectAuthorityProfile) -> EffectAuthorityCapability:
        if exact_profile != self._exact_profile:
            return EffectAuthorityCapability(exact_profile.profile_id, False, False, False, False, "profile mismatch")
        capability = self._broker.capabilities(exact_profile)
        if not capability.supports(exact_profile) and capability.reason is None:
            capability = EffectAuthorityCapability(
                profile_id=capability.profile_id,
                dynamic_credentials=capability.dynamic_credentials,
                revocation=capability.revocation,
                termination_confirmation=capability.termination_confirmation,
                authenticated_outcome_probe=capability.authenticated_outcome_probe,
                reason="epoch-scoped credentials, revocation, termination and outcome probing are required",
            )
        return capability

    def mint(
        self,
        *,
        attempt_id: UUID,
        epoch: int,
        effect_id: UUID,
        effect_kind: EffectKind,
        planned_resource: PlannedResource,
    ) -> EffectGrant:
        self._require_supported(planned_resource)
        grant = self._broker.mint(
            attempt_id=attempt_id,
            epoch=epoch,
            effect_id=effect_id,
            effect_kind=effect_kind,
            planned_resource=planned_resource,
        )
        if (grant.effect_id, grant.lease_epoch, grant.effect_kind) != (effect_id, epoch, effect_kind):
            raise ValueError("broker grant does not match requested effect, epoch, and kind")
        return grant

    def revoke(self, grant: EffectGrant) -> None:
        self._require_admitted()
        self._broker.revoke(grant)

    def terminate_and_confirm(self, grant: EffectGrant) -> object:
        self._require_admitted()
        return self._broker.terminate_and_confirm(grant)

    def probe_outcome(self, grant: EffectGrant, planned_resource: PlannedResource) -> object:
        self._require_supported(planned_resource)
        return self._broker.probe_outcome(grant, planned_resource)

    def _require_admitted(self) -> None:
        capability = self._broker.capabilities(self._exact_profile)
        if not capability.supports(self._exact_profile):
            reason = capability.reason
            raise UnsupportedEffectAuthorityError(reason or "epoch-scoped effect authority is unsupported")

    def _require_supported(self, resource: PlannedResource) -> None:
        self._require_admitted()
        if resource.authority not in {self._exact_profile.source_authority, self._exact_profile.target_authority}:
            raise UnsupportedEffectAuthorityError("resource authority is outside the admitted exact profile")
