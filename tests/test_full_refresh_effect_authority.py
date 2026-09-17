from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from dpone.runtime.full_refresh_attempt import EffectKind, EffectState, PlannedResource, ResourceKind, ResourceState
from dpone.runtime.full_refresh_effect_authority import (
    CreateTransition,
    EffectAuthorityCapability,
    EffectAuthorityProfile,
    EffectGrant,
    MssqlClickHouseEpochScopedEffectAuthority,
    UnsupportedEffectAuthorityError,
)


class Broker:
    def __init__(self, *, dynamic: bool = True, supported_profile_id: str = "mssql_clickhouse_full_refresh_v1") -> None:
        self.dynamic = dynamic
        self.supported_profile_id = supported_profile_id
        self.minted: list[tuple[int, EffectKind]] = []

    def capabilities(self, exact_profile: EffectAuthorityProfile) -> EffectAuthorityCapability:
        return EffectAuthorityCapability(
            profile_id=self.supported_profile_id,
            dynamic_credentials=self.dynamic,
            revocation=self.dynamic,
            termination_confirmation=self.dynamic,
            authenticated_outcome_probe=self.dynamic,
        )

    def mint(self, *, epoch: int, effect_kind: EffectKind, **_: object) -> EffectGrant:
        self.minted.append((epoch, effect_kind))
        return EffectGrant(
            effect_id=UUID("33333333-3333-4333-8333-333333333333"),
            lease_epoch=epoch,
            effect_kind=effect_kind,
            credential_version_ref="cred-version-7",
            grant_until=datetime.now(UTC) + timedelta(minutes=1),
            credential={"token": "ephemeral-secret"},
        )

    def revoke(self, grant: EffectGrant) -> None:
        pass

    def terminate_and_confirm(self, grant: EffectGrant):
        return {"terminated": True}

    def probe_outcome(self, grant: EffectGrant, resource: PlannedResource):
        return {"status": "not_applied"}


def profile() -> EffectAuthorityProfile:
    return EffectAuthorityProfile(
        profile_id="mssql_clickhouse_full_refresh_v1",
        source_authority="mssql-source",
        target_authority="clickhouse-target",
    )


def resource() -> PlannedResource:
    return PlannedResource(
        resource_id=UUID("44444444-4444-4444-8444-444444444444"),
        authority="clickhouse-target",
        planned_name="slice_1",
        resource_kind=ResourceKind.CLICKHOUSE_SLICE_TABLE,
        schema_hash="a" * 64,
    )


def test_static_credentials_are_rejected_before_mint() -> None:
    broker = Broker(dynamic=False)
    authority = MssqlClickHouseEpochScopedEffectAuthority(broker, profile())

    capability = authority.admit(profile())
    assert not capability.supported
    with pytest.raises(UnsupportedEffectAuthorityError, match="epoch-scoped"):
        authority.mint(
            attempt_id=UUID("11111111-1111-4111-8111-111111111111"),
            epoch=1,
            effect_id=UUID("33333333-3333-4333-8333-333333333333"),
            effect_kind=EffectKind.CREATE,
            planned_resource=resource(),
        )
    assert broker.minted == []


def test_dynamic_grant_exposes_opaque_reference_separately_from_secret() -> None:
    broker = Broker()
    authority = MssqlClickHouseEpochScopedEffectAuthority(broker, profile())

    assert authority.admit(profile()).supported
    grant = authority.mint(
        attempt_id=UUID("11111111-1111-4111-8111-111111111111"),
        epoch=4,
        effect_id=UUID("33333333-3333-4333-8333-333333333333"),
        effect_kind=EffectKind.CREATE,
        planned_resource=resource(),
    )

    assert grant.credential_version_ref == "cred-version-7"
    assert grant.credential == {"token": "ephemeral-secret"}
    assert "ephemeral-secret" not in repr(grant)


def test_grant_must_match_requested_effect_and_epoch() -> None:
    class WrongBroker(Broker):
        def mint(self, **kwargs: object) -> EffectGrant:
            return EffectGrant(
                effect_id=UUID("55555555-5555-4555-8555-555555555555"),
                lease_epoch=99,
                effect_kind=EffectKind.RENAME,
                credential_version_ref="cred-version-8",
                grant_until=datetime.now(UTC) + timedelta(minutes=1),
                credential=object(),
            )

    authority = MssqlClickHouseEpochScopedEffectAuthority(WrongBroker(), profile())
    authority.admit(profile())

    with pytest.raises(ValueError, match="does not match"):
        authority.mint(
            attempt_id=UUID("11111111-1111-4111-8111-111111111111"),
            epoch=1,
            effect_id=UUID("33333333-3333-4333-8333-333333333333"),
            effect_kind=EffectKind.CREATE,
            planned_resource=resource(),
        )


def test_authority_is_bound_to_one_exact_profile_and_rechecks_capability() -> None:
    broker = Broker()
    exact = profile()
    authority = MssqlClickHouseEpochScopedEffectAuthority(broker, exact)
    mismatched = EffectAuthorityProfile("another-profile", exact.source_authority, exact.target_authority)

    assert not authority.admit(mismatched).supported
    broker.supported_profile_id = "another-profile"
    with pytest.raises(UnsupportedEffectAuthorityError, match="unsupported"):
        authority.mint(
            attempt_id=UUID("11111111-1111-4111-8111-111111111111"),
            epoch=1,
            effect_id=UUID("33333333-3333-4333-8333-333333333333"),
            effect_kind=EffectKind.CREATE,
            planned_resource=resource(),
        )


def test_grant_expiry_must_be_timezone_aware() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        EffectGrant(
            effect_id=UUID("33333333-3333-4333-8333-333333333333"),
            lease_epoch=1,
            effect_kind=EffectKind.CREATE,
            credential_version_ref="cred-version-7",
            grant_until=datetime.now(),
            credential=object(),
        )


def test_create_transition_rejects_individually_valid_but_unpaired_edges() -> None:
    transition = CreateTransition(
        ResourceState.CREATE_PLANNED,
        ResourceState.CREATE_GRANTED,
        EffectState.GRANTED,
        EffectState.DISPATCHED,
    )

    with pytest.raises(ValueError, match="paired CREATE"):
        transition.validate(object())
