from __future__ import annotations

from dataclasses import replace

import pytest

from dpone.contracts.postgres_mssql_correctness_profile import (
    PostgresMssqlCorrectnessActivation,
    PostgresMssqlCorrectnessProfile,
    PostgresMssqlCorrectnessRequirements,
    PostgresMssqlCorrectnessRouteRequest,
    SourceMode,
)
from dpone.readiness.postgres_mssql_correctness_profile import (
    PostgresMssqlCorrectnessProfileResolutionError,
    PostgresMssqlCorrectnessProfileResolver,
)


def _profile(**overrides: object) -> PostgresMssqlCorrectnessProfile:
    values: dict[str, object] = {
        "profile_id": "postgres-mssql-r1",
        "source_connection_ref": "postgres_orders",
        "sink_connection_ref": "mssql_dwh",
        "allowed_source_modes": (SourceMode.BATCH_FULL_REFRESH, SourceMode.XMIN_CURRENT_STATE),
        "source_major": 16,
        "target_major": 2022,
        "topology": "standalone_same_database",
        "object_profile": "ordinary_disk_rowstore",
        "key_types": ("int2", "int4", "int8", "uuid"),
        "receipt_contract": "mssql_effect_receipt_v2",
        "hash_policy": "postgres_mssql_row_hash_v1",
        "writer_fence": "mssql_target_head_v2",
        "session_count": 1,
        "transaction_scope": "local_database",
        "delayed_durability_disabled": True,
        "max_descendant_proof_receipts": 100_000,
        "target_binding_uuid": "3316d0bd-3d61-4a25-a14e-bf21fe038e37",
        "target_contract_revision": 7,
        "quality_policy": "postgres_mssql_r1_bounded_v1",
        "certification_ref": "postgres-mssql-r1-vendor-live",
        "implementation_status": "implemented",
        "certification_status": "vendor_pass",
        "activation_status": "explicit_opt_in",
    }
    values.update(overrides)
    return PostgresMssqlCorrectnessProfile(**values)  # type: ignore[arg-type]


class _Profiles:
    def __init__(self, profile: PostgresMssqlCorrectnessProfile) -> None:
        self.profile = profile
        self.loads: list[str] = []

    def load(self, profile_id: str) -> PostgresMssqlCorrectnessProfile:
        self.loads.append(profile_id)
        return self.profile


class _Evidence:
    def __init__(self, profile: PostgresMssqlCorrectnessProfile | None = None) -> None:
        self.profile = profile
        self.binds = 0

    def bind_current_evidence(
        self,
        profile: PostgresMssqlCorrectnessProfile,
    ) -> PostgresMssqlCorrectnessProfile:
        self.binds += 1
        return self.profile or profile


def _requirements(key: str = "int8") -> PostgresMssqlCorrectnessRequirements:
    return PostgresMssqlCorrectnessRequirements(
        source_mode=SourceMode.BATCH_FULL_REFRESH,
        business_key_types=(key,),
    )


def test_resolver_uses_injected_profile_and_evidence_without_runtime_imports() -> None:
    profiles = _Profiles(_profile())
    evidence = _Evidence()

    decision = PostgresMssqlCorrectnessProfileResolver(profiles, evidence).resolve(
        profile_id="postgres-mssql-r1",
        requirements=_requirements(),
    )

    assert decision.accepted is True
    assert decision.capability_id == "postgres_mssql_target_uow_v2"
    assert decision.certification_status == "vendor_pass"
    assert profiles.loads == ["postgres-mssql-r1"]
    assert evidence.binds == 1


def test_resolver_rejects_empty_profile_reference_before_provider_access() -> None:
    profiles = _Profiles(_profile())

    with pytest.raises(PostgresMssqlCorrectnessProfileResolutionError) as error:
        PostgresMssqlCorrectnessProfileResolver(profiles, _Evidence()).resolve(
            profile_id=" ",
            requirements=_requirements(),
        )

    assert error.value.code == "DPONE_POSTGRES_MSSQL_PROFILE_REQUIRED"
    assert profiles.loads == []


def test_require_activatable_rejects_weaker_tuple_and_unverified_evidence() -> None:
    weak = replace(_profile(), topology="same_instance_cross_database")
    resolver = PostgresMssqlCorrectnessProfileResolver(_Profiles(weak), _Evidence())

    with pytest.raises(PostgresMssqlCorrectnessProfileResolutionError) as error:
        resolver.require_activatable(profile_id="postgres-mssql-r1", requirements=_requirements())

    assert error.value.code == "DPONE_POSTGRES_MSSQL_SAME_DATABASE_REQUIRED"

    unverified = replace(_profile(), certification_status="unverified", activation_status="blocked")
    resolver = PostgresMssqlCorrectnessProfileResolver(_Profiles(unverified), _Evidence())
    with pytest.raises(PostgresMssqlCorrectnessProfileResolutionError) as error:
        resolver.require_activatable(profile_id="postgres-mssql-r1", requirements=_requirements())

    assert error.value.code == "DPONE_POSTGRES_MSSQL_PROFILE_WEAKER_THAN_REQUIRED"


def test_evidence_binding_cannot_change_profile_identity() -> None:
    configured = _profile()
    rebound = replace(configured, profile_id="different-profile")

    with pytest.raises(PostgresMssqlCorrectnessProfileResolutionError) as error:
        PostgresMssqlCorrectnessProfileResolver(_Profiles(configured), _Evidence(rebound)).resolve(
            profile_id="postgres-mssql-r1",
            requirements=_requirements(),
        )

    assert error.value.code == "DPONE_POSTGRES_MSSQL_PROFILE_WEAKER_THAN_REQUIRED"


def test_runtime_activation_keeps_trusted_physical_profile_with_exact_decision() -> None:
    profile = _profile()
    request = PostgresMssqlCorrectnessRouteRequest(
        source_connection_ref="postgres_orders",
        sink_connection_ref="mssql_dwh",
        source_mode=SourceMode.BATCH_FULL_REFRESH,
        target_schema="dbo",
        target_table="orders",
        business_key_types=("int8",),
    )

    activation = PostgresMssqlCorrectnessProfileResolver(
        _Profiles(profile),
        _Evidence(),
    ).require_route_activation(profile_id=profile.profile_id, request=request)

    assert isinstance(activation, PostgresMssqlCorrectnessActivation)
    assert activation.profile is profile
    assert activation.decision.accepted is True
