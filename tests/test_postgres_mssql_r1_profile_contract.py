from __future__ import annotations

from dataclasses import FrozenInstanceError, replace

import pytest

from dpone.contracts.postgres_mssql_correctness_profile import (
    ACTIVATION_BLOCKED,
    CERTIFICATION_UNVERIFIED,
    IMPLEMENTATION_ABSENT,
    PostgresMssqlCorrectnessProfile,
    PostgresMssqlCorrectnessRequirements,
    PostgresMssqlProfileError,
    SourceMode,
    decide_postgres_mssql_correctness_profile,
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
        "implementation_status": IMPLEMENTATION_ABSENT,
        "certification_status": CERTIFICATION_UNVERIFIED,
        "activation_status": ACTIVATION_BLOCKED,
    }
    values.update(overrides)
    return PostgresMssqlCorrectnessProfile(**values)  # type: ignore[arg-type]


def test_profile_decision_is_multidimensional_and_immutable() -> None:
    requirements = PostgresMssqlCorrectnessRequirements(
        source_mode=SourceMode.XMIN_CURRENT_STATE,
        business_key_types=("uuid",),
    )

    decision = decide_postgres_mssql_correctness_profile(requirements, _profile())

    assert decision.accepted is True
    assert decision.blockers == ()
    assert decision.capability_id == "postgres_mssql_target_uow_v2"
    assert decision.canonical_sha256.startswith("sha256:")
    with pytest.raises(FrozenInstanceError):
        decision.accepted = False  # type: ignore[misc]


@pytest.mark.parametrize(
    ("field", "value", "blocker"),
    [
        ("topology", "same_instance_cross_database", "DPONE_POSTGRES_MSSQL_SAME_DATABASE_REQUIRED"),
        ("receipt_contract", "mssql_effect_receipt_v1", "DPONE_POSTGRES_MSSQL_RECEIPT_V2_REQUIRED"),
        ("session_count", 2, "DPONE_POSTGRES_MSSQL_PROFILE_WEAKER_THAN_REQUIRED"),
        ("delayed_durability_disabled", False, "DPONE_POSTGRES_MSSQL_PROFILE_WEAKER_THAN_REQUIRED"),
    ],
)
def test_profile_decision_fails_closed_for_any_weaker_dimension(
    field: str,
    value: object,
    blocker: str,
) -> None:
    profile = replace(_profile(), **{field: value})

    decision = decide_postgres_mssql_correctness_profile(
        PostgresMssqlCorrectnessRequirements(
            source_mode=SourceMode.BATCH_FULL_REFRESH,
            business_key_types=("int8",),
        ),
        profile,
    )

    assert decision.accepted is False
    assert blocker in decision.blockers


def test_profile_rejects_unsupported_key_with_stable_error() -> None:
    decision = decide_postgres_mssql_correctness_profile(
        PostgresMssqlCorrectnessRequirements(
            source_mode=SourceMode.BATCH_FULL_REFRESH,
            business_key_types=("text",),
        ),
        _profile(),
    )

    assert decision.blockers == ("DPONE_POSTGRES_MSSQL_UNSUPPORTED_KEY",)


def test_profile_rejects_noncanonical_dimensions_at_construction() -> None:
    with pytest.raises(PostgresMssqlProfileError, match="business key"):
        PostgresMssqlCorrectnessRequirements(
            source_mode=SourceMode.BATCH_FULL_REFRESH,
            business_key_types=("int8", "uuid"),
        )
    with pytest.raises(PostgresMssqlProfileError, match="proof bound"):
        _profile(max_descendant_proof_receipts=100_001)
    with pytest.raises(PostgresMssqlProfileError, match="certification status"):
        _profile(certification_status="passed")
    with pytest.raises(PostgresMssqlProfileError, match="source_connection_ref"):
        _profile(source_connection_ref=" postgres_orders")
    with pytest.raises(PostgresMssqlProfileError, match="target_binding_uuid"):
        _profile(target_binding_uuid="not-a-uuid")
    with pytest.raises(PostgresMssqlProfileError, match="target_contract_revision"):
        _profile(target_contract_revision=0)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("source_connection_ref", "postgres_orders_replica"),
        ("sink_connection_ref", "mssql_dwh_secondary"),
        ("target_binding_uuid", "fdf32979-112d-49ac-8688-749a08c6b9b7"),
        ("target_contract_revision", 8),
        ("quality_policy", "postgres_mssql_r1_bounded_v2"),
        ("certification_ref", "postgres-mssql-r1-vendor-live-2"),
    ],
)
def test_profile_identity_binds_every_environment_authority_field(field: str, value: object) -> None:
    requirements = PostgresMssqlCorrectnessRequirements(
        source_mode=SourceMode.BATCH_FULL_REFRESH,
        business_key_types=("int8",),
    )

    baseline = decide_postgres_mssql_correctness_profile(requirements, _profile())
    changed = decide_postgres_mssql_correctness_profile(requirements, replace(_profile(), **{field: value}))

    assert baseline.canonical_sha256 != changed.canonical_sha256
