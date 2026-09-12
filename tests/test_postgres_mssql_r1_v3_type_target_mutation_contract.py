from __future__ import annotations

from dataclasses import fields, replace

import pytest

from dpone.contracts.mssql_r1_v3_identity import MssqlR1V3ContractError
from dpone.contracts.postgres_mssql_type_authority import (
    PostgresMssqlSourceColumnRefV1,
    PostgresMssqlTypePolicyAuthorityV1,
)
from dpone.contracts.postgres_mssql_type_derivation import PostgresMssqlTypeDecisionAuthorityV1, derive_type_decision
from dpone.contracts.postgres_mssql_type_target_enums import (
    MssqlR1TargetScalarFamilyV1,
    PostgresMssqlCodecV1,
    PostgresMssqlEqualityPolicyV1,
    PostgresMssqlNormalizationV1,
    PostgresMssqlValueGuardV1,
)
from dpone.contracts.postgres_mssql_type_target_enums import (
    PostgresMssqlSourceScalarFamilyV1 as Source,
)
from dpone.contracts.postgres_mssql_type_target_shapes import (
    MssqlR1CanonicalTargetScalarShapeV1,
    PostgresMssqlSourceScalarShapeV1,
    PostgresMssqlValueAdmissionAuthorityV1,
)
from tests.postgres_mssql_r1_v3_type_target_test_support import source_shape

_STRUCTURAL_CONTRACT_CLASSES = (
    PostgresMssqlSourceScalarShapeV1,
    MssqlR1CanonicalTargetScalarShapeV1,
    PostgresMssqlValueAdmissionAuthorityV1,
    PostgresMssqlTypeDecisionAuthorityV1,
    PostgresMssqlTypePolicyAuthorityV1,
    PostgresMssqlSourceColumnRefV1,
)
STRUCTURAL_WRONG_TYPE_CASES = tuple(
    (f"must_reject__structural_wrong_type__{contract.__name__}__{field.name}", index, field.name)
    for index, contract in enumerate(_STRUCTURAL_CONTRACT_CLASSES)
    for field in fields(contract)
)


def _contract_values():
    source = source_shape(Source.INT4, 23)
    decision = derive_type_decision(source, maximum_input_bytes=32)
    policy = PostgresMssqlTypePolicyAuthorityV1.create((decision,), (source,))
    reference = policy.source_column_ref(ordinal=1, name="order_id", nullable=False, source_shape=source)
    return source, decision.target_shape, decision.value_admission, decision, policy, reference


@pytest.mark.parametrize(
    ("case_id", "contract_index", "field_name"),
    STRUCTURAL_WRONG_TYPE_CASES,
    ids=[case[0] for case in STRUCTURAL_WRONG_TYPE_CASES],
)
def test_type_contract_rejects_structural_wrong_types(case_id, contract_index, field_name) -> None:
    value = _contract_values()[contract_index]

    assert case_id.startswith("must_reject__")
    with pytest.raises(MssqlR1V3ContractError):
        replace(value, **{field_name: object()})


def _all_source_shapes() -> tuple[PostgresMssqlSourceScalarShapeV1, ...]:
    numeric_typmod = ((10 << 16) | (2 & 0x7FF)) + 4
    return (
        source_shape(Source.BOOL, 16),
        source_shape(Source.INT2, 21),
        source_shape(Source.INT4, 23),
        source_shape(Source.INT8, 20),
        source_shape(Source.NUMERIC, 1700, numeric_typmod, precision=10, scale=2),
        source_shape(Source.FLOAT4, 700),
        source_shape(Source.FLOAT8, 701),
        source_shape(Source.UUID, 2950),
        source_shape(Source.DATE, 1082),
        source_shape(Source.TIME, 1083, 6, precision=6),
        source_shape(Source.TIMESTAMP, 1114, 6, precision=6),
        source_shape(Source.TIMESTAMPTZ, 1184, 6, precision=6),
        source_shape(Source.TEXT, 25),
        source_shape(Source.VARCHAR, 1043, 14, maximum_characters=10),
        source_shape(Source.BYTEA, 17),
    )


def test_every_closed_type_enum_arm_is_reached_by_an_admitted_source_family() -> None:
    decisions = tuple(derive_type_decision(shape, maximum_input_bytes=1_048_576) for shape in _all_source_shapes())

    assert {decision.source_shape.family for decision in decisions} == set(Source)
    assert {decision.target_shape.family for decision in decisions} == set(MssqlR1TargetScalarFamilyV1)
    assert {decision.codec for decision in decisions} == set(PostgresMssqlCodecV1)
    assert {decision.normalization for decision in decisions} == set(PostgresMssqlNormalizationV1)
    assert {decision.equality_policy for decision in decisions} == set(PostgresMssqlEqualityPolicyV1)
    assert {decision.value_admission.guard for decision in decisions} == set(PostgresMssqlValueGuardV1)


@pytest.mark.parametrize(
    ("precision", "scale", "admitted"),
    (
        (0, 0, False),
        (1, 0, True),
        (38, 0, True),
        (38, 38, True),
        (4, -2, True),
        (2, 4, True),
        (39, 0, False),
        (38, -1, False),
        (2, 39, False),
    ),
    ids=(
        "must_reject__numeric_precision_0",
        "valid_distinct__numeric_p1_s0",
        "valid_distinct__numeric_p38_s0",
        "valid_distinct__numeric_p38_s38",
        "valid_distinct__numeric_negative_scale",
        "valid_distinct__numeric_scale_above_precision",
        "must_reject__numeric_precision_39",
        "must_reject__numeric_derived_precision_39",
        "must_reject__numeric_scale_39",
    ),
)
def test_numeric_precision_scale_boundaries(precision, scale, admitted) -> None:
    typmod = ((precision << 16) | (scale & 0x7FF)) + 4

    def factory():
        return source_shape(Source.NUMERIC, 1700, typmod, precision=precision, scale=scale)

    if admitted:
        assert derive_type_decision(factory(), maximum_input_bytes=128).target_shape.precision <= 38
    else:
        with pytest.raises(MssqlR1V3ContractError):
            derive_type_decision(factory(), maximum_input_bytes=128)


@pytest.mark.parametrize(
    ("precision", "admitted"),
    ((-1, True), (0, True), (6, True), (7, False)),
    ids=(
        "valid_distinct__temporal_default_precision",
        "valid_distinct__temporal_precision_0",
        "valid_distinct__temporal_precision_6",
        "must_reject__temporal_precision_7",
    ),
)
def test_temporal_precision_boundaries(precision, admitted) -> None:
    effective_precision = 6 if precision == -1 else precision

    def factory():
        return source_shape(Source.TIMESTAMP, 1114, precision, precision=effective_precision)

    if admitted:
        assert derive_type_decision(factory(), maximum_input_bytes=64).target_shape.precision == effective_precision
    else:
        with pytest.raises(MssqlR1V3ContractError):
            factory()


@pytest.mark.parametrize(
    ("maximum_characters", "admitted"),
    ((0, False), (1, True), (2000, True), (2001, True), (10_485_760, True), (10_485_761, False)),
    ids=(
        "must_reject__varchar_length_0",
        "valid_distinct__varchar_length_1",
        "valid_distinct__varchar_length_2000",
        "valid_distinct__varchar_length_2001",
        "valid_distinct__varchar_length_max",
        "must_reject__varchar_length_overflow",
    ),
)
def test_varchar_declared_length_boundaries(maximum_characters, admitted) -> None:
    def factory():
        return source_shape(
            Source.VARCHAR,
            1043,
            maximum_characters + 4,
            maximum_characters=maximum_characters,
        )

    if admitted:
        assert derive_type_decision(factory(), maximum_input_bytes=2**31 - 1)
    else:
        with pytest.raises(MssqlR1V3ContractError):
            factory()


@pytest.mark.parametrize(
    ("maximum_input_bytes", "admitted"),
    ((0, False), (1, True), (2**31 - 1, True), (2**31, False)),
    ids=(
        "must_reject__input_bytes_0",
        "valid_distinct__input_bytes_1",
        "valid_distinct__input_bytes_sql_int_max",
        "must_reject__input_bytes_overflow",
    ),
)
def test_value_admission_byte_limit_boundaries(maximum_input_bytes, admitted) -> None:
    shape = source_shape(Source.INT4, 23)

    if admitted:
        assert derive_type_decision(shape, maximum_input_bytes=maximum_input_bytes)
    else:
        with pytest.raises(MssqlR1V3ContractError):
            derive_type_decision(shape, maximum_input_bytes=maximum_input_bytes)


def test_sql_max_storage_sentinel_is_exact_for_unbounded_text_and_binary() -> None:
    text = derive_type_decision(source_shape(Source.TEXT, 25), maximum_input_bytes=2**31 - 1)
    binary = derive_type_decision(source_shape(Source.BYTEA, 17), maximum_input_bytes=2**31 - 1)

    assert text.target_shape.maximum_bytes == -1
    assert binary.target_shape.maximum_bytes == -1


VALUE_GUARD_MUST_REJECT_CASES = (
    ("must_reject__boolean_guard", source_shape(Source.BOOL, 16), "true"),
    ("must_reject__int2_guard", source_shape(Source.INT2, 21), "32768"),
    ("must_reject__int4_guard", source_shape(Source.INT4, 23), "2147483648"),
    ("must_reject__int8_guard", source_shape(Source.INT8, 20), "9223372036854775808"),
    (
        "must_reject__decimal_guard",
        source_shape(Source.NUMERIC, 1700, ((10 << 16) | 2) + 4, precision=10, scale=2),
        "00.00",
    ),
    ("must_reject__float4_guard", source_shape(Source.FLOAT4, 700), "nan"),
    ("must_reject__float8_guard", source_shape(Source.FLOAT8, 701), "inf"),
    ("must_reject__uuid_guard", source_shape(Source.UUID, 2950), "12345678-1234-1234-1234-123456789ABC"),
    ("must_reject__date_guard", source_shape(Source.DATE, 1082), "0000-01-01"),
    ("must_reject__time_guard", source_shape(Source.TIME, 1083, 6, precision=6), "12:34:56.1234567"),
    (
        "must_reject__timestamp_guard",
        source_shape(Source.TIMESTAMP, 1114, 6, precision=6),
        "2026-01-01 12:34:56.000000",
    ),
    (
        "must_reject__timestamptz_guard",
        source_shape(Source.TIMESTAMPTZ, 1184, 6, precision=6),
        "2026-01-01T12:34:56+00:00",
    ),
    ("must_reject__unicode_guard", source_shape(Source.TEXT, 25), "\ud800"),
    ("must_reject__binary_guard", source_shape(Source.BYTEA, 17), b"x" * 33),
)


@pytest.mark.parametrize(
    ("case_id", "shape", "invalid_value"),
    VALUE_GUARD_MUST_REJECT_CASES,
    ids=[case[0] for case in VALUE_GUARD_MUST_REJECT_CASES],
)
def test_every_value_guard_has_a_named_rejection_vector(case_id, shape, invalid_value) -> None:
    decision = derive_type_decision(shape, maximum_input_bytes=32)

    assert case_id.startswith("must_reject__")
    with pytest.raises(MssqlR1V3ContractError):
        if decision.value_admission.guard is PostgresMssqlValueGuardV1.BINARY_CAPACITY:
            decision.admit_binary(invalid_value)
        else:
            decision.admit_text(invalid_value)


def test_value_guard_rejection_registry_is_complete() -> None:
    guards = {
        derive_type_decision(shape, maximum_input_bytes=32).value_admission.guard
        for _case_id, shape, _invalid_value in VALUE_GUARD_MUST_REJECT_CASES
    }

    assert guards == set(PostgresMssqlValueGuardV1)


def test_source_column_ordinal_closes_exact_1024_boundary_and_integer_subclasses() -> None:
    source = source_shape(Source.INT4, 23)
    policy = PostgresMssqlTypePolicyAuthorityV1.create(
        (derive_type_decision(source, maximum_input_bytes=32),),
        (source,),
    )

    assert policy.source_column_ref(ordinal=1024, name="boundary", nullable=False, source_shape=source).ordinal == 1024
    for ordinal in (0, 1025, True, type("IntSubclass", (int,), {})(1)):
        with pytest.raises(MssqlR1V3ContractError):
            policy.source_column_ref(ordinal=ordinal, name="boundary", nullable=False, source_shape=source)


def test_policy_closes_the_1024_referenced_shape_boundary() -> None:
    shapes = tuple(
        source_shape(Source.VARCHAR, 1043, maximum_characters + 4, maximum_characters=maximum_characters)
        for maximum_characters in range(1, 1026)
    )
    decisions = tuple(derive_type_decision(shape, maximum_input_bytes=4096) for shape in shapes)

    assert len(PostgresMssqlTypePolicyAuthorityV1.create(decisions[:1024], shapes[:1024]).ordered_decisions) == 1024
    with pytest.raises(MssqlR1V3ContractError):
        PostgresMssqlTypePolicyAuthorityV1.create(decisions, shapes)
    with pytest.raises(MssqlR1V3ContractError):
        PostgresMssqlTypePolicyAuthorityV1(
            "dpone-postgres-mssql-type-policy-1",
            tuple(sorted(decisions, key=lambda decision: decision.source_shape.sort_key)),
        )


def _timestamp_typmod_pair():
    before_shape = source_shape(Source.TIMESTAMP, 1114, -1, precision=6)
    after_shape = replace(before_shape, source_typmod=6)
    return before_shape, after_shape


def _admission_limit_pair():
    before_admission = derive_type_decision(source_shape(Source.INT4, 23), maximum_input_bytes=32).value_admission
    after_admission = replace(before_admission, maximum_input_bytes=33)
    return before_admission, after_admission


def _policy_shape_pair():
    before_shape, after_shape = _timestamp_typmod_pair()
    before_policy = PostgresMssqlTypePolicyAuthorityV1.create(
        (derive_type_decision(before_shape, maximum_input_bytes=64),),
        (before_shape,),
    )
    after_policy = PostgresMssqlTypePolicyAuthorityV1.create(
        (derive_type_decision(after_shape, maximum_input_bytes=64),),
        (after_shape,),
    )
    return before_policy, after_policy


TYPE_VALID_DISTINCT_CASES = (
    ("valid_distinct__source_timestamp_typmod", _timestamp_typmod_pair),
    ("valid_distinct__value_admission_limit", _admission_limit_pair),
    ("valid_distinct__policy_shape", _policy_shape_pair),
)


@pytest.mark.parametrize(
    ("case_id", "build_pair"),
    TYPE_VALID_DISTINCT_CASES,
    ids=[case[0] for case in TYPE_VALID_DISTINCT_CASES],
)
def test_valid_semantic_mutations_change_canonical_authority_bytes(case_id, build_pair) -> None:
    before, after = build_pair()

    assert case_id.startswith("valid_distinct__")
    assert before.canonical_bytes != after.canonical_bytes
