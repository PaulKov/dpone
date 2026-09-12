from __future__ import annotations

from dataclasses import replace

import pytest

from dpone.contracts.mssql_r1_v3_codec import canonical_bytes
from dpone.contracts.mssql_r1_v3_identity import MssqlR1V3ContractError
from dpone.contracts.postgres_mssql_type_authority import (
    PostgresMssqlSourceColumnRefV1,
    PostgresMssqlTypePolicyAuthorityV1,
)
from dpone.contracts.postgres_mssql_type_derivation import derive_type_decision
from dpone.contracts.postgres_mssql_type_target_enums import (
    MssqlR1TargetScalarFamilyV1,
    MssqlR1TypeTargetAuthorityError,
    MssqlR1TypeTargetRecoveryClass,
    PostgresMssqlCodecV1,
    PostgresMssqlLengthKindV1,
    PostgresMssqlSourceScalarFamilyV1,
)
from dpone.contracts.postgres_mssql_type_target_shapes import (
    MssqlR1CanonicalTargetScalarShapeV1,
    PostgresMssqlSourceScalarShapeV1,
)
from dpone.contracts.postgres_mssql_value_admission import (
    canonical_binary32_text,
    canonical_binary64_text,
)


def _shape(
    family: PostgresMssqlSourceScalarFamilyV1,
    oid: int,
    typmod: int = -1,
    *,
    precision: int | None = None,
    scale: int | None = None,
    maximum_characters: int | None = None,
) -> PostgresMssqlSourceScalarShapeV1:
    length_kind = (
        PostgresMssqlLengthKindV1.BOUNDED
        if family is PostgresMssqlSourceScalarFamilyV1.VARCHAR
        else PostgresMssqlLengthKindV1.MAXIMUM
        if family is PostgresMssqlSourceScalarFamilyV1.TEXT
        else PostgresMssqlLengthKindV1.NOT_APPLICABLE
    )
    return PostgresMssqlSourceScalarShapeV1(
        family,
        oid,
        typmod,
        length_kind,
        precision,
        scale,
        maximum_characters,
    )


@pytest.mark.parametrize(
    ("shape", "target", "codec"),
    (
        (
            _shape(PostgresMssqlSourceScalarFamilyV1.BOOL, 16),
            MssqlR1TargetScalarFamilyV1.BIT,
            PostgresMssqlCodecV1.BOOL_ASCII_V1,
        ),
        (
            _shape(PostgresMssqlSourceScalarFamilyV1.INT2, 21),
            MssqlR1TargetScalarFamilyV1.SMALLINT,
            PostgresMssqlCodecV1.SIGNED_INTEGER_ASCII_V1,
        ),
        (
            _shape(PostgresMssqlSourceScalarFamilyV1.INT4, 23),
            MssqlR1TargetScalarFamilyV1.INT,
            PostgresMssqlCodecV1.SIGNED_INTEGER_ASCII_V1,
        ),
        (
            _shape(PostgresMssqlSourceScalarFamilyV1.INT8, 20),
            MssqlR1TargetScalarFamilyV1.BIGINT,
            PostgresMssqlCodecV1.SIGNED_INTEGER_ASCII_V1,
        ),
        (
            _shape(PostgresMssqlSourceScalarFamilyV1.FLOAT4, 700),
            MssqlR1TargetScalarFamilyV1.REAL,
            PostgresMssqlCodecV1.RYU_BINARY32_SHORTEST_ASCII_V1,
        ),
        (
            _shape(PostgresMssqlSourceScalarFamilyV1.FLOAT8, 701),
            MssqlR1TargetScalarFamilyV1.FLOAT_53,
            PostgresMssqlCodecV1.RYU_BINARY64_SHORTEST_ASCII_V1,
        ),
        (
            _shape(PostgresMssqlSourceScalarFamilyV1.UUID, 2950),
            MssqlR1TargetScalarFamilyV1.UNIQUEIDENTIFIER,
            PostgresMssqlCodecV1.UUID_LOWER_ASCII_V1,
        ),
        (
            _shape(PostgresMssqlSourceScalarFamilyV1.TEXT, 25),
            MssqlR1TargetScalarFamilyV1.NVARCHAR,
            PostgresMssqlCodecV1.UTF8_TO_UTF16LE_V1,
        ),
        (
            _shape(PostgresMssqlSourceScalarFamilyV1.BYTEA, 17),
            MssqlR1TargetScalarFamilyV1.VARBINARY,
            PostgresMssqlCodecV1.RAW_BINARY_V1,
        ),
    ),
    ids=("bool", "int2", "int4", "int8", "float4", "float8", "uuid", "text", "bytea"),
)
def test_exact_scalar_decision_matrix(shape, target, codec) -> None:
    decision = derive_type_decision(shape, maximum_input_bytes=1_048_576)

    assert decision.target_shape.family is target
    assert decision.stage_shape == decision.target_shape
    assert decision.codec is codec
    assert decision.loss_policy == "exact_or_block"
    assert len(decision.decision_id) == len("pgmssql_") + 24


def test_numeric_typmod_and_mapping_are_exact() -> None:
    typmod = ((20 << 16) | (4 & 0x7FF)) + 4
    decision = derive_type_decision(
        _shape(PostgresMssqlSourceScalarFamilyV1.NUMERIC, 1700, typmod, precision=20, scale=4),
        maximum_input_bytes=128,
    )

    assert decision.target_shape.family is MssqlR1TargetScalarFamilyV1.DECIMAL
    assert (decision.target_shape.precision, decision.target_shape.scale) == (20, 4)
    assert decision.admit_text("0.1000") == "0.1000"
    assert decision.admit_text("-0.1000") == "-0.1000"
    with pytest.raises(MssqlR1V3ContractError):
        decision.admit_text("-0.0000")
    with pytest.raises(MssqlR1V3ContractError):
        decision.admit_text("00.1000")
    with pytest.raises(MssqlR1V3ContractError):
        _shape(
            PostgresMssqlSourceScalarFamilyV1.NUMERIC,
            1700,
            ((20 << 16) | (2048 & 0x7FF)) + 4,
            precision=20,
            scale=2048,
        )


def test_policy_reuses_equal_shapes_and_totally_orders_temporal_typmods() -> None:
    default = _shape(PostgresMssqlSourceScalarFamilyV1.TIMESTAMP, 1114, -1, precision=6)
    explicit = _shape(PostgresMssqlSourceScalarFamilyV1.TIMESTAMP, 1114, 6, precision=6)
    first = derive_type_decision(default, maximum_input_bytes=64)
    second = derive_type_decision(explicit, maximum_input_bytes=64)

    policy = PostgresMssqlTypePolicyAuthorityV1.create((second, first, first), (default, explicit, default))

    assert policy.ordered_decisions == tuple(sorted((first, second), key=lambda item: item.source_shape.sort_key))
    assert PostgresMssqlTypePolicyAuthorityV1.from_canonical_bytes(policy.canonical_bytes) == policy
    with pytest.raises(MssqlR1V3ContractError):
        PostgresMssqlTypePolicyAuthorityV1(
            "dpone-postgres-mssql-type-policy-1",
            tuple(reversed(policy.ordered_decisions)),
        )


def test_shape_rejects_oid_typmod_and_structural_substitution() -> None:
    value = _shape(PostgresMssqlSourceScalarFamilyV1.VARCHAR, 1043, 14, maximum_characters=10)
    assert PostgresMssqlSourceScalarShapeV1.from_canonical_bytes(value.canonical_bytes) == value

    for field, changed in (
        ("source_type_oid", 25),
        ("source_typmod", 13),
        ("maximum_characters", 0),
    ):
        with pytest.raises(MssqlR1V3ContractError):
            replace(value, **{field: changed})

    with pytest.raises(MssqlR1V3ContractError):
        PostgresMssqlSourceScalarShapeV1(
            "varchar",  # type: ignore[arg-type]
            1043,
            14,
            PostgresMssqlLengthKindV1.BOUNDED,
            None,
            None,
            10,
        )

    decision = derive_type_decision(value, maximum_input_bytes=64)
    with pytest.raises(MssqlR1TypeTargetAuthorityError):
        replace(decision, codec=decision.codec.value)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("bits", "expected"),
    (
        ("00000000", "0"),
        ("80000000", "0"),
        ("3dcccccd", "0.1"),
        ("00800000", "1.1754944e-38"),
        ("00000001", "1e-45"),
        ("7f7fffff", "3.4028235e38"),
    ),
)
def test_binary32_ryu_golden_vectors(bits: str, expected: str) -> None:
    assert canonical_binary32_text(bytes.fromhex(bits)) == expected


@pytest.mark.parametrize(
    ("bits", "expected"),
    (
        ("0000000000000000", "0"),
        ("8000000000000000", "0"),
        ("3fb999999999999a", "0.1"),
        ("0010000000000000", "2.2250738585072014e-308"),
        ("0000000000000001", "5e-324"),
        ("7fefffffffffffff", "1.7976931348623157e308"),
    ),
)
def test_binary64_ryu_golden_vectors(bits: str, expected: str) -> None:
    assert canonical_binary64_text(bytes.fromhex(bits)) == expected


def test_binary64_ryu_removes_redundant_integral_suffix() -> None:
    assert canonical_binary64_text(bytes.fromhex("3ff0000000000000")) == "1"


@pytest.mark.parametrize(
    ("bits", "expected"),
    (
        ("416312d000000000", "1e7"),
        ("430c6bf526340000", "1e15"),
        ("3f50624dd2f1a9fc", "1e-3"),
        ("3f1a36e2eb1c432d", "1e-4"),
    ),
)
def test_binary64_shortest_notation_is_independent_of_python_repr_thresholds(bits, expected) -> None:
    assert canonical_binary64_text(bytes.fromhex(bits)) == expected


def test_target_shape_rejects_bool_in_integer_storage_facet() -> None:
    with pytest.raises(MssqlR1V3ContractError):
        MssqlR1CanonicalTargetScalarShapeV1(
            MssqlR1TargetScalarFamilyV1.BIT,
            PostgresMssqlLengthKindV1.NOT_APPLICABLE,
            None,
            None,
            None,
            True,
            None,
        )


def test_unknown_enum_decode_uses_closed_enum_reason() -> None:
    payload = canonical_bytes(
        b"dpone-postgres-mssql-source-scalar-shape-v1\0",
        ("future", 23, -1, PostgresMssqlLengthKindV1.NOT_APPLICABLE, None, None, None),
    )

    with pytest.raises(MssqlR1TypeTargetAuthorityError) as caught:
        PostgresMssqlSourceScalarShapeV1.from_canonical_bytes(payload)

    assert caught.value.reason_id == "enum_unsupported"


def test_value_admission_closes_float_uuid_temporal_text_and_binary_grammars() -> None:
    float4 = derive_type_decision(
        _shape(PostgresMssqlSourceScalarFamilyV1.FLOAT4, 700),
        maximum_input_bytes=32,
    )
    uuid = derive_type_decision(
        _shape(PostgresMssqlSourceScalarFamilyV1.UUID, 2950),
        maximum_input_bytes=36,
    )
    timestamp = derive_type_decision(
        _shape(PostgresMssqlSourceScalarFamilyV1.TIMESTAMP, 1114, 6, precision=6),
        maximum_input_bytes=32,
    )
    timestamptz = derive_type_decision(
        _shape(PostgresMssqlSourceScalarFamilyV1.TIMESTAMPTZ, 1184, 6, precision=6),
        maximum_input_bytes=32,
    )
    text = derive_type_decision(
        _shape(PostgresMssqlSourceScalarFamilyV1.VARCHAR, 1043, 6, maximum_characters=2),
        maximum_input_bytes=16,
    )
    binary = derive_type_decision(
        _shape(PostgresMssqlSourceScalarFamilyV1.BYTEA, 17),
        maximum_input_bytes=2,
    )

    assert float4.admit_text("0.1") == "0.1"
    assert uuid.admit_text("12345678-1234-1234-1234-123456789abc").endswith("9abc")
    assert timestamp.admit_text("2026-09-05T12:34:56.123456").endswith("123456")
    assert timestamptz.admit_text("2026-09-05T12:34:56.123456Z").endswith("Z")
    assert text.admit_text("😀") == "😀"
    assert binary.admit_binary(b"\x00\xff") == b"\x00\xff"

    for decision, value in (
        (float4, "0.10000000149011612"),
        (uuid, "12345678-1234-1234-1234-123456789ABC"),
        (timestamp, "2026-09-05T12:34:56.1234567"),
        (timestamptz, "2026-09-05T14:34:56.123456+02:00"),
        (text, "😀😀a"),
    ):
        with pytest.raises(MssqlR1TypeTargetAuthorityError):
            decision.admit_text(value)
    with pytest.raises(MssqlR1TypeTargetAuthorityError):
        binary.admit_binary(b"abc")


def test_negative_numeric_scale_requires_exact_divisibility() -> None:
    typmod = ((4 << 16) | (-2 & 0x7FF)) + 4
    decision = derive_type_decision(
        _shape(
            PostgresMssqlSourceScalarFamilyV1.NUMERIC,
            1700,
            typmod,
            precision=4,
            scale=-2,
        ),
        maximum_input_bytes=32,
    )

    assert decision.admit_text("1200") == "1200"
    with pytest.raises(MssqlR1TypeTargetAuthorityError):
        decision.admit_text("1201")


def test_policy_issues_digest_bound_source_reference() -> None:
    shape = _shape(PostgresMssqlSourceScalarFamilyV1.INT8, 20)
    policy = PostgresMssqlTypePolicyAuthorityV1.create(
        (derive_type_decision(shape, maximum_input_bytes=32),),
        (shape,),
    )

    reference = policy.source_column_ref(
        ordinal=1,
        name="order_id",
        nullable=False,
        source_shape=shape,
    )

    assert reference.type_policy_digest == policy.digest
    assert PostgresMssqlSourceColumnRefV1.from_canonical_bytes(reference.canonical_bytes) == reference


def test_source_column_ref_requires_policy_owned_construction_and_revalidation() -> None:
    source = _shape(PostgresMssqlSourceScalarFamilyV1.INT4, 23)
    policy = PostgresMssqlTypePolicyAuthorityV1.create(
        (derive_type_decision(source, maximum_input_bytes=32),),
        (source,),
    )
    reference = policy.source_column_ref(ordinal=1, name="order_id", nullable=False, source_shape=source)
    other_source = _shape(PostgresMssqlSourceScalarFamilyV1.INT8, 20)
    other = PostgresMssqlTypePolicyAuthorityV1.create(
        (derive_type_decision(other_source, maximum_input_bytes=32),),
        (other_source,),
    )

    with pytest.raises(MssqlR1V3ContractError):
        PostgresMssqlSourceColumnRefV1(
            reference.ordinal,
            reference.name,
            reference.nullable,
            reference.source_shape,
            reference.type_policy_digest,
        )
    reference.validate_against_policy(policy)
    with pytest.raises(MssqlR1V3ContractError):
        reference.validate_against_policy(other)


def test_closed_error_taxonomy_uses_specified_recovery_classes() -> None:
    cases = (
        ("invalid_facet", MssqlR1TypeTargetRecoveryClass.PERMANENT_INPUT_ERROR),
        ("active_head_stale", MssqlR1TypeTargetRecoveryClass.REFRESH_AUTHORITY_AND_RETRY),
        ("authority_splice", MssqlR1TypeTargetRecoveryClass.OPERATOR_INTERVENTION),
    )
    for reason, recovery in cases:
        with pytest.raises(MssqlR1TypeTargetAuthorityError) as raised:
            from dpone.contracts.postgres_mssql_type_target_enums import reject

            reject(reason)
        assert (raised.value.reason_id, raised.value.recovery_class) == (reason, recovery)


def test_decode_wraps_wrong_domain_and_malformed_bytes_in_closed_error_family() -> None:
    shape = _shape(PostgresMssqlSourceScalarFamilyV1.INT4, 23)

    for payload, reason in (
        (b"wrong-domain\0", "wrong_domain"),
        (shape.canonical_bytes[:-1], "malformed_canonical_bytes"),
    ):
        with pytest.raises(MssqlR1TypeTargetAuthorityError) as raised:
            PostgresMssqlSourceScalarShapeV1.from_canonical_bytes(payload)
        assert raised.value.reason_id == reason
