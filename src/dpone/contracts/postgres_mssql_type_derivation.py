"""Exact source-shape to target-shape derivation for R1 type authority."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

from dpone.contracts.mssql_r1_v3_identity import (
    canonical_bytes,
    decode_canonical_bytes,
    expect_bytes,
    expect_enum,
    expect_text,
)
from dpone.contracts.postgres_mssql_value_admission import (
    MssqlR1CanonicalTargetScalarShapeV1,
    PostgresMssqlSourceScalarShapeV1,
    PostgresMssqlValueAdmissionAuthorityV1,
    admit_binary_value,
    admit_text_value,
    authority_decode,
    authority_validation,
    reject,
    require_identifier_v1,
)
from dpone.contracts.postgres_mssql_value_admission import (
    MssqlR1TargetScalarFamilyV1 as Target,
)
from dpone.contracts.postgres_mssql_value_admission import (
    PostgresMssqlCodecV1 as Codec,
)
from dpone.contracts.postgres_mssql_value_admission import (
    PostgresMssqlEqualityPolicyV1 as Equality,
)
from dpone.contracts.postgres_mssql_value_admission import (
    PostgresMssqlLengthKindV1 as Length,
)
from dpone.contracts.postgres_mssql_value_admission import (
    PostgresMssqlNormalizationV1 as Normalization,
)
from dpone.contracts.postgres_mssql_value_admission import (
    PostgresMssqlSourceScalarFamilyV1 as Source,
)
from dpone.contracts.postgres_mssql_value_admission import (
    PostgresMssqlValueGuardV1 as Guard,
)


@dataclass(frozen=True, slots=True)
class DecisionParts:
    target: MssqlR1CanonicalTargetScalarShapeV1
    codec: Codec
    normalization: Normalization
    guard: Guard
    equality: Equality


def decision_parts(source: PostgresMssqlSourceScalarShapeV1) -> DecisionParts:
    family = source.family
    simple = {
        Source.BOOL: (
            Target.BIT,
            Codec.BOOL_ASCII_V1,
            Normalization.IDENTITY,
            Guard.BOOLEAN_DOMAIN,
            Equality.BOOLEAN_EXACT,
        ),
        Source.INT2: (
            Target.SMALLINT,
            Codec.SIGNED_INTEGER_ASCII_V1,
            Normalization.IDENTITY,
            Guard.INT2_RANGE,
            Equality.INTEGER_EXACT,
        ),
        Source.INT4: (
            Target.INT,
            Codec.SIGNED_INTEGER_ASCII_V1,
            Normalization.IDENTITY,
            Guard.INT4_RANGE,
            Equality.INTEGER_EXACT,
        ),
        Source.INT8: (
            Target.BIGINT,
            Codec.SIGNED_INTEGER_ASCII_V1,
            Normalization.IDENTITY,
            Guard.INT8_RANGE,
            Equality.INTEGER_EXACT,
        ),
        Source.FLOAT4: (
            Target.REAL,
            Codec.RYU_BINARY32_SHORTEST_ASCII_V1,
            Normalization.CANONICAL_POSITIVE_ZERO,
            Guard.FINITE_FLOAT4,
            Equality.IEEE_VALUE_AFTER_POSITIVE_ZERO,
        ),
        Source.FLOAT8: (
            Target.FLOAT_53,
            Codec.RYU_BINARY64_SHORTEST_ASCII_V1,
            Normalization.CANONICAL_POSITIVE_ZERO,
            Guard.FINITE_FLOAT8,
            Equality.IEEE_VALUE_AFTER_POSITIVE_ZERO,
        ),
        Source.UUID: (
            Target.UNIQUEIDENTIFIER,
            Codec.UUID_LOWER_ASCII_V1,
            Normalization.IDENTITY,
            Guard.UUID_DOMAIN,
            Equality.UUID_OCTETS,
        ),
        Source.DATE: (
            Target.DATE,
            Codec.ISO_DATE_ASCII_V1,
            Normalization.IDENTITY,
            Guard.SQLSERVER_DATE_RANGE,
            Equality.DATE_EXACT,
        ),
        Source.BYTEA: (
            Target.VARBINARY,
            Codec.RAW_BINARY_V1,
            Normalization.IDENTITY,
            Guard.BINARY_CAPACITY,
            Equality.BINARY_OCTETS,
        ),
    }
    if family in simple:
        target, codec, normalization, guard, equality = simple[family]
        return DecisionParts(_target(target), codec, normalization, guard, equality)
    if family is Source.NUMERIC:
        return _numeric_parts(source)
    if family in {Source.TIME, Source.TIMESTAMP, Source.TIMESTAMPTZ}:
        target, codec, normalization, guard, equality = {
            Source.TIME: (
                Target.TIME,
                Codec.ISO_TIME_ASCII_V1,
                Normalization.IDENTITY,
                Guard.SQLSERVER_TIME_PRECISION,
                Equality.TIME_EXACT,
            ),
            Source.TIMESTAMP: (
                Target.DATETIME2,
                Codec.ISO_TIMESTAMP_ASCII_V1,
                Normalization.IDENTITY,
                Guard.SQLSERVER_DATETIME2_RANGE_PRECISION,
                Equality.TIMESTAMP_EXACT,
            ),
            Source.TIMESTAMPTZ: (
                Target.DATETIMEOFFSET,
                Codec.ISO_UTC_TIMESTAMP_ASCII_V1,
                Normalization.UTC_INSTANT,
                Guard.SQLSERVER_DATETIMEOFFSET_RANGE_PRECISION,
                Equality.UTC_INSTANT_EXACT,
            ),
        }[family]
        return DecisionParts(_target(target, precision=source.precision), codec, normalization, guard, equality)
    if family in {Source.TEXT, Source.VARCHAR}:
        units = None if family is Source.TEXT else source.maximum_characters * 2  # type: ignore[operator]
        return DecisionParts(
            _target(Target.NVARCHAR, units=units),
            Codec.UTF8_TO_UTF16LE_V1,
            Normalization.UNICODE_SCALAR_IDENTITY,
            Guard.UTF8_AND_UTF16_CAPACITY,
            Equality.UNICODE_CODEPOINT_EXACT,
        )
    reject("lossy_mapping", operator=True)


def _numeric_parts(source: PostgresMssqlSourceScalarShapeV1) -> DecisionParts:
    p, s = source.precision, source.scale
    assert p is not None and s is not None
    if 0 <= s <= p <= 38:
        target_p, target_s = p, s
    elif s < 0 and p - s <= 38:
        target_p, target_s = p - s, 0
    elif s > p and s <= 38:
        target_p = target_s = s
    else:
        reject("lossy_mapping", operator=True)
    return DecisionParts(
        _target(Target.DECIMAL, precision=target_p, scale=target_s),
        Codec.DECIMAL_FIXED_ASCII_V1,
        Normalization.CANONICAL_POSITIVE_ZERO,
        Guard.DECIMAL_SHAPE_AND_VALUE,
        Equality.DECIMAL_EXACT,
    )


def _target(
    family: Target,
    *,
    precision: int | None = None,
    scale: int | None = None,
    units: int | None = None,
) -> MssqlR1CanonicalTargetScalarShapeV1:
    if family is Target.NVARCHAR:
        bounded = units is not None and units <= 4000
        maximum_bytes = units * 2 if units is not None and bounded else -1
        return MssqlR1CanonicalTargetScalarShapeV1(
            family,
            Length.BOUNDED if bounded else Length.MAXIMUM,
            None,
            None,
            units if bounded else None,
            maximum_bytes,
            "Latin1_General_100_BIN2",
        )
    if family is Target.VARBINARY:
        return MssqlR1CanonicalTargetScalarShapeV1(family, Length.MAXIMUM, None, None, None, -1, None)
    return MssqlR1CanonicalTargetScalarShapeV1(
        family,
        Length.NOT_APPLICABLE,
        precision,
        scale,
        None,
        _target_storage(family, precision),
        None,
    )


def _target_storage(family: Target, precision: int | None) -> int:
    fixed = {
        Target.BIT: 1,
        Target.SMALLINT: 2,
        Target.INT: 4,
        Target.BIGINT: 8,
        Target.REAL: 4,
        Target.FLOAT_53: 8,
        Target.UNIQUEIDENTIFIER: 16,
        Target.DATE: 3,
    }
    if family in fixed:
        return fixed[family]
    assert precision is not None
    if family is Target.DECIMAL:
        return 5 if precision <= 9 else 9 if precision <= 19 else 13 if precision <= 28 else 17
    time_bytes = 3 if precision <= 2 else 4 if precision <= 4 else 5
    return time_bytes + (0 if family is Target.TIME else 3 if family is Target.DATETIME2 else 5)


__all__ = [
    "DecisionParts",
    "PostgresMssqlSourceScalarShapeV1",
    "PostgresMssqlTypeDecisionAuthorityV1",
    "authority_decode",
    "authority_validation",
    "decision_parts",
    "derive_type_decision",
    "reject",
    "require_identifier_v1",
]


_DECISION = b"dpone-postgres-mssql-type-decision-authority-v1\0"


@dataclass(frozen=True, slots=True)
class PostgresMssqlTypeDecisionAuthorityV1:
    decision_id: str
    source_shape: PostgresMssqlSourceScalarShapeV1
    stage_shape: MssqlR1CanonicalTargetScalarShapeV1
    target_shape: MssqlR1CanonicalTargetScalarShapeV1
    codec: Codec
    normalization: Normalization
    value_admission: PostgresMssqlValueAdmissionAuthorityV1
    loss_policy: str
    equality_policy: Equality
    hash_policy: str

    @authority_validation
    def __post_init__(self) -> None:
        if (
            type(self.source_shape) is not PostgresMssqlSourceScalarShapeV1
            or type(self.stage_shape) is not MssqlR1CanonicalTargetScalarShapeV1
            or type(self.target_shape) is not MssqlR1CanonicalTargetScalarShapeV1
            or type(self.codec) is not Codec
            or type(self.normalization) is not Normalization
            or type(self.equality_policy) is not Equality
        ):
            reject("invalid_facet")
        if type(self.decision_id) is not str:
            reject("decision_id_invalid")
        expected_id = "pgmssql_" + hashlib.sha256(self.source_shape.canonical_bytes).hexdigest()[:24]
        if self.decision_id != expected_id:
            reject("decision_id_invalid")
        if self.stage_shape != self.target_shape:
            reject("lossy_mapping", operator=True)
        if type(self.value_admission) is not PostgresMssqlValueAdmissionAuthorityV1:
            reject("authority_splice")
        expected = decision_parts(self.source_shape)
        if (self.target_shape, self.codec, self.normalization, self.value_admission.guard, self.equality_policy) != (
            expected.target,
            expected.codec,
            expected.normalization,
            expected.guard,
            expected.equality,
        ):
            reject("lossy_mapping", operator=True)
        if (
            type(self.loss_policy) is not str
            or type(self.hash_policy) is not str
            or self.loss_policy != "exact_or_block"
            or self.hash_policy != "dpone-canonical-logical-row-sha256-v1"
        ):
            reject("lossy_mapping", operator=True)
        if self.value_admission.maximum_utf16_units != expected.target.maximum_utf16_units:
            reject("invalid_facet")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _DECISION,
            (
                self.decision_id,
                self.source_shape.canonical_bytes,
                self.stage_shape.canonical_bytes,
                self.target_shape.canonical_bytes,
                self.codec,
                self.normalization,
                self.value_admission.canonical_bytes,
                self.loss_policy,
                self.equality_policy,
                self.hash_policy,
            ),
        )

    @classmethod
    @authority_decode
    def from_canonical_bytes(cls, payload: bytes) -> PostgresMssqlTypeDecisionAuthorityV1:
        values = decode_canonical_bytes(payload, _DECISION, field_count=10)
        return cls(
            expect_text(values[0], "decision ID"),
            PostgresMssqlSourceScalarShapeV1.from_canonical_bytes(expect_bytes(values[1], "source shape")),
            MssqlR1CanonicalTargetScalarShapeV1.from_canonical_bytes(expect_bytes(values[2], "stage shape")),
            MssqlR1CanonicalTargetScalarShapeV1.from_canonical_bytes(expect_bytes(values[3], "target shape")),
            expect_enum(Codec, values[4], "codec"),
            expect_enum(Normalization, values[5], "normalization"),
            PostgresMssqlValueAdmissionAuthorityV1.from_canonical_bytes(expect_bytes(values[6], "value admission")),
            expect_text(values[7], "loss policy"),
            expect_enum(Equality, values[8], "equality policy"),
            expect_text(values[9], "hash policy"),
        )

    def admit_text(self, value: str) -> str:
        """Validate one already-projected scalar text without changing it."""

        return admit_text_value(value, self.source_shape, self.value_admission, self.target_shape)

    def admit_binary(self, value: bytes) -> bytes:
        """Validate one raw-binary value without copying or coercing it."""

        return admit_binary_value(value, self.codec, self.value_admission.maximum_input_bytes)


def derive_type_decision(
    source: PostgresMssqlSourceScalarShapeV1, *, maximum_input_bytes: int
) -> PostgresMssqlTypeDecisionAuthorityV1:
    if type(source) is not PostgresMssqlSourceScalarShapeV1:
        reject("invalid_facet")
    parts = decision_parts(source)
    admission = PostgresMssqlValueAdmissionAuthorityV1(
        parts.guard,
        maximum_input_bytes,
        parts.target.maximum_utf16_units,
    )
    return PostgresMssqlTypeDecisionAuthorityV1(
        "pgmssql_" + hashlib.sha256(source.canonical_bytes).hexdigest()[:24],
        source,
        parts.target,
        parts.target,
        parts.codec,
        parts.normalization,
        admission,
        "exact_or_block",
        parts.equality,
        "dpone-canonical-logical-row-sha256-v1",
    )
