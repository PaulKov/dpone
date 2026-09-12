"""Canonical scalar shapes for the PostgreSQL→MSSQL R1 authority."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.contracts.mssql_r1_v3_identity import (
    MAX_SQL_INT,
    canonical_bytes,
    decode_canonical_bytes,
    expect_enum,
    expect_int,
)
from dpone.contracts.postgres_mssql_type_target_enums import (
    MssqlR1TargetScalarFamilyV1,
    PostgresMssqlCodecV1,
    PostgresMssqlEqualityPolicyV1,
    PostgresMssqlLengthKindV1,
    PostgresMssqlNormalizationV1,
    PostgresMssqlSourceScalarFamilyV1,
    PostgresMssqlValueGuardV1,
    authority_decode,
    authority_validation,
    reject,
    require_identifier_v1,
)

_SOURCE = b"dpone-postgres-mssql-source-scalar-shape-v1\0"
_TARGET = b"dpone-mssql-r1-target-scalar-shape-v1\0"
_ADMISSION = b"dpone-postgres-mssql-value-admission-authority-v1\0"

_SOURCE_OIDS = {
    PostgresMssqlSourceScalarFamilyV1.BYTEA: 17,
    PostgresMssqlSourceScalarFamilyV1.BOOL: 16,
    PostgresMssqlSourceScalarFamilyV1.INT8: 20,
    PostgresMssqlSourceScalarFamilyV1.INT2: 21,
    PostgresMssqlSourceScalarFamilyV1.INT4: 23,
    PostgresMssqlSourceScalarFamilyV1.TEXT: 25,
    PostgresMssqlSourceScalarFamilyV1.FLOAT4: 700,
    PostgresMssqlSourceScalarFamilyV1.FLOAT8: 701,
    PostgresMssqlSourceScalarFamilyV1.VARCHAR: 1043,
    PostgresMssqlSourceScalarFamilyV1.DATE: 1082,
    PostgresMssqlSourceScalarFamilyV1.TIME: 1083,
    PostgresMssqlSourceScalarFamilyV1.TIMESTAMP: 1114,
    PostgresMssqlSourceScalarFamilyV1.TIMESTAMPTZ: 1184,
    PostgresMssqlSourceScalarFamilyV1.NUMERIC: 1700,
    PostgresMssqlSourceScalarFamilyV1.UUID: 2950,
}
_NO_FACET = frozenset(
    {
        PostgresMssqlSourceScalarFamilyV1.BOOL,
        PostgresMssqlSourceScalarFamilyV1.INT2,
        PostgresMssqlSourceScalarFamilyV1.INT4,
        PostgresMssqlSourceScalarFamilyV1.INT8,
        PostgresMssqlSourceScalarFamilyV1.FLOAT4,
        PostgresMssqlSourceScalarFamilyV1.FLOAT8,
        PostgresMssqlSourceScalarFamilyV1.UUID,
        PostgresMssqlSourceScalarFamilyV1.DATE,
        PostgresMssqlSourceScalarFamilyV1.BYTEA,
    }
)
_TEMPORAL = frozenset(
    {
        PostgresMssqlSourceScalarFamilyV1.TIME,
        PostgresMssqlSourceScalarFamilyV1.TIMESTAMP,
        PostgresMssqlSourceScalarFamilyV1.TIMESTAMPTZ,
    }
)
_FIXED_TARGET_BYTES = {
    MssqlR1TargetScalarFamilyV1.BIT: 1,
    MssqlR1TargetScalarFamilyV1.SMALLINT: 2,
    MssqlR1TargetScalarFamilyV1.INT: 4,
    MssqlR1TargetScalarFamilyV1.BIGINT: 8,
    MssqlR1TargetScalarFamilyV1.REAL: 4,
    MssqlR1TargetScalarFamilyV1.FLOAT_53: 8,
    MssqlR1TargetScalarFamilyV1.UNIQUEIDENTIFIER: 16,
    MssqlR1TargetScalarFamilyV1.DATE: 3,
}


def _optional_int(value: object, field: str) -> int | None:
    return None if value is None else expect_int(value, field)


@dataclass(frozen=True, slots=True)
class PostgresMssqlSourceScalarShapeV1:
    family: PostgresMssqlSourceScalarFamilyV1
    source_type_oid: int
    source_typmod: int
    length_kind: PostgresMssqlLengthKindV1
    precision: int | None
    scale: int | None
    maximum_characters: int | None

    @authority_validation
    def __post_init__(self) -> None:
        if type(self.family) is not PostgresMssqlSourceScalarFamilyV1:
            reject("enum_unsupported")
        if type(self.length_kind) is not PostgresMssqlLengthKindV1:
            reject("enum_unsupported")
        if type(self.source_type_oid) is not int or self.source_type_oid != _SOURCE_OIDS[self.family]:
            reject("invalid_facet")
        if type(self.source_typmod) is not int:
            reject("invalid_facet")
        self._validate_facets()

    def _validate_facets(self) -> None:
        if self.family in _NO_FACET:
            if (self.source_typmod, self.length_kind, self.precision, self.scale, self.maximum_characters) != (
                -1,
                PostgresMssqlLengthKindV1.NOT_APPLICABLE,
                None,
                None,
                None,
            ):
                reject("invalid_facet")
            return
        if self.family is PostgresMssqlSourceScalarFamilyV1.NUMERIC:
            if (
                type(self.precision) is not int
                or self.precision < 1
                or type(self.scale) is not int
                or not -1024 <= self.scale <= 1023
                or self.maximum_characters is not None
                or self.length_kind is not PostgresMssqlLengthKindV1.NOT_APPLICABLE
                or self.source_typmod != ((self.precision << 16) | (self.scale & 0x7FF)) + 4
            ):
                reject("invalid_facet")
            return
        if self.family in _TEMPORAL:
            if (
                type(self.precision) is not int
                or not 0 <= self.precision <= 6
                or self.scale is not None
                or self.maximum_characters is not None
                or self.length_kind is not PostgresMssqlLengthKindV1.NOT_APPLICABLE
                or self.source_typmod not in {-1, self.precision}
                or (self.source_typmod == -1 and self.precision != 6)
            ):
                reject("invalid_facet")
            return
        if self.family is PostgresMssqlSourceScalarFamilyV1.TEXT:
            if (self.source_typmod, self.length_kind, self.precision, self.scale, self.maximum_characters) != (
                -1,
                PostgresMssqlLengthKindV1.MAXIMUM,
                None,
                None,
                None,
            ):
                reject("invalid_facet")
            return
        if (
            self.family is not PostgresMssqlSourceScalarFamilyV1.VARCHAR
            or self.length_kind is not PostgresMssqlLengthKindV1.BOUNDED
            or type(self.maximum_characters) is not int
            or not 1 <= self.maximum_characters <= 10_485_760
            or self.source_typmod != self.maximum_characters + 4
            or self.precision is not None
            or self.scale is not None
        ):
            reject("invalid_facet")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            _SOURCE,
            (
                self.family,
                self.source_type_oid,
                self.source_typmod,
                self.length_kind,
                self.precision,
                self.scale,
                self.maximum_characters,
            ),
        )

    @property
    def sort_key(self) -> tuple[int, int, int, int, int, bytes]:
        family_order = list(PostgresMssqlSourceScalarFamilyV1).index(self.family)
        length_order = list(PostgresMssqlLengthKindV1).index(self.length_kind)
        return (
            family_order,
            length_order,
            -1 if self.precision is None else self.precision,
            -1 if self.scale is None else self.scale,
            -1 if self.maximum_characters is None else self.maximum_characters,
            self.canonical_bytes,
        )

    @classmethod
    @authority_decode
    def from_canonical_bytes(cls, payload: bytes) -> PostgresMssqlSourceScalarShapeV1:
        values = decode_canonical_bytes(payload, _SOURCE, field_count=7)
        return cls(
            expect_enum(PostgresMssqlSourceScalarFamilyV1, values[0], "source family"),
            expect_int(values[1], "source OID"),
            expect_int(values[2], "source typmod"),
            expect_enum(PostgresMssqlLengthKindV1, values[3], "length kind"),
            _optional_int(values[4], "precision"),
            _optional_int(values[5], "scale"),
            _optional_int(values[6], "maximum characters"),
        )


@dataclass(frozen=True, slots=True)
class MssqlR1CanonicalTargetScalarShapeV1:
    family: MssqlR1TargetScalarFamilyV1
    length_kind: PostgresMssqlLengthKindV1
    precision: int | None
    scale: int | None
    maximum_utf16_units: int | None
    maximum_bytes: int | None
    collation: str | None

    @authority_validation
    def __post_init__(self) -> None:
        if (
            type(self.family) is not MssqlR1TargetScalarFamilyV1
            or type(self.length_kind) is not PostgresMssqlLengthKindV1
        ):
            reject("enum_unsupported")
        self._validate_facets()

    def _validate_facets(self) -> None:
        for value in (self.precision, self.scale, self.maximum_utf16_units, self.maximum_bytes):
            if value is not None and type(value) is not int:
                reject("invalid_facet")
        if self.collation is not None and type(self.collation) is not str:
            reject("invalid_facet")
        expected: tuple[
            PostgresMssqlLengthKindV1,
            int | None,
            int | None,
            int | None,
            int,
            str | None,
        ]
        if self.family in _FIXED_TARGET_BYTES:
            expected = (
                PostgresMssqlLengthKindV1.NOT_APPLICABLE,
                None,
                None,
                None,
                _FIXED_TARGET_BYTES[self.family],
                None,
            )
        elif self.family is MssqlR1TargetScalarFamilyV1.DECIMAL:
            if (
                type(self.precision) is not int
                or type(self.scale) is not int
                or not 1 <= self.precision <= 38
                or not 0 <= self.scale <= self.precision
            ):
                reject("invalid_facet")
            storage = 5 if self.precision <= 9 else 9 if self.precision <= 19 else 13 if self.precision <= 28 else 17
            expected = (PostgresMssqlLengthKindV1.NOT_APPLICABLE, self.precision, self.scale, None, storage, None)
        elif self.family in {
            MssqlR1TargetScalarFamilyV1.TIME,
            MssqlR1TargetScalarFamilyV1.DATETIME2,
            MssqlR1TargetScalarFamilyV1.DATETIMEOFFSET,
        }:
            if type(self.precision) is not int or not 0 <= self.precision <= 6:
                reject("invalid_facet")
            time_bytes = 3 if self.precision <= 2 else 4 if self.precision <= 4 else 5
            extra = (
                0
                if self.family is MssqlR1TargetScalarFamilyV1.TIME
                else 3
                if self.family is MssqlR1TargetScalarFamilyV1.DATETIME2
                else 5
            )
            expected = (PostgresMssqlLengthKindV1.NOT_APPLICABLE, self.precision, None, None, time_bytes + extra, None)
        elif self.family is MssqlR1TargetScalarFamilyV1.NVARCHAR:
            if (
                self.length_kind is PostgresMssqlLengthKindV1.BOUNDED
                and type(self.maximum_utf16_units) is int
                and 1 <= self.maximum_utf16_units <= 4000
            ):
                expected = (
                    self.length_kind,
                    None,
                    None,
                    self.maximum_utf16_units,
                    self.maximum_utf16_units * 2,
                    "Latin1_General_100_BIN2",
                )
            else:
                expected = (PostgresMssqlLengthKindV1.MAXIMUM, None, None, None, -1, "Latin1_General_100_BIN2")
        else:
            expected = (PostgresMssqlLengthKindV1.MAXIMUM, None, None, None, -1, None)
        actual = (
            self.length_kind,
            self.precision,
            self.scale,
            self.maximum_utf16_units,
            self.maximum_bytes,
            self.collation,
        )
        if actual != expected:
            reject("invalid_facet")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(_TARGET, tuple(getattr(self, name) for name in self.__dataclass_fields__))

    @classmethod
    @authority_decode
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1CanonicalTargetScalarShapeV1:
        values = decode_canonical_bytes(payload, _TARGET, field_count=7)
        return cls(
            expect_enum(MssqlR1TargetScalarFamilyV1, values[0], "target family"),
            expect_enum(PostgresMssqlLengthKindV1, values[1], "length kind"),
            _optional_int(values[2], "precision"),
            _optional_int(values[3], "scale"),
            _optional_int(values[4], "maximum UTF-16 units"),
            _optional_int(values[5], "maximum bytes"),
            values[6] if values[6] is None or type(values[6]) is str else reject("invalid_facet"),
        )


@dataclass(frozen=True, slots=True)
class PostgresMssqlValueAdmissionAuthorityV1:
    guard: PostgresMssqlValueGuardV1
    maximum_input_bytes: int
    maximum_utf16_units: int | None

    @authority_validation
    def __post_init__(self) -> None:
        if type(self.guard) is not PostgresMssqlValueGuardV1:
            reject("enum_unsupported")
        if type(self.maximum_input_bytes) is not int or not 1 <= self.maximum_input_bytes <= MAX_SQL_INT:
            reject("invalid_facet")
        if self.maximum_utf16_units is not None and (
            type(self.maximum_utf16_units) is not int or not 1 <= self.maximum_utf16_units <= 4000
        ):
            reject("invalid_facet")
        if self.guard is not PostgresMssqlValueGuardV1.UTF8_AND_UTF16_CAPACITY and self.maximum_utf16_units is not None:
            reject("invalid_facet")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(_ADMISSION, (self.guard, self.maximum_input_bytes, self.maximum_utf16_units))

    @classmethod
    @authority_decode
    def from_canonical_bytes(cls, payload: bytes) -> PostgresMssqlValueAdmissionAuthorityV1:
        guard, size, units = decode_canonical_bytes(payload, _ADMISSION, field_count=3)
        return cls(
            expect_enum(PostgresMssqlValueGuardV1, guard, "value guard"),
            expect_int(size, "maximum input bytes"),
            _optional_int(units, "maximum UTF-16 units"),
        )


__all__ = [
    "MssqlR1CanonicalTargetScalarShapeV1",
    "MssqlR1TargetScalarFamilyV1",
    "PostgresMssqlCodecV1",
    "PostgresMssqlEqualityPolicyV1",
    "PostgresMssqlLengthKindV1",
    "PostgresMssqlNormalizationV1",
    "PostgresMssqlSourceScalarFamilyV1",
    "PostgresMssqlSourceScalarShapeV1",
    "PostgresMssqlValueAdmissionAuthorityV1",
    "PostgresMssqlValueGuardV1",
    "authority_decode",
    "authority_validation",
    "reject",
    "require_identifier_v1",
]
