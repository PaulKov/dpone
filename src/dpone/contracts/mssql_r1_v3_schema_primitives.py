"""Primitive schema-2 value contracts and canonical validation helpers."""

from __future__ import annotations

import unicodedata
from dataclasses import dataclass, fields
from typing import Any, TypeVar

from dpone._compat import StrEnum
from dpone.contracts.mssql_r1_v3_identity import (
    MAX_SQL_INT,
    MssqlR1V3ContractError,
    canonical_bytes,
    canonical_utc_text,
    decode_canonical_bytes,
    expect_bytes,
    expect_enum,
    expect_text,
    expect_tuple,
    parse_canonical_utc_text,
    require_canonical_text,
    require_digest,
    require_positive,
    require_sql_int,
    require_uuid,
)


class MssqlR1SchemaObjectKindV3(StrEnum):
    TABLE = "table"
    PROCEDURE = "procedure"


class MssqlR1ConstraintKindV3(StrEnum):
    PRIMARY_KEY = "primary_key"
    UNIQUE = "unique"
    FOREIGN_KEY = "foreign_key"
    CHECK = "check"


class MssqlR1IndexDirectionV3(StrEnum):
    ASC = "asc"
    DESC = "desc"


class MssqlR1ParameterDirectionV3(StrEnum):
    INPUT = "input"
    OUTPUT = "output"
    INPUT_OUTPUT = "input_output"


class MssqlR1ResultCardinalityV3(StrEnum):
    ZERO_OR_ONE = "zero_or_one"
    EXACTLY_ONE = "exactly_one"
    ZERO_OR_MANY = "zero_or_many"


class MssqlR1SignerProfileKindV3(StrEnum):
    NONE = "none"
    STAGE_OWNER = "stage_owner"
    ATTESTOR = "attestor"


def require_schema_identifier(value: object, field: str) -> str:
    """Apply the exact schema-2 identifier predicate."""
    if not isinstance(value, str):
        raise MssqlR1V3ContractError(f"{field} must be a schema-2 identifier")
    try:
        units = len(value.encode("utf-16-le")) // 2
        value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise MssqlR1V3ContractError(f"{field} must be a schema-2 identifier") from exc
    if (
        not value
        or value != value.strip()
        or unicodedata.normalize("NFC", value) != value
        or any(unicodedata.category(char) == "Cc" for char in value)
        or units > 128
    ):
        raise MssqlR1V3ContractError(f"{field} must be a schema-2 identifier")
    return value


def require_zero_sql_int(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= MAX_SQL_INT:
        raise MssqlR1V3ContractError(f"{field} must fit a non-negative SQL int")
    return value


def require_maximum_length(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not -1 <= value <= MAX_SQL_INT:
        raise MssqlR1V3ContractError("maximum_length must be -1 or a non-negative SQL int")
    return value


def require_ordinal(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= MAX_SQL_INT:
        raise MssqlR1V3ContractError(f"{field} must fit a positive SQL int")
    return value


def require_identifiers(values: object, field: str, *, required: bool = False) -> tuple[str, ...]:
    if not isinstance(values, tuple) or (required and not values):
        raise MssqlR1V3ContractError(f"{field} must be an ordered tuple")
    result = tuple(require_schema_identifier(value, field) for value in values)
    if len(set(result)) != len(result):
        raise MssqlR1V3ContractError(f"{field} contain duplicates")
    return result


def require_contiguous(values: tuple[Any, ...], field: str) -> None:
    if tuple(item.ordinal for item in values) != tuple(range(1, len(values) + 1)):
        raise MssqlR1V3ContractError(f"{field} ordinals must be contiguous from one")
    if len({item.name for item in values}) != len(values):
        raise MssqlR1V3ContractError(f"{field} names contain duplicates")


T = TypeVar("T")


def decode_members(value: object, contract: type[T], field: str) -> tuple[T, ...]:
    return tuple(
        contract.from_canonical_bytes(expect_bytes(item, field))  # type: ignore[attr-defined]
        for item in expect_tuple(value, field)
    )


def require_canonical_set(values: object, contract: type[T], field: str) -> tuple[T, ...]:
    if not isinstance(values, tuple) or not all(isinstance(item, contract) for item in values):
        raise MssqlR1V3ContractError(f"{field} must be a typed tuple")
    encoded = tuple(item.canonical_bytes for item in values)
    if encoded != tuple(sorted(encoded)) or len(set(encoded)) != len(encoded):
        raise MssqlR1V3ContractError(f"{field} must use strict canonical order")
    return values


def require_canonical_named_set(values: tuple[Any, ...], contract: type[Any], field: str, attribute: str) -> None:
    """Validate a canonical set whose semantic names must also be unique."""
    require_canonical_set(values, contract, field)
    if len({getattr(item, attribute) for item in values}) != len(values):
        raise MssqlR1V3ContractError(f"{field} names contain duplicates")


def require_module_options(
    execute_as: object,
    flags: tuple[object, object, object, object],
    signer_profile: object,
) -> None:
    """Validate the exact closed R1 module-option literals and types."""
    if (
        not all(isinstance(value, bool) for value in flags)
        or (execute_as, *flags) != ("caller", True, True, False, False)
        or not isinstance(signer_profile, MssqlR1SignerProfileKindV3)
    ):
        raise MssqlR1V3ContractError("module options are outside the closed R1 profile")


def field_values(record: Any) -> tuple[object, ...]:
    return tuple(getattr(record, item.name) for item in fields(record))


def _validate_sql_value(
    ordinal: object,
    name: object,
    sql_type: object,
    maximum_length: object,
    precision: object,
    scale: object,
    field: str,
) -> None:
    require_ordinal(ordinal, f"{field} ordinal")
    require_schema_identifier(name, f"{field} name")
    require_canonical_text(sql_type, f"{field} SQL type", maximum_bytes=128)
    require_maximum_length(maximum_length)
    require_zero_sql_int(precision, "precision")
    require_zero_sql_int(scale, "scale")


@dataclass(frozen=True, slots=True)
class MssqlR1SchemaColumnV3:
    ordinal: int
    name: str
    sql_type: str
    maximum_length: int
    precision: int
    scale: int
    nullable: bool
    collation: str | None
    identity: bool
    computed: bool

    def __post_init__(self) -> None:
        _validate_sql_value(
            self.ordinal,
            self.name,
            self.sql_type,
            self.maximum_length,
            self.precision,
            self.scale,
            "column",
        )
        if not all(isinstance(value, bool) for value in (self.nullable, self.identity, self.computed)):
            raise MssqlR1V3ContractError("column flags must be boolean")
        if self.collation is not None:
            require_schema_identifier(self.collation, "collation")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(b"dpone-r1-schema-column-v3-schema-2\0", field_values(self))

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1SchemaColumnV3:
        return cls(*decode_canonical_bytes(payload, b"dpone-r1-schema-column-v3-schema-2\0", field_count=10))  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1SchemaProcedureParameterV3:
    ordinal: int
    name: str
    sql_type: str
    maximum_length: int
    precision: int
    scale: int
    direction: MssqlR1ParameterDirectionV3

    def __post_init__(self) -> None:
        _validate_sql_value(
            self.ordinal,
            self.name,
            self.sql_type,
            self.maximum_length,
            self.precision,
            self.scale,
            "parameter",
        )
        if not isinstance(self.direction, MssqlR1ParameterDirectionV3):
            raise MssqlR1V3ContractError("parameter direction is unsupported")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(b"dpone-r1-schema-procedure-parameter-v3-schema-2\0", field_values(self))

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1SchemaProcedureParameterV3:
        values = list(
            decode_canonical_bytes(payload, b"dpone-r1-schema-procedure-parameter-v3-schema-2\0", field_count=7)
        )
        values[6] = expect_enum(MssqlR1ParameterDirectionV3, values[6], "parameter direction")
        return cls(*values)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1ResultColumnV3:
    ordinal: int
    name: str
    sql_type: str
    maximum_length: int
    precision: int
    scale: int
    nullable: bool
    collation: str | None

    def __post_init__(self) -> None:
        _validate_sql_value(
            self.ordinal,
            self.name,
            self.sql_type,
            self.maximum_length,
            self.precision,
            self.scale,
            "result column",
        )
        if not isinstance(self.nullable, bool):
            raise MssqlR1V3ContractError("result nullability must be boolean")
        if self.collation is not None:
            require_schema_identifier(self.collation, "collation")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(b"dpone-r1-schema-result-column-v3-schema-2\0", field_values(self))

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1ResultColumnV3:
        return cls(*decode_canonical_bytes(payload, b"dpone-r1-schema-result-column-v3-schema-2\0", field_count=8))  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1ExtendedPropertyV3:
    name: str
    value_digest: bytes

    def __post_init__(self) -> None:
        require_schema_identifier(self.name, "extended property")
        require_digest(self.value_digest, "extended property digest")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(b"dpone-r1-schema-property-v3-schema-2\0", field_values(self))

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1ExtendedPropertyV3:
        return cls(*decode_canonical_bytes(payload, b"dpone-r1-schema-property-v3-schema-2\0", field_count=2))  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class MssqlR1SupportedCodecEntryV3:
    codec_id: str
    codec_version: str
    implementation_digest: bytes

    def __post_init__(self) -> None:
        require_canonical_text(self.codec_id, "codec_id", maximum_bytes=256)
        require_canonical_text(self.codec_version, "codec_version", maximum_bytes=128)
        require_digest(self.implementation_digest, "implementation_digest")

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(b"dpone-r1-schema-codec-v3-schema-2\0", field_values(self))

    @classmethod
    def from_canonical_bytes(cls, payload: bytes) -> MssqlR1SupportedCodecEntryV3:
        return cls(*decode_canonical_bytes(payload, b"dpone-r1-schema-codec-v3-schema-2\0", field_count=3))  # type: ignore[arg-type]


__all__ = [name for name in tuple(globals()) if name.startswith("MssqlR1")] + [
    "canonical_bytes",
    "canonical_utc_text",
    "decode_canonical_bytes",
    "decode_members",
    "expect_bytes",
    "expect_enum",
    "expect_text",
    "expect_tuple",
    "field_values",
    "parse_canonical_utc_text",
    "require_canonical_set",
    "require_canonical_named_set",
    "require_canonical_text",
    "require_contiguous",
    "require_digest",
    "require_identifiers",
    "require_module_options",
    "require_positive",
    "require_schema_identifier",
    "require_sql_int",
    "require_uuid",
]
