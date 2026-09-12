"""Render canonical PostgreSQL and MSSQL scalar shapes for compatibility DTOs."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dpone.contracts.postgres_mssql_source_schema_authority import PostgresMssqlSourceSchemaAuthorityErrorV1

if TYPE_CHECKING:
    from dpone.contracts.postgres_mssql_type_target_shapes import (
        MssqlR1CanonicalTargetScalarShapeV1,
        PostgresMssqlSourceScalarShapeV1,
    )


_SOURCE_FIXED = {
    "bool": "boolean",
    "int2": "smallint",
    "int4": "integer",
    "int8": "bigint",
    "float4": "real",
    "float8": "double precision",
    "uuid": "uuid",
    "date": "date",
    "text": "text",
    "bytea": "bytea",
}
_TARGET_FIXED = {
    "bit": "bit",
    "smallint": "smallint",
    "int": "int",
    "bigint": "bigint",
    "real": "real",
    "uniqueidentifier": "uniqueidentifier",
    "date": "date",
}
_REPRESENTATION = {
    "bool_ascii_v1": "0/1 text",
    "signed_integer_ascii_v1": "integer text",
    "decimal_fixed_ascii_v1": "decimal text",
    "ryu_binary32_shortest_ascii_v1": "float text",
    "ryu_binary64_shortest_ascii_v1": "float text",
    "uuid_lower_ascii_v1": "uuid text",
    "iso_date_ascii_v1": "ISO date text",
    "iso_time_ascii_v1": "time text",
    "iso_timestamp_ascii_v1": "timestamp text",
    "iso_utc_timestamp_ascii_v1": "offset timestamp text",
    "utf8_to_utf16le_v1": "BulkTextCodec text",
    "raw_binary_v1": "hex text via character BCP",
}


def render_postgres_declared_type(shape: PostgresMssqlSourceScalarShapeV1) -> str:
    family = shape.family.value
    fixed = _SOURCE_FIXED.get(family)
    if fixed is not None:
        return fixed
    if family == "numeric":
        return f"numeric({shape.precision},{shape.scale})"
    if family == "varchar":
        return f"character varying({shape.maximum_characters})"
    stem = {
        "time": "time",
        "timestamp": "timestamp",
        "timestamptz": "timestamp",
    }[family]
    zone = "with time zone" if family == "timestamptz" else "without time zone"
    precision = "" if shape.source_typmod == -1 else f"({shape.precision})"
    return f"{stem}{precision} {zone}"


def render_mssql_target_type(shape: MssqlR1CanonicalTargetScalarShapeV1) -> str:
    family = shape.family.value
    fixed = _TARGET_FIXED.get(family)
    if fixed is not None:
        return fixed
    if family == "decimal":
        return f"decimal({shape.precision},{shape.scale})"
    if family == "float_53":
        return "float(53)"
    if family in {"time", "datetime2", "datetimeoffset"}:
        return f"{family}({shape.precision})"
    if family in {"nvarchar", "varbinary"}:
        size = "max" if shape.length_kind.value == "maximum" else shape.maximum_utf16_units
        return f"{family}({size})"
    raise PostgresMssqlSourceSchemaAuthorityErrorV1("internal_invariant_violation")


def render_transfer_representation(codec: Any) -> str:
    """Render the closed compatibility transfer token for one exact codec."""

    try:
        return _REPRESENTATION[codec.value]
    except (AttributeError, KeyError, TypeError):
        raise PostgresMssqlSourceSchemaAuthorityErrorV1("internal_invariant_violation") from None
