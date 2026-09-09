"""Complete PostgreSQL→MSSQL type-certification inventory."""

from __future__ import annotations

from dpone.type_system.source_sink.certification_models import (
    TypeCertificationCase,
    TypeCertificationSuite,
)
from dpone.type_system.source_sink.postgres_mssql import PostgresMssqlTypeMapper


def postgres_mssql_suite() -> TypeCertificationSuite:
    """Return the canonical built-in, boundary, and policy-required inventory."""

    source, sink = "postgres", "mssql"
    cases = (
        *_aliases("smallint", "integer", "smallint", "smallint", "int2"),
        *_aliases("integer", "integer", "int", "integer", "int", "int4"),
        *_aliases("bigint", "integer", "bigint", "bigint", "int8"),
        _case("smallserial_catalog_int2", "smallint", "integer", "smallint"),
        _case("serial_catalog_int4", "integer", "integer", "int"),
        _case("bigserial_catalog_int8", "bigint", "integer", "bigint"),
        _case("identity_catalog_int4", "integer", "integer", "int"),
        _case("numeric_18_4", "numeric(18,4)", "decimal", "decimal(18,4)"),
        _case("decimal_18_4", "decimal(18,4)", "decimal", "decimal(18,4)"),
        _case("numeric_scale_gt_precision", "numeric(3,5)", "decimal", "decimal(5,5)"),
        _case("numeric_negative_scale", "numeric(2,-3)", "decimal", "decimal(5,0)"),
        _case("numeric_boundary_38", "numeric(38,38)", "decimal", "decimal(38,38)"),
        _explicit("numeric_negative_scale_overflow", "numeric(38,-1)", "decimal"),
        _explicit("numeric_scale_overflow", "numeric(3,39)", "decimal"),
        _explicit("numeric_unconstrained", "numeric", "decimal"),
        _explicit("decimal_unconstrained", "decimal", "decimal"),
        _case("real", "real", "float", "real"),
        _case("real_catalog_float4", "float4", "float", "real"),
        _case("double_precision", "double precision", "float", "float"),
        _case("double_catalog_float8", "float8", "float", "float"),
        *_aliases("boolean", "boolean", "bit", "boolean", "bool"),
        _case("text", "text", "string", "nvarchar(max)"),
        _case("varchar_unbounded", "character varying", "string", "nvarchar(max)"),
        _case("varchar_alias_unbounded", "varchar", "string", "nvarchar(max)"),
        _case("character_default", "character", "string", "nvarchar(2)"),
        _case("char_default", "char", "string", "nvarchar(2)"),
        _case("varchar_255_unicode", "character varying(255)", "string", "nvarchar(510)"),
        _case("varchar_2000_unicode_boundary", "varchar(2000)", "string", "nvarchar(4000)"),
        _explicit("varchar_2001_unicode_overflow", "varchar(2001)", "string"),
        _case("character_8_unicode", "character(8)", "string", "nvarchar(16)"),
        _case("uuid", "uuid", "uuid", "uniqueidentifier"),
        _case("bytea", "bytea", "binary", "varbinary(max)"),
        _case("date", "date", "date", "date"),
        _case("timestamp", "timestamp without time zone", "timestamp", "datetime2(6)"),
        _case("timestamp_alias", "timestamp", "timestamp", "datetime2(6)"),
        _case("timestamp_scale_0", "timestamp(0) without time zone", "timestamp", "datetime2(0)"),
        _case("timestamp_alias_scale_6", "timestamp(6)", "timestamp", "datetime2(6)"),
        _case("timestamptz", "timestamp with time zone", "offset_timestamp", "datetimeoffset(6)"),
        _case("timestamptz_alias", "timestamptz", "offset_timestamp", "datetimeoffset(6)"),
        _case("timestamptz_scale_3", "timestamptz(3)", "offset_timestamp", "datetimeoffset(3)"),
        _case("time", "time without time zone", "time", "time(6)"),
        _case("time_alias", "time", "time", "time(6)"),
        _case("time_scale_0", "time(0)", "time", "time(0)"),
        _case("json", "json", "json", "nvarchar(max)"),
        _case("jsonb", "jsonb", "json", "nvarchar(max)"),
        _explicit("interval", "interval", "interval"),
        _explicit("interval_qualified", "interval day to second(6)", "interval"),
        _explicit("timetz", "time with time zone", "time"),
        _explicit("timetz_alias", "timetz", "time"),
        _explicit("bit", "bit", "bit_string"),
        _explicit("bit_bounded", "bit(8)", "bit_string"),
        _explicit("bit_varying", "bit varying", "bit_string"),
        _explicit("varbit_unbounded", "varbit", "bit_string"),
        _explicit("varbit", "varbit(8)", "bit_string"),
        _explicit("money", "money", "money"),
        *_explicit_aliases("network", "inet", "cidr", "macaddr", "macaddr8"),
        _explicit("xml", "xml", "xml"),
        _explicit("jsonpath", "jsonpath", "jsonpath"),
        _explicit("aclitem", "aclitem", "acl"),
        _explicit("internal_char", '"char"', "internal_char"),
        _explicit("name", "name", "name"),
        _explicit("bpchar_catalog", "bpchar", "string"),
        _explicit("refcursor", "refcursor", "cursor"),
        *_explicit_aliases(
            "oid",
            "oid",
            "xid",
            "xid8",
            "cid",
            "tid",
            "regclass",
            "regcollation",
            "regconfig",
            "regdictionary",
            "regnamespace",
            "regoper",
            "regoperator",
            "regproc",
            "regprocedure",
            "regrole",
            "regtype",
        ),
        *_explicit_aliases("search", "tsvector", "tsquery"),
        _explicit("pg_lsn", "pg_lsn", "lsn"),
        _explicit("pg_snapshot", "pg_snapshot", "snapshot"),
        _explicit("txid_snapshot", "txid_snapshot", "snapshot"),
        *_explicit_aliases("geometric", "point", "line", "lseg", "box", "path", "polygon", "circle"),
        _explicit("array_contract_required", "text[]", "array"),
        _explicit("array_catalog", 'array:{"name":"_text","schema":"pg_catalog"}', "array"),
        _explicit("enum_contract_required", 'enum:{"name":"status","schema":"public"}', "enum"),
        _explicit("domain_catalog", 'domain:{"name":"amount","schema":"public"}', "domain"),
        _explicit("composite_catalog", 'composite:{"name":"address","schema":"public"}', "composite"),
        _explicit("range_contract_required", "int4range", "range"),
        _explicit("range_catalog", 'range:{"name":"int4range","schema":"pg_catalog"}', "range"),
        *_explicit_aliases("range_builtin", "int8range", "numrange", "tsrange", "tstzrange", "daterange"),
        _explicit(
            "multirange_catalog",
            'multirange:{"name":"int4multirange","schema":"pg_catalog"}',
            "multirange",
        ),
        _explicit("multirange_contract_required", "int4multirange", "multirange"),
        *_explicit_aliases(
            "multirange_builtin",
            "int8multirange",
            "nummultirange",
            "tsmultirange",
            "tstzmultirange",
            "datemultirange",
        ),
        _explicit("geometry", "geometry(point,4326)", "spatial"),
        _explicit("geography", "geography(point,4326)", "spatial"),
        _explicit("postgis_raster", "raster", "spatial"),
        _explicit("postgis_catalog", 'spatial:{"name":"geometry","schema":"public"}', "spatial"),
        _explicit("custom_catalog", 'custom:{"name":"citext","schema":"public"}', "custom"),
    )
    return TypeCertificationSuite(
        source=source,
        sink=sink,
        profile="postgres_to_mssql_native_v2",
        runbook="docs/source-sink/postgres-to-mssql.md#type-matrix-certification",
        cases=cases,
    )


def _aliases(prefix: str, canonical: str, target: str, *source_types: str) -> tuple[TypeCertificationCase, ...]:
    return tuple(
        _case(prefix if index == 1 else f"{prefix}_alias_{index - 1}", dtype, canonical, target)
        for index, dtype in enumerate(source_types, 1)
    )


def _explicit_aliases(prefix: str, *source_types: str) -> tuple[TypeCertificationCase, ...]:
    return tuple(_explicit(f"{prefix}_{index}", dtype, prefix) for index, dtype in enumerate(source_types, 1))


def _explicit(name: str, source_type: str, canonical: str) -> TypeCertificationCase:
    return _case(name, source_type, canonical, "nvarchar(max)", requires_contract=True)


def _case(
    name: str,
    source_type: str,
    canonical: str,
    target: str,
    *,
    requires_contract: bool = False,
) -> TypeCertificationCase:
    decision = PostgresMssqlTypeMapper().resolve(source_type)
    return TypeCertificationCase(
        name=name,
        source="postgres",
        sink="mssql",
        source_type=source_type,
        expected_canonical_type=canonical,
        expected_target_type=target,
        expected_transport=decision.transfer_representation,
        decision_category="incompatible_requires_policy" if requires_contract else "auto_inferred",
    )


__all__ = ["postgres_mssql_suite"]
