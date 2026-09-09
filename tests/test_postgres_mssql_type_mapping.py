from __future__ import annotations

import pytest

from dpone.runtime.sources.strategies.postgres.postgres_base_strategy import PostgresBaseStrategy
from dpone.type_system.source_sink.postgres_mssql import PostgresMssqlTypeMapper


def test_postgres_mssql_exact_scalar_mappings() -> None:
    mapper = PostgresMssqlTypeMapper()

    assert mapper.resolve("integer").target_type == "int"
    assert mapper.resolve("bigint").target_type == "bigint"
    assert mapper.resolve("numeric(18,4)").target_type == "decimal(18,4)"
    assert mapper.resolve("uuid").target_type == "uniqueidentifier"
    assert mapper.resolve("bytea").target_type == "varbinary(max)"
    assert mapper.resolve("boolean").target_type == "bit"
    assert mapper.resolve("character varying(255)").target_type == "nvarchar(510)"
    assert mapper.resolve("varchar(64)").target_type == "nvarchar(128)"
    assert mapper.resolve("character(8)").target_type == "nvarchar(16)"
    assert mapper.resolve("char(1)").target_type == "nvarchar(2)"


def test_postgres_mssql_temporal_mappings() -> None:
    mapper = PostgresMssqlTypeMapper()

    assert mapper.resolve("timestamp without time zone").target_type == "datetime2(6)"
    assert mapper.resolve("timestamp with time zone").target_type == "datetimeoffset(6)"
    assert mapper.resolve("date").target_type == "date"
    assert mapper.resolve("time without time zone").target_type == "time(6)"


def test_postgres_mssql_complex_types_land_as_json_or_string() -> None:
    mapper = PostgresMssqlTypeMapper()

    assert mapper.resolve("jsonb").target_type == "nvarchar(max)"
    assert mapper.resolve("text[]").target_type == "nvarchar(max)"
    assert mapper.resolve("int4range").target_type == "nvarchar(max)"
    assert mapper.resolve("my_enum").requires_explicit_contract is True


def test_unconstrained_postgres_numeric_never_invents_lossy_decimal_shape() -> None:
    raw_type = PostgresBaseStrategy._format_type(
        {
            "data_type": "numeric",
            "character_maximum_length": None,
            "numeric_precision": None,
            "numeric_scale": None,
        }
    )

    decision = PostgresMssqlTypeMapper().resolve(raw_type)

    assert raw_type == "numeric"
    assert decision.target_type == "nvarchar(max)"
    assert decision.requires_explicit_contract is True


@pytest.mark.parametrize(
    ("catalog_declared_type", "information_schema_scale", "expected"),
    [
        ("numeric(2,-3)", 2045, "numeric(2,-3)"),
        ("numeric(3,5)", 5, "numeric(3,5)"),
        ("timestamp(3) with time zone", None, "timestamp(3) with time zone"),
        ("character varying(2001)", None, "character varying(2001)"),
    ],
)
def test_postgres_catalog_format_type_is_the_declared_typmod_authority(
    catalog_declared_type: str,
    information_schema_scale: int | None,
    expected: str,
) -> None:
    """Never reconstruct signed/qualified typmods from information_schema."""

    assert (
        PostgresBaseStrategy._format_type(
            {
                "data_type": catalog_declared_type.split("(", 1)[0],
                "catalog_declared_type": catalog_declared_type,
                "numeric_precision": 2,
                "numeric_scale": information_schema_scale,
                "udt_kind": "b",
                "udt_category": "N",
            }
        )
        == expected
    )


def test_postgres_numeric_wider_than_sql_server_decimal_lands_as_exact_text() -> None:
    decision = PostgresMssqlTypeMapper().resolve("numeric(1000,500)")

    assert decision.target_type == "nvarchar(max)"
    assert decision.transfer_representation == "exact numeric text via BulkTextCodec"
    assert decision.requires_explicit_contract is True


def test_postgres_extended_numeric_scale_maps_to_valid_lossless_mssql_shape() -> None:
    mapper = PostgresMssqlTypeMapper()

    assert mapper.resolve("numeric(3,5)").target_type == "decimal(5,5)"
    assert mapper.resolve("numeric(2,-3)").target_type == "decimal(5,0)"
    assert mapper.resolve("numeric(38,38)").target_type == "decimal(38,38)"
    assert mapper.resolve("numeric(38,-1)").target_type == "nvarchar(max)"
    assert mapper.resolve("numeric(3,39)").target_type == "nvarchar(max)"


@pytest.mark.parametrize(
    "source_type",
    [
        "interval",
        "time with time zone",
        "bit varying(12)",
        "money",
        "inet",
        "xml",
        "oid",
        'domain:{"name":"amount","schema":"public"}',
        'enum:{"name":"status","schema":"public"}',
        'composite:{"name":"address","schema":"public"}',
        'multirange:{"name":"int4multirange","schema":"pg_catalog"}',
        'custom:{"name":"citext","schema":"public"}',
        "geometry(point,4326)",
    ],
)
def test_postgres_textual_families_require_explicit_string_contract(source_type: str) -> None:
    decision = PostgresMssqlTypeMapper().resolve(source_type)

    assert decision.target_type == "nvarchar(max)"
    assert decision.requires_explicit_contract is True


def test_postgres_character_wider_than_sql_server_unicode_limit_lands_as_exact_text() -> None:
    varying = PostgresMssqlTypeMapper().resolve("character varying(10000)")
    fixed = PostgresMssqlTypeMapper().resolve("character(4001)")

    assert varying.target_type == "nvarchar(max)"
    assert fixed.target_type == "nvarchar(max)"
    assert varying.requires_explicit_contract is True
    assert fixed.requires_explicit_contract is True
