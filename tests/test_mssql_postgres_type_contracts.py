"""Hermetic contracts for mssql_to_postgres_native_v1."""

from __future__ import annotations

import pytest

from dpone.services.schema_type_matrix import PairTypeMatrixService
from dpone.type_system.source_sink.mssql_postgres import MSSQLPostgresTypeMapper
from dpone.type_system.source_sink.profiles import build_default_profiles


@pytest.mark.parametrize(
    ("source_type", "expected"),
    [
        ("int", "integer"),
        ("int nullable", "integer"),
        ("tinyint", "smallint"),
        ("smallint", "smallint"),
        ("bigint", "bigint"),
        ("bit", "boolean"),
        ("bit nullable", "boolean"),
        ("real", "real"),
        ("float", "double precision"),
        ("decimal(18,4)", "numeric(18,4)"),
        ("decimal(18,4) nullable", "numeric(18,4)"),
        ("numeric(10,2)", "numeric(10,2)"),
        ("date", "date"),
        ("datetime", "timestamp"),
        ("datetime2", "timestamp"),
        ("datetime2(3)", "timestamp"),
        ("datetime2(3) nullable", "timestamp"),
        ("datetimeoffset", "timestamp with time zone"),
        ("datetimeoffset(6)", "timestamp with time zone"),
        ("time", "time"),
        ("time(0)", "time"),
        ("nvarchar(255)", "varchar(255)"),
        ("nvarchar(255) nullable", "varchar(255)"),
        ("nvarchar(max)", "text"),
        ("varchar(64)", "varchar(64)"),
        ("nchar(8)", "char(8)"),
        ("uniqueidentifier", "uuid"),
        ("varbinary(16)", "bytea"),
        ("varbinary(max) nullable", "bytea"),
        ("binary(8)", "bytea"),
        ("xml", "text"),
    ],
)
def test_mssql_postgres_core_types(source_type: str, expected: str) -> None:
    decision = MSSQLPostgresTypeMapper().resolve(source_type)
    assert decision.target_type == expected
    assert decision.compatible is True


def test_mssql_postgres_unknown_requires_contract() -> None:
    decision = MSSQLPostgresTypeMapper().resolve("geography")
    assert decision.target_type == "text"
    assert decision.requires_explicit_contract is True
    assert decision.lossless is False


def test_mssql_postgres_profile_and_matrix_register_pair() -> None:
    profile = build_default_profiles()[("mssql", "postgres")]
    assert profile.profile == "mssql_to_postgres_native_v1"
    assert profile.resolve("nvarchar(max)").target_type == "text"
    matrix = PairTypeMatrixService().build(source="mssql", sink="postgres")
    assert matrix["profile"] == "mssql_to_postgres_native_v1"
    assert matrix["entries"]
