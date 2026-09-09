"""Hermetic contracts for mssql_to_bigquery_analytics_v1."""

from __future__ import annotations

import pytest

from dpone.services.schema_type_matrix import PairTypeMatrixService
from dpone.type_system.source_sink.mssql_bigquery import MSSQLBigQueryTypeMapper
from dpone.type_system.source_sink.profiles import build_default_profiles


@pytest.mark.parametrize(
    ("source_type", "expected"),
    [
        ("int", "INT64"),
        ("tinyint", "INT64"),
        ("smallint", "INT64"),
        ("bigint", "INT64"),
        ("bit", "BOOL"),
        ("real", "FLOAT64"),
        ("float", "FLOAT64"),
        ("date", "DATE"),
        ("datetime", "DATETIME"),
        ("datetime2", "DATETIME"),
        ("datetime2(3)", "DATETIME"),
        ("datetimeoffset", "TIMESTAMP"),
        ("datetimeoffset(6)", "TIMESTAMP"),
        ("time", "TIME"),
        ("time(0)", "TIME"),
        ("nvarchar(255)", "STRING"),
        ("nvarchar(max)", "STRING"),
        ("varchar(64)", "STRING"),
        ("uniqueidentifier", "STRING"),
        ("varbinary(16)", "BYTES"),
        ("varbinary(max)", "BYTES"),
        ("binary(8)", "BYTES"),
        ("decimal(18,4)", "NUMERIC"),
        ("numeric(10,2)", "NUMERIC"),
    ],
)
def test_mssql_bigquery_mapper_core_types(source_type: str, expected: str) -> None:
    decision = MSSQLBigQueryTypeMapper().resolve(source_type)
    assert decision.target_type == expected


def test_mssql_bigquery_varbinary_is_base64_wire() -> None:
    decision = MSSQLBigQueryTypeMapper().resolve("varbinary(16)")
    assert decision.target_type == "BYTES"
    assert decision.lossless is True
    assert "base64" in decision.transfer_representation.lower()


def test_mssql_bigquery_unknown_requires_contract() -> None:
    decision = MSSQLBigQueryTypeMapper().resolve("geography")
    assert decision.target_type == "STRING"
    assert decision.requires_explicit_contract is True
    assert decision.lossless is False


def test_mssql_bigquery_profile_and_matrix_register_pair() -> None:
    profile = build_default_profiles()[("mssql", "bigquery")]
    assert profile.profile == "mssql_to_bigquery_analytics_v1"
    assert profile.resolve("varbinary(16)").target_type == "BYTES"
    matrix = PairTypeMatrixService().build(source="mssql", sink="bigquery")
    assert matrix["profile"] == "mssql_to_bigquery_analytics_v1"
    assert matrix["entries"]
    by_source = {entry["source_type"]: entry["target_type"] for entry in matrix["entries"]}
    assert by_source["int"] == "INT64"
    assert by_source["varbinary(16)"] == "BYTES"
