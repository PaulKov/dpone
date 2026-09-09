"""Hermetic contracts for postgres_to_bigquery_analytics_v1."""

from __future__ import annotations

import pytest

from dpone.services.schema_type_matrix import PairTypeMatrixService
from dpone.type_system.source_sink.postgres_bigquery import PostgresBigQueryTypeMapper


@pytest.mark.parametrize(
    ("source_type", "expected"),
    [
        ("integer", "INT64"),
        ("smallint", "INT64"),
        ("bigint", "INT64"),
        ("boolean", "BOOL"),
        ("real", "FLOAT64"),
        ("double precision", "FLOAT64"),
        ("date", "DATE"),
        ("timestamp without time zone", "DATETIME"),
        ("timestamp with time zone", "TIMESTAMP"),
        ("time without time zone", "TIME"),
        ("character varying(255)", "STRING"),
        ("text", "STRING"),
        ("jsonb", "JSON"),
        ("bytea", "BYTES"),
        ("numeric(18,4)", "NUMERIC"),
        ("uuid", "STRING"),
    ],
)
def test_postgres_bigquery_mapper_core_types(source_type: str, expected: str) -> None:
    decision = PostgresBigQueryTypeMapper().resolve(source_type)
    assert decision.target_type == expected


def test_postgres_bigquery_type_matrix_service_registers_pair() -> None:
    matrix = PairTypeMatrixService().build(source="postgres", sink="bigquery")
    assert matrix["profile"] == "postgres_to_bigquery_analytics_v1"
    assert matrix["entries"]
    by_source = {entry["source_type"]: entry["target_type"] for entry in matrix["entries"]}
    assert by_source["integer"] == "INT64"
    assert by_source["jsonb"] == "JSON"


def test_postgres_bigquery_bytea_is_lossless_base64_wire() -> None:
    decision = PostgresBigQueryTypeMapper().resolve("bytea")
    assert decision.target_type == "BYTES"
    assert decision.lossless is True
    assert "base64" in decision.transfer_representation.lower()


def test_postgres_bigquery_csv_wrap_encodes_bytes_columns() -> None:
    from dpone.runtime.sources.strategies.postgres.postgres_file_export_mixin import PostgresFileExportMixin

    wrapped = PostgresFileExportMixin._wrap_bigquery_csv_query(
        PostgresFileExportMixin,
        "SELECT id, c_blob FROM src",
        [("id", "INT64"), ("c_blob", "BYTES")],
    )
    assert "encode(dpone_src.\"c_blob\", 'base64')" in wrapped
    assert 'dpone_src."id"' in wrapped
