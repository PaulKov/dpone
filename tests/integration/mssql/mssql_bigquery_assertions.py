"""Assertions for mssql→BigQuery wide live certification."""

from __future__ import annotations

from collections.abc import Sequence

from tests.integration.mssql.mssql_bigquery_wide_fixtures import WideColumn

_BQ_TYPE_ALIASES = {
    "INTEGER": "INT64",
    "FLOAT": "FLOAT64",
    "BOOLEAN": "BOOL",
}


def _normalize_bq_type(value: str) -> str:
    return _BQ_TYPE_ALIASES.get(value.upper(), value.upper())


def assert_row_count(bq, *, dataset: str, table: str, expected: int) -> None:
    rows = bq.get_records(f"SELECT COUNT(*) AS c FROM `{bq.project_id}.{dataset}.{table}`")
    assert int(rows[0]["c"]) == expected


def assert_typed_spot_checks(
    bq,
    *,
    dataset: str,
    table: str,
    columns: Sequence[WideColumn],
    row_id: int = 1,
    expected_name: str = "row-one",
    check_seed_scalars: bool = True,
) -> None:
    client = bq.connection
    bq_table = client.get_table(f"{bq.project_id}.{dataset}.{table}")
    field_types = {field.name: field.field_type for field in bq_table.schema}
    for col in columns:
        if col.name.startswith("__"):
            continue
        assert col.name in field_types, f"missing column {col.name} in BigQuery schema"
        if col.name == "c_blob":
            assert _normalize_bq_type(field_types[col.name]) == "BYTES"
            continue
        assert _normalize_bq_type(field_types[col.name]) == _normalize_bq_type(col.expected_bq_type), (
            f"{col.name}: expected {col.expected_bq_type}, got {field_types[col.name]}"
        )

    if not check_seed_scalars:
        return
    rows = bq.get_records(
        f"SELECT id, c_name, c_varchar, c_bool, c_decimal, c_bigint, TO_HEX(c_blob) AS blob_hex "
        f"FROM `{bq.project_id}.{dataset}.{table}` WHERE id = {row_id}"
    )
    assert len(rows) == 1, f"expected unique id={row_id}, got {rows!r}"
    row = rows[0]
    assert int(row["id"]) == row_id
    assert row["c_name"] == expected_name
    assert row["c_varchar"] == "alpha,comma"
    assert bool(row["c_bool"]) is True
    assert float(row["c_decimal"]) == 12.3456
    assert int(row["c_bigint"]) == 7000000000
    assert str(row["blob_hex"]).lower() == "010203", f"c_blob hex: got {row['blob_hex']!r}"


def assert_unique_ids(bq, *, dataset: str, table: str) -> None:
    rows = bq.get_records(
        f"SELECT id, COUNT(*) AS c FROM `{bq.project_id}.{dataset}.{table}` GROUP BY id HAVING COUNT(*) > 1"
    )
    assert not rows, f"duplicate ids in {dataset}.{table}: {rows!r}"


def fq(bq, dataset: str, table: str) -> str:
    return f"`{bq.project_id}.{dataset}.{table}`"


__all__ = ["assert_row_count", "assert_typed_spot_checks", "assert_unique_ids", "fq"]
