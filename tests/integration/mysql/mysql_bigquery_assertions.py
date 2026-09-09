"""Assertions for mysql→BigQuery wide live certification."""

from __future__ import annotations

from collections.abc import Sequence

from tests.integration.mysql.mysql_wide_type_fixtures import WideColumn


def assert_row_count(bq, *, dataset: str, table: str, expected: int) -> None:
    rows = bq.get_records(f"SELECT COUNT(*) AS c FROM `{bq.project_id}.{dataset}.{table}`")
    assert int(rows[0]["c"]) == expected


_BQ_TYPE_ALIASES = {
    "INTEGER": "INT64",
    "FLOAT": "FLOAT64",
    "BOOLEAN": "BOOL",
}


def _normalize_bq_type(value: str) -> str:
    return _BQ_TYPE_ALIASES.get(value.upper(), value.upper())


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
    """Validate schema types and a subset of scalar values for one seeded row."""

    client = bq.connection
    bq_table = client.get_table(f"{bq.project_id}.{dataset}.{table}")
    field_types = {field.name: field.field_type for field in bq_table.schema}
    for col in columns:
        if col.name.startswith("__"):
            continue
        # Blob may land as BYTES or STRING depending on load coercion; require presence.
        assert col.name in field_types, f"missing column {col.name} in BigQuery schema"
        if col.name == "c_blob":
            assert _normalize_bq_type(field_types[col.name]) in {"BYTES", "STRING"}
            continue
        assert _normalize_bq_type(field_types[col.name]) == _normalize_bq_type(col.expected_bq_type), (
            f"{col.name}: expected {col.expected_bq_type}, got {field_types[col.name]}"
        )

    if not check_seed_scalars:
        return
    rows = bq.get_records(
        f"SELECT id, c_name, c_varchar, c_bool, c_decimal, c_bigint "
        f"FROM `{bq.project_id}.{dataset}.{table}` WHERE id = {row_id}"
    )
    assert len(rows) == 1, f"expected unique id={row_id}, got {len(rows)} rows: {rows!r}"
    row = rows[0]
    assert row["id"] == row_id
    assert row["c_name"] == expected_name, f"c_name: expected {expected_name!r}, got {row['c_name']!r}"
    assert row["c_varchar"] == "alpha,comma", f"c_varchar: got {row['c_varchar']!r}"
    assert bool(row["c_bool"]) is True, f"c_bool: got {row['c_bool']!r}"
    assert float(row["c_decimal"]) == 12.3456, f"c_decimal: got {row['c_decimal']!r}"
    assert int(row["c_bigint"]) == 7000000000, f"c_bigint: got {row['c_bigint']!r}"


def assert_unique_ids(bq, *, dataset: str, table: str) -> None:
    """Fail closed if a load strategy left duplicate primary keys."""

    rows = bq.get_records(
        f"SELECT id, COUNT(*) AS c FROM `{bq.project_id}.{dataset}.{table}` GROUP BY id HAVING COUNT(*) > 1"
    )
    assert not rows, f"duplicate ids in {dataset}.{table}: {rows!r}"


def fq(bq, dataset: str, table: str) -> str:
    return f"`{bq.project_id}.{dataset}.{table}`"


__all__ = ["assert_row_count", "assert_typed_spot_checks", "assert_unique_ids", "fq"]
