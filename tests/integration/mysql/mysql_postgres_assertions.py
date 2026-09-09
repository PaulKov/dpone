"""Assertions for mysql→Postgres wide live certification."""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal

from tests.integration.mysql.mysql_postgres_strategy_configs import TARGET_SCHEMA
from tests.integration.mysql.mysql_postgres_wide_fixtures import WideColumn

# information_schema.data_type / format_type aliases for pair-profile targets.
_PG_TYPE_ALIASES = {
    "integer": {"integer", "int4"},
    "smallint": {"smallint", "int2"},
    "bigint": {"bigint", "int8"},
    "boolean": {"boolean", "bool"},
    "real": {"real", "float4"},
    "double precision": {"double precision", "float8"},
    "text": {"text"},
    "bytea": {"bytea"},
    "jsonb": {"jsonb", "json"},
    "date": {"date"},
    "time": {"time", "time without time zone"},
    "timestamp": {"timestamp without time zone", "timestamp"},
}


def assert_row_count(postgres, *, table: str, expected: int, schema: str = TARGET_SCHEMA) -> None:
    rows = postgres.get_records(f'SELECT COUNT(*) AS c FROM "{schema}"."{table}"')
    assert int(rows[0]["c"] if isinstance(rows[0], dict) else rows[0][0]) == expected


def assert_unique_ids(postgres, *, table: str, schema: str = TARGET_SCHEMA) -> None:
    rows = postgres.get_records(f'SELECT id, COUNT(*) AS c FROM "{schema}"."{table}" GROUP BY id HAVING COUNT(*) > 1')
    assert not rows, f"duplicate ids in {schema}.{table}: {rows!r}"


def assert_typed_spot_checks(
    postgres,
    *,
    table: str,
    columns: Sequence[WideColumn],
    row_id: int = 1,
    expected_name: str = "row-one",
    check_seed_scalars: bool = True,
    schema: str = TARGET_SCHEMA,
) -> None:
    type_rows = postgres.get_records(
        f"""
        SELECT column_name, data_type, udt_name
        FROM information_schema.columns
        WHERE table_schema = '{schema}' AND table_name = '{table}'
        """
    )
    by_name = {}
    for row in type_rows:
        if isinstance(row, dict):
            by_name[row["column_name"]] = (row["data_type"], row["udt_name"])
        else:
            by_name[row[0]] = (row[1], row[2])

    for col in columns:
        if col.name.startswith("__"):
            continue
        assert col.name in by_name, f"missing column {col.name} in Postgres schema"
        data_type, udt_name = by_name[col.name]
        expected = col.expected_pg_type.lower()
        if expected.startswith(("numeric(", "varchar(", "char(")):
            # Precision/length may appear in data_type or be stripped; accept family.
            family = expected.split("(", 1)[0]
            assert data_type == family or udt_name == family, (
                f"{col.name}: expected {col.expected_pg_type}, got data_type={data_type} udt={udt_name}"
            )
            continue
        if col.name == "c_blob":
            assert data_type == "bytea" or udt_name == "bytea"
            continue
        aliases = _PG_TYPE_ALIASES.get(expected, {expected})
        assert data_type in aliases or udt_name in aliases, (
            f"{col.name}: expected {col.expected_pg_type}, got data_type={data_type} udt={udt_name}"
        )

    if not check_seed_scalars:
        return
    rows = postgres.get_records(
        f'SELECT id, c_name, c_varchar, c_bool, c_decimal, c_bigint FROM "{schema}"."{table}" WHERE id = {row_id}'
    )
    assert len(rows) == 1, f"expected unique id={row_id}, got {rows!r}"
    row = rows[0]
    get = row.__getitem__ if not isinstance(row, dict) else row.__getitem__
    if isinstance(row, dict):
        assert row["id"] == row_id
        assert row["c_name"] == expected_name
        assert row["c_varchar"] == "alpha,comma"
        assert bool(row["c_bool"]) is True
        assert Decimal(str(row["c_decimal"])) == Decimal("12.3456")
        assert int(row["c_bigint"]) == 7000000000
    else:
        assert get(0) == row_id
        assert get(1) == expected_name
        assert get(2) == "alpha,comma"
        assert bool(get(3)) is True
        assert Decimal(str(get(4))) == Decimal("12.3456")
        assert int(get(5)) == 7000000000


def fq(table: str, *, schema: str = TARGET_SCHEMA) -> str:
    return f'"{schema}"."{table}"'


__all__ = ["assert_row_count", "assert_typed_spot_checks", "assert_unique_ids", "fq"]
