"""Assertions for mysql→ClickHouse wide live certification."""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal

from tests.integration.mysql.mysql_clickhouse_strategy_configs import TARGET_DATABASE
from tests.integration.mysql.mysql_clickhouse_wide_fixtures import WideColumn

# system.columns `type` aliases for pair-profile targets.
_CH_TYPE_ALIASES = {
    "int8": {"Int8"},
    "int16": {"Int16"},
    "int32": {"Int32"},
    "int64": {"Int64"},
    "uint8": {"UInt8"},
    "uint16": {"UInt16"},
    "uint32": {"UInt32"},
    "uint64": {"UInt64"},
    "float32": {"Float32"},
    "float64": {"Float64"},
    "bool": {"Bool", "UInt8"},
    "date": {"Date"},
    "string": {"String"},
    "datetime64(6)": {"DateTime64(6)", "DateTime64(3)", "DateTime64(0)"},
}


def _normalize_ch_type(value: str) -> str:
    return str(value).strip()


def _type_family(expected: str) -> str:
    lowered = expected.lower()
    if lowered.startswith("decimal("):
        return "decimal"
    return lowered


def _matches_expected(actual: str, expected: str) -> bool:
    actual_norm = _normalize_ch_type(actual)
    expected_norm = _normalize_ch_type(expected)
    if expected_norm.lower().startswith("decimal("):
        return actual_norm.lower().startswith("decimal(")
    family = _type_family(expected_norm)
    aliases = _CH_TYPE_ALIASES.get(family, {expected_norm})
    return actual_norm in aliases or actual_norm.lower() in {item.lower() for item in aliases}


def assert_row_count(clickhouse, *, table: str, expected: int, database: str = TARGET_DATABASE) -> None:
    rows = clickhouse.get_records(f"SELECT COUNT(*) AS c FROM `{database}`.`{table}`", as_dict=True)
    assert int(rows[0]["c"]) == expected


def assert_unique_ids(clickhouse, *, table: str, database: str = TARGET_DATABASE) -> None:
    rows = clickhouse.get_records(
        f"SELECT id, COUNT(*) AS c FROM `{database}`.`{table}` GROUP BY id HAVING c > 1",
        as_dict=True,
    )
    assert not rows, f"duplicate ids in {database}.{table}: {rows!r}"


def assert_column_types(
    clickhouse,
    *,
    table: str,
    columns: Sequence[WideColumn],
    database: str = TARGET_DATABASE,
) -> None:
    type_rows = clickhouse.get_records(
        f"""
        SELECT name, type
        FROM system.columns
        WHERE database = '{database}' AND table = '{table}'
        """,
        as_dict=True,
    )
    by_name = {row["name"]: _normalize_ch_type(row["type"]) for row in type_rows}

    for col in columns:
        if col.name.startswith("__"):
            continue
        assert col.name in by_name, f"missing column {col.name} in ClickHouse schema"
        actual = by_name[col.name]
        assert _matches_expected(actual, col.expected_ch_type), (
            f"{col.name}: expected {col.expected_ch_type}, got {actual}"
        )


def assert_typed_spot_checks(
    clickhouse,
    *,
    table: str,
    columns: Sequence[WideColumn],
    row_id: int = 1,
    expected_name: str = "row-one",
    check_seed_scalars: bool = True,
    database: str = TARGET_DATABASE,
) -> None:
    assert_column_types(clickhouse, table=table, columns=columns, database=database)

    if not check_seed_scalars:
        return
    rows = clickhouse.get_records(
        f"""
        SELECT id, c_name, c_varchar, c_bool, c_decimal, c_bigint
        FROM `{database}`.`{table}`
        WHERE id = {row_id}
        """,
        as_dict=True,
    )
    assert len(rows) == 1, f"expected unique id={row_id}, got {rows!r}"
    row = rows[0]
    assert int(row["id"]) == row_id
    assert row["c_name"] == expected_name
    assert row["c_varchar"] == "alpha,comma"
    assert bool(row["c_bool"]) is True
    assert Decimal(str(row["c_decimal"])) == Decimal("12.3456")
    assert int(row["c_bigint"]) == 7000000000


def fq(table: str, *, database: str = TARGET_DATABASE) -> str:
    return f"`{database}`.`{table}`"


__all__ = [
    "assert_column_types",
    "assert_row_count",
    "assert_typed_spot_checks",
    "assert_unique_ids",
    "fq",
]
