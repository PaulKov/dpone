"""Assertions for postgres→ClickHouse wide live certification."""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal

from tests.integration.postgres.postgres_clickhouse_strategy_configs import TARGET_DATABASE
from tests.integration.postgres.postgres_clickhouse_wide_fixtures import WideColumn

_CH_TYPE_ALIASES = {
    "int16": {"Int16"},
    "int32": {"Int32"},
    "int64": {"Int64"},
    "float32": {"Float32"},
    "float64": {"Float64"},
    "bool": {"Bool", "UInt8"},
    "date": {"Date"},
    "string": {"String"},
    "uuid": {"UUID"},
    "datetime64(6)": {"DateTime64(6)", "DateTime64(3)", "DateTime64(0)"},
    "datetime64(6, 'utc')": {"DateTime64(6, 'UTC')", "DateTime64(6)"},
}


def _strip_nullable(value: str) -> str:
    text = str(value).strip()
    if text.startswith("Nullable(") and text.endswith(")"):
        return text[len("Nullable(") : -1]
    return text


def _matches_expected(actual: str, expected: str) -> bool:
    actual_norm = _strip_nullable(actual)
    expected_norm = _strip_nullable(expected)
    if expected_norm.lower().startswith("decimal("):
        return actual_norm.lower().startswith("decimal(")
    family = expected_norm.lower()
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
    type_rows = clickhouse.get_records(
        f"""
        SELECT name, type
        FROM system.columns
        WHERE database = '{database}' AND table = '{table}'
        """,
        as_dict=True,
    )
    by_name = {row["name"]: str(row["type"]) for row in type_rows}
    for col in columns:
        if col.name.startswith("__"):
            continue
        assert col.name in by_name, f"missing column {col.name} in ClickHouse schema"
        assert _matches_expected(by_name[col.name], col.expected_ch_type), (
            f"{col.name}: expected {col.expected_ch_type}, got {by_name[col.name]}"
        )

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


__all__ = ["assert_row_count", "assert_typed_spot_checks", "assert_unique_ids", "fq"]
