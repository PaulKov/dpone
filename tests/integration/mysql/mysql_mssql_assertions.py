"""Assertions for mysql→MSSQL wide live certification."""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal

from tests.integration.mysql.mysql_mssql_strategy_configs import TARGET_SCHEMA
from tests.integration.mysql.mysql_mssql_wide_fixtures import WideColumn

# SQL Server INFORMATION_SCHEMA.DATA_TYPE families for pair-profile targets.
_MSSQL_TYPE_ALIASES = {
    "int": {"int"},
    "smallint": {"smallint"},
    "bigint": {"bigint"},
    "tinyint": {"tinyint"},
    "bit": {"bit"},
    # Pair profile maps MySQL FLOAT→real; generic MSSQL mapper keeps float for "float".
    "real": {"real", "float"},
    "float": {"float", "real"},
    "date": {"date"},
    "time": {"time"},
    "datetime2": {"datetime2", "datetime"},
    "datetimeoffset": {"datetimeoffset"},
    "varbinary": {"varbinary"},
    "nvarchar": {"nvarchar"},
    "nchar": {"nchar"},
    "decimal": {"decimal", "numeric"},
}


def assert_row_count(mssql, *, table: str, expected: int, schema: str = TARGET_SCHEMA) -> None:
    rows = mssql.get_records(f"SELECT COUNT(*) AS c FROM [{schema}].[{table}]", as_dict=True)
    assert int(rows[0]["c"]) == expected


def assert_unique_ids(mssql, *, table: str, schema: str = TARGET_SCHEMA) -> None:
    rows = mssql.get_records(
        f"SELECT id, COUNT(*) AS c FROM [{schema}].[{table}] GROUP BY id HAVING COUNT(*) > 1",
        as_dict=True,
    )
    assert not rows, f"duplicate ids in {schema}.{table}: {rows!r}"


def assert_typed_spot_checks(
    mssql,
    *,
    table: str,
    columns: Sequence[WideColumn],
    row_id: int = 1,
    expected_name: str = "row-one",
    check_seed_scalars: bool = True,
    schema: str = TARGET_SCHEMA,
) -> None:
    type_rows = mssql.get_records(
        f"""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
        """,
        as_dict=True,
    )
    by_name = {row["COLUMN_NAME"]: str(row["DATA_TYPE"]).lower() for row in type_rows}

    for col in columns:
        if col.name.startswith("__"):
            continue
        assert col.name in by_name, f"missing column {col.name} in MSSQL schema"
        data_type = by_name[col.name]
        expected = col.expected_mssql_type.lower()
        family = expected.split("(", 1)[0]
        aliases = _MSSQL_TYPE_ALIASES.get(family, {family})
        assert data_type in aliases, f"{col.name}: expected {col.expected_mssql_type}, got {data_type}"

    if not check_seed_scalars:
        return
    rows = mssql.get_records(
        f"""
        SELECT id, c_name, c_varchar, c_bool, c_decimal, c_bigint,
               CONVERT(varchar(max), c_blob, 2) AS blob_hex
        FROM [{schema}].[{table}] WHERE id = {row_id}
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
    assert str(row["blob_hex"]).lower() == "010203", f"c_blob hex: got {row['blob_hex']!r}"


def fq(table: str, *, schema: str = TARGET_SCHEMA) -> str:
    return f"[{schema}].[{table}]"


__all__ = ["assert_row_count", "assert_typed_spot_checks", "assert_unique_ids", "fq"]
