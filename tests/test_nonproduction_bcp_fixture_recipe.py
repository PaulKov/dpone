"""Finite SQL-text semantics; no statement is executed by these tests."""

import hashlib
import itertools
import json
import re
from dataclasses import FrozenInstanceError, asdict, replace
from typing import Any

import pytest

from dpone.adapters.nonproduction_bcp_fixture_recipe import (
    BcpFixtureColumn,
    bcp_fixture_columns,
    render_bcp_finite_insert,
)
from dpone.contracts.nonproduction_plan_values import NonproductionPlanObject
from dpone.contracts.nonproduction_scope import NonproductionAuthorityError
from tests.nonproduction_plan_helpers import declared


def source() -> NonproductionPlanObject:
    return NonproductionPlanObject.from_dict(declared("source_table", "mssql"))


def selected_keys(sql: str) -> list[int]:
    """Inspect the fixed emitted arithmetic, without emulating SQL Server."""
    assert "(VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9))" in sql
    assert "CONVERT(bigint, 1 + a.n + 10*b.n + 100*c.n + 1000*d.n + 10000*e.n) AS gs" in sql
    assert "FROM digits AS a CROSS JOIN digits AS b CROSS JOIN digits AS c" in sql
    assert "CROSS JOIN digits AS d CROSS JOIN digits AS e" in sql
    limit = re.search(r"WHERE gs >= 1 AND gs <= (\d+)", sql)
    assert limit is not None
    return [
        value
        for digits in itertools.product(range(10), repeat=5)
        if 1 <= (value := 1 + sum(digit * 10**index for index, digit in enumerate(digits))) <= int(limit[1])
    ]


def test_all_202_column_values_match_pre_edit_original() -> None:
    columns = bcp_fixture_columns(202)
    assert type(columns) is tuple and len(columns) == 202
    assert all(type(column) is BcpFixtureColumn for column in columns)
    raw = json.dumps([asdict(column) for column in columns], ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    assert (
        hashlib.sha256(raw.encode()).hexdigest() == "d333f6406514a8b50f579bb2301d990ada4cdc6008dc5ffb52ed375b338955a3"
    )
    with pytest.raises(FrozenInstanceError):
        columns[0].name = "changed"  # type: ignore[misc]


@pytest.mark.parametrize("rows", [0, 1, 2, 7, 11, 2147, 2148, 99999, 100000])
def test_complete_arithmetic_prefix_and_full_projection(rows: int) -> None:
    sql = render_bcp_finite_insert(source(), row_count=rows)
    keys = selected_keys(sql)
    assert sorted(keys) == list(range(1, rows + 1))
    assert len(set(keys)) == rows
    assert "INSERT INTO [database].[schema].[source_table]" in sql
    assert "ORDER BY gs" in sql
    assert all(token not in sql.upper() for token in ("TOP", "ROW_NUMBER", "GENERATE_SERIES", "DROP ", "CREATE "))
    for column in bcp_fixture_columns(202):
        assert f"{column.insert_expression} AS [{column.name}]" in sql


@pytest.mark.parametrize("rows", [2147, 2148, 100000])
def test_bigint_conversion_precedes_reused_product_expression(rows: int) -> None:
    sql = render_bcp_finite_insert(source(), row_count=rows)
    prefix = "CONVERT(bigint, 1 + a.n + 10*b.n + 100*c.n + 1000*d.n + 10000*e.n) AS gs"
    product = "CAST(gs * 1000003 AS bigint)"
    assert sql.index(prefix) < sql.index(product)
    assert (rows * 1000003 > 2**31 - 1) is (rows >= 2148)
    assert rows * 1000003 <= 2**63 - 1


@pytest.mark.parametrize("rows", [-1, 100001, True, False, 1.0, "1", None])
def test_guarded_row_count_is_strict(rows: Any) -> None:
    with pytest.raises(NonproductionAuthorityError, match="fixture_recipe_rows$"):
        render_bcp_finite_insert(source(), row_count=rows)


@pytest.mark.parametrize("bad", [None, {}, "source"])
def test_source_type_is_checked_before_row_count(bad: Any) -> None:
    with pytest.raises(NonproductionAuthorityError, match="fixture_recipe_source$"):
        render_bcp_finite_insert(bad, row_count=-1)


@pytest.mark.parametrize("connector,kind", [("postgres", "table"), ("clickhouse", "table"), ("postgres", "sequence")])
def test_guarded_source_is_mssql_table(connector: str, kind: str) -> None:
    bad = NonproductionPlanObject.from_dict(declared("source", connector, kind))
    with pytest.raises(NonproductionAuthorityError, match="fixture_recipe_source$"):
        render_bcp_finite_insert(bad, row_count=1)


def test_source_revalidation_preserves_nested_error() -> None:
    value = source()
    object.__setattr__(value, "qualified_name", ("database", "schema", "bad; DROP TABLE source"))
    with pytest.raises(NonproductionAuthorityError, match="plan_objects$"):
        render_bcp_finite_insert(value, row_count=1)


def test_valid_source_names_are_used_without_defaulting() -> None:
    value = replace(source(), qualified_name=("Db_2", "Schema_3", "Table_4"))
    assert "INSERT INTO [Db_2].[Schema_3].[Table_4]" in render_bcp_finite_insert(value, row_count=1)


@pytest.mark.parametrize(
    "before,after",
    [
        (
            "CONVERT(bigint, 1 + a.n + 10*b.n + 100*c.n + 1000*d.n + 10000*e.n)",
            "(1 + a.n + 10*b.n + 100*c.n + 1000*d.n + 10000*e.n)",
        ),
        ("10000*e.n", "1000*e.n"),
        ("(8),(9)", "(8)"),
        ("WHERE gs >= 1", "WHERE gs > 1"),
        ("bigint, 1 + a.n", "bigint, 0 + a.n"),
    ],
)
def test_fixed_selector_oracle_rejects_isolated_sql_mutations(before: str, after: str) -> None:
    sql = render_bcp_finite_insert(source(), row_count=2148)
    assert before in sql
    with pytest.raises(AssertionError):
        selected_keys(sql.replace(before, after))


def test_complete_bcp_bound_query_preserves_native_decimal_and_null_framing() -> None:
    from dpone.adapters.nonproduction_bcp_fixture_recipe import render_bcp_fixture_bound_query
    from dpone.runtime.native_wire_mssql import build_mssql_bcp_native_contract

    query = render_bcp_fixture_bound_query(source(), wire_contract_builder=build_mssql_bcp_native_contract)
    assert "COUNT_BIG(*) AS source_rows" in query
    assert "FROM [database].[schema].[source_table]" in query
    assert "CASE WHEN [amount_high] IS NULL THEN 0 ELSE 19 END" in query
    assert "DATALENGTH([unicode_max])" in query
    assert all(token not in query.upper() for token in ("WHERE", "TOP", "VARCHAR(", "ROW_COUNT", "ORDER BY"))
    assert all(f"[{column.name}]" in query for column in bcp_fixture_columns(202))
