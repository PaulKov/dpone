"""Exact source expressions and fixed row vectors; no backend evaluation."""

import hashlib
import json
from dataclasses import FrozenInstanceError, astuple
from typing import Any

import pytest

from dpone.adapters.nonproduction_postgres_fixture_recipe import PostgresFixtureColumn, postgres_fixture_columns
from dpone.adapters.nonproduction_postgres_fixture_rows import postgres_seed_rows
from dpone.contracts.nonproduction_scope import NonproductionAuthorityError


def test_every_raw_column_matches_the_pre_edit_inventory() -> None:
    columns = postgres_fixture_columns()
    assert type(columns) is tuple and len(columns) == 128
    assert all(type(column) is PostgresFixtureColumn for column in columns)
    raw = json.dumps([astuple(column) for column in columns], ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    assert (
        hashlib.sha256(raw.encode()).hexdigest() == "85a20821a9cc225f59e27ac152c9b64a069a59ca288c16e54931822414eded6b"
    )
    with pytest.raises(FrozenInstanceError):
        columns[0].seed_sql = "2"  # type: ignore[misc]
    assert columns[0].pg_ddl == "INTEGER NOT NULL"


@pytest.mark.parametrize(
    "step,index,expected",
    [
        ("initial", 0, "188c746404954411250287e3c3a48ae9ea16d5880f18c323e9b8989240498767"),
        ("initial", 1, "acc16de48fea99d1e3e49a7d7405923e68a7f1114e2b5be9083137a94f487dd9"),
        ("watermark_3", 0, "f2dea096974de9518151c2d41f79d99521b1317e5ade1f9448ef11abd1a7472b"),
    ],
)
def test_all_row_expression_bytes_match_original_sql(step: str, index: int, expected: str) -> None:
    rows = postgres_seed_rows(step)
    assert type(rows) is tuple and len(rows) == (2 if step == "initial" else 1)
    assert all(type(row) is tuple and len(row) == 128 for row in rows)
    values = ", ".join(rows[index]).replace("__DPONE_ENUM_CAST__", '"fixture_schema"."dpone_wide_enum"')
    assert hashlib.sha256(values.encode()).hexdigest() == expected


@pytest.mark.parametrize("step", ["initial", "watermark_3"])
def test_serial_values_are_explicit_and_enum_tokens_are_retained(step: str) -> None:
    names = [column.name for column in postgres_fixture_columns()]
    for row in postgres_seed_rows(step):
        values = dict(zip(names, row, strict=True))
        assert tuple(values[name] for name in ("c_smallserial", "c_serial", "c_identity", "c_bigserial")) == (
            "71",
            "72",
            "73",
            "74",
        )
        assert not any("nextval" in expression.lower() for expression in row)
    assert any("__DPONE_ENUM_CAST__" in expression for expression in postgres_seed_rows(step)[0])


def test_second_row_nullability_and_fixed_watermark_keys() -> None:
    columns = postgres_fixture_columns()
    first, second = postgres_seed_rows("initial")
    (third,) = postgres_seed_rows("watermark_3")
    assert (first[0], second[0], third[0]) == ("1", "2", "3")
    for column, value in zip(columns, second, strict=True):
        if column.name not in {"id", "updated_at", "c_name"}:
            assert value == (column.seed_sql if "NOT NULL" in column.pg_ddl.upper() else "NULL")


@pytest.mark.parametrize("step", ["", "watermark", "watermark_7", " initial", "INITIAL", True, 3, None, []])
def test_fixed_step_has_no_arbitrary_key_or_coercion(step: Any) -> None:
    with pytest.raises(NonproductionAuthorityError, match="fixture_recipe_step$"):
        postgres_seed_rows(step)


def test_complete_postgres_bound_query_has_no_selector_or_expected_count() -> None:
    from dpone.adapters.nonproduction_postgres_fixture_rows import render_postgres_fixture_bound_query
    from dpone.contracts.nonproduction_plan_values import NonproductionPlanObject
    from tests.nonproduction_plan_helpers import declared

    source = NonproductionPlanObject.from_dict(declared("source", "postgres"))
    query = render_postgres_fixture_bound_query(source)
    assert 'FROM "schema"."source"' in query
    assert "COUNT(*) AS source_rows" in query
    assert "convert_to" in query and "UTF8" in query
    assert all(token not in query.upper() for token in ("WHERE", "LIMIT", "FETCH", "ROW_COUNT", "ORDER BY"))
    assert all(f'"{column.name}"' in query for column in postgres_fixture_columns())


@pytest.mark.parametrize("rows", [0, 1, 2, 3])
def test_complete_snapshot_vectors_cover_empty_and_disappeared_keys(rows: int) -> None:
    from dpone.adapters.nonproduction_postgres_fixture_rows import postgres_snapshot_rows

    snapshot = postgres_snapshot_rows(row_count=rows)
    assert tuple(row[0] for row in snapshot) == tuple(str(key) for key in range(1, rows + 1))
    assert all(len(row) == 128 for row in snapshot)
    if rows == 2:
        assert snapshot == postgres_seed_rows("initial")


@pytest.mark.parametrize("rows", [-1, 4, True, "2", 2.0, None])
def test_snapshot_vectors_reject_unmodeled_size(rows: Any) -> None:
    from dpone.adapters.nonproduction_postgres_fixture_rows import postgres_snapshot_rows

    with pytest.raises(NonproductionAuthorityError, match="fixture_recipe_rows$"):
        postgres_snapshot_rows(row_count=rows)


@pytest.mark.parametrize("value", [None, "", "\t\r\n\x1d\x1f", '"' * 100, "😀Ω" * 8192, "\\x" + "ab" * 8192])
def test_utf8_bound_covers_default_codec_and_csv_framing(value) -> None:
    import csv
    import io

    from dpone.runtime.connectors.bulk_text_codec import BulkTextCodec

    # Independent serialization rather than treating a fixture's intended row
    # count as measured bytes. SQL execution/snapshot continuity is live scope.
    raw_bytes = len(value.encode("utf-8")) if value is not None else 0
    bound = 4 * raw_bytes + 64
    for field in (value, BulkTextCodec().encode(value) if value is not None else None):
        output = io.StringIO(newline="")
        csv.writer(output, quoting=csv.QUOTE_ALL, lineterminator="\n").writerow([field])
        assert len(output.getvalue().encode("utf-8")) <= bound
