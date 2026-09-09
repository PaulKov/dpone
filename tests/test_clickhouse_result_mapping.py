"""Unit contracts for ClickHouse result → dict mapping (silent-NULL guard)."""

from __future__ import annotations

import pytest

from dpone.ports.sql_result_probe import header_probe_sql
from dpone.runtime.connectors.clickhouse_result_mapping import (
    column_names_from_types,
    row_as_dict,
    rows_as_dicts,
)


def test_header_probe_sql_wraps_query_without_consuming_rows() -> None:
    assert header_probe_sql("SELECT id, name FROM db.t") == ("SELECT * FROM (SELECT id, name FROM db.t) AS sub LIMIT 0")


def test_column_names_from_types_reads_metadata_when_rows_empty() -> None:
    assert column_names_from_types([("id", "UInt64"), ("name", "String")]) == ["id", "name"]


def test_column_names_from_types_fail_closed_when_metadata_missing() -> None:
    with pytest.raises(RuntimeError, match="could not resolve column names"):
        column_names_from_types([])


def test_row_as_dict_maps_values_by_position() -> None:
    assert row_as_dict(["id", "name"], (1, "a")) == {"id": 1, "name": "a"}


def test_row_as_dict_fail_closed_on_arity_mismatch() -> None:
    with pytest.raises(RuntimeError, match="row arity mismatch"):
        row_as_dict(["id", "name"], (1,))


def test_rows_as_dicts_preserves_order() -> None:
    assert rows_as_dicts(["id", "name"], [(1, "a"), (2, "b")]) == [
        {"id": 1, "name": "a"},
        {"id": 2, "name": "b"},
    ]
