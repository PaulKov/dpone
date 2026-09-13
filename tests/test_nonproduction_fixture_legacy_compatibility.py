"""Frozen pre-extraction API/text vectors; recording connectors do not run SQL.

Expectations were captured at 9974 before any producer edit. Full inputs, column
values, SQL and events: /tmp/dpone-v3-finite-fixtures-pre-edit-baseline.json,
SHA256 559d5c292d3c2cf0b0337acaee2d119dc83a431a2122b90774db010154c4c5a8.
"""

import hashlib
import json
import sys
from collections.abc import Callable
from dataclasses import asdict
from types import SimpleNamespace
from typing import Any

import pytest

from tests.integration.postgres import postgres_mssql_wide_fixtures as pg
from tests.test_tools_mssql_clickhouse_bcp_native_type_certification import _load_tool_module


def encoded_sha(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


class RecordingConnector:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def execute_query(self, query: str) -> None:
        self.queries.append(query)


@pytest.mark.parametrize(
    ("count", "length", "expected"),
    [
        (-30, 0, "4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945"),
        (-1, 28, "505163f735c2815d2163fe66a09f07b2b5cc286ab35c68977430bf0a95f1fd8a"),
        (0, 0, "4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945"),
        (False, 0, "4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945"),
        (True, 1, "fef460f22453e267a396eea82241d72a07f0dc88a248632d48c7e67d42c0b8c7"),
        (1, 1, "fef460f22453e267a396eea82241d72a07f0dc88a248632d48c7e67d42c0b8c7"),
        (29, 29, "1871643d0b331d135da60071b49b6864881e8d03826901d0ef47a3325c2d0c06"),
        (30, 30, "ae62a55658d98950e797cfbd6c05340e3b3cd39278292c6b55872bf0a4f3c29c"),
        (200, 200, "4a6f848c9c8688de3417014951b168c4948197db9d23b921c05f35504d5eadcb"),
        (202, 202, "d333f6406514a8b50f579bb2301d990ada4cdc6008dc5ffb52ed375b338955a3"),
    ],
)
def test_legacy_bcp_all_values_and_exact_old_class(count: int, length: int, expected: str) -> None:
    module = _load_tool_module()
    columns = module.build_bcp_native_columns(count)
    assert type(columns) is list and len(columns) == length
    assert all(type(column) is module.base.WideTypeColumn for column in columns)
    assert sys.modules[module.build_bcp_native_columns.__module__].__all__ == ["build_bcp_native_columns"]
    assert encoded_sha([asdict(column) for column in columns]) == expected
    assert module.build_bcp_native_columns(count) is not columns


@pytest.mark.parametrize("count", [1.0, 30.0, "202", None])
def test_legacy_bcp_count_errors_remain_builtin(count: Any) -> None:
    with pytest.raises(TypeError):
        _load_tool_module().build_bcp_native_columns(count)


@pytest.mark.parametrize(
    ("rows", "expected"),
    [
        (0, "0d47fb70aa34b9f258e092cebfa9b53a58a7b083d4a9945968bd231dd1107126"),
        (1, "3244f15f5c0d75f363918f29fd391a46cff6c8e9b69ec8b19ecde1587d586e7c"),
        (2, "300ebd6d0b478e134baf16dff5d92338fee7b8f6743da9f3647423cef07f6d4a"),
        (7, "19c0282f30db97f744f03799d0ebf2a67b11a60d71e9c90d92231534e115ea27"),
        (11, "081543427d57d2b6c33e7d2faf7dbe95bf7199f32a0edd2c035a8b65ac24f476"),
        (2147, "5c31d0b24cc0de4959a9f0c1698931594cf940a53a65872302cc259ccba0cbda"),
        (2148, "b2016af5eebfa00efc70a0656b5834dbd8f038331e319409b6ff93900d119322"),
        (99999, "bbf17ef7ec4bc7effee663a4d045d85947ec21742da553655487121d648112ac"),
        (100000, "293fa3829a26a4c536be9f1c13f0e4ff88c09a799c081ae402baabe7b78c8153"),
    ],
)
def test_all_legacy_bcp_statements_and_events(rows: int, expected: str) -> None:
    module = _load_tool_module()
    connector = RecordingConnector()
    events: list[list[Any]] = []
    config = SimpleNamespace(source_schema="fixture_schema", source_table="fixture_table", column_count=202, rows=rows)
    module._prepare_source(connector, config, emit_event=lambda event, payload: events.append([event, payload]))
    assert len(connector.queries) == 4 and len(events) == 2
    assert encoded_sha({"queries": connector.queries, "events": events}) == expected


def test_legacy_pg_classes_raw_inventory_mapper_and_exports() -> None:
    columns = pg.wide_columns()
    assert type(columns) is list and len(columns) == 128
    assert all(type(column) is pg.WideColumn for column in columns)
    assert (
        encoded_sha([asdict(column) for column in columns])
        == "0d3711390f63547b77042520be7b4d9d6b50aebfef6ac9ebe8c9463b0abc4314"
    )
    assert encoded_sha(pg._SPECS) == "85a20821a9cc225f59e27ac152c9b64a069a59ca288c16e54931822414eded6b"
    assert (pg.SOURCE_SCHEMA, pg.WIDE_TABLE, pg.WIDE_COLUMN_TARGET) == ("dpone_src", "postgres_to_mssql_wide", 128)
    assert pg.__all__ == [
        "SOURCE_SCHEMA",
        "WIDE_TABLE",
        "WIDE_COLUMN_TARGET",
        "WideColumn",
        "create_wide_postgres_table",
        "insert_wide_watermark_row",
        "wide_columns",
    ]
    assert pg.wide_columns() is not columns


@pytest.mark.parametrize(
    ("schema", "table", "expected"),
    [
        ("dpone_src", "postgres_to_mssql_wide", "c7e1c109ed4966b22ebc22cdbbed38291b968bea98d949a6c33d650dc24dde6d"),
        ("fixture_schema", "fixture_table", "d52d8c197053c552b29191a49fec80ad2709d7f542b619fdac04f2687401bf17"),
    ],
)
def test_all_legacy_pg_initial_statement_bytes(schema: str, table: str, expected: str) -> None:
    connector = RecordingConnector()
    columns = pg.create_wide_postgres_table(connector, schema=schema, table=table)
    assert type(columns) is list and len(connector.queries) == 5
    assert (
        encoded_sha({"queries": connector.queries, "returned_columns": [asdict(column) for column in columns]})
        == expected
    )


@pytest.mark.parametrize(
    ("row_id", "expected"),
    [
        (3, "c7b15ee03c710a27920de203709c08281a49c0ae7c8d4cca542faae43c85a953"),
        (7, "e1365e1d0e4cad2148e6907b158b733acda5e770f8c7a05f2efae7ba295218dc"),
        ("key", "f2f11479879e2349b79328243e3692fdb39e2add970ed1e398c25209e418043b"),
        (False, "5a6fc7c81c0502046d70fbb062f59bbe68ad6b5178379f3414ced4f585b26739"),
    ],
)
def test_legacy_watermark_conversion_and_all_statement_bytes(row_id: Any, expected: str) -> None:
    connector = RecordingConnector()
    invoke: Callable[..., object] = pg.insert_wide_watermark_row
    assert invoke(connector, schema="fixture_schema", table="fixture_table", row_id=row_id) is None
    assert len(connector.queries) == 1
    assert encoded_sha(connector.queries) == expected


@pytest.mark.parametrize("operation", ["initial", "watermark", "mapper"])
def test_original_connector_and_mapper_exceptions_propagate(operation: str, monkeypatch: pytest.MonkeyPatch) -> None:
    failure = RuntimeError("synthetic failure")

    class FailingConnector:
        def execute_query(self, query: str) -> None:
            raise failure

    def fail_mapper() -> None:
        raise failure

    if operation == "mapper":
        monkeypatch.setattr(pg, "PostgresMssqlTypeMapper", fail_mapper)
    with pytest.raises(RuntimeError) as caught:
        if operation == "mapper":
            pg.create_wide_postgres_table(RecordingConnector())
        elif operation == "initial":
            pg.create_wide_postgres_table(FailingConnector())
        else:
            pg.insert_wide_watermark_row(FailingConnector())
    assert caught.value is failure


@pytest.mark.parametrize(
    "count,expected",
    [
        (-1, "505163f735c2815d2163fe66a09f07b2b5cc286ab35c68977430bf0a95f1fd8a"),
        (1, "fef460f22453e267a396eea82241d72a07f0dc88a248632d48c7e67d42c0b8c7"),
        (30, "ae62a55658d98950e797cfbd6c05340e3b3cd39278292c6b55872bf0a4f3c29c"),
        (202, "d333f6406514a8b50f579bb2301d990ada4cdc6008dc5ffb52ed375b338955a3"),
    ],
)
def test_legacy_count_preserves_integer_protocols(count: int, expected: str) -> None:
    class IndexCount:
        def __le__(self, other: int) -> bool:
            return count <= other

        def __sub__(self, other: int) -> int:
            return count - other

        def __index__(self) -> int:
            return count

    columns = _load_tool_module().build_bcp_native_columns(IndexCount())
    assert encoded_sha([asdict(column) for column in columns]) == expected
