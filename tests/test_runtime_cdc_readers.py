from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from dpone.readiness.cdc import CDCBackend, CDCConfig, CDCOffset, build_mssql_change_tracking_enable_sql
from dpone.runtime.cdc import (
    CDCOperation,
    MSSQLCDCReader,
    MSSQLChangeTrackingReader,
    MSSQLChangeTrackingReaderConfig,
    MSSQLTableCDCReaderConfig,
    PgOutputMessageParser,
    PostgresLogicalCDCReader,
    PostgresLogicalCDCReaderConfig,
    TestDecodingMessageParser,
)


@dataclass
class FakePostgresConnector:
    responses: list[list[dict[str, Any]]] = field(default_factory=list)
    calls: list[tuple[str, tuple[Any, ...] | None, bool]] = field(default_factory=list)

    def get_records(self, query, params=None, as_dict=False):
        self.calls.append((str(query), tuple(params) if params is not None else None, as_dict))
        return self.responses.pop(0) if self.responses else []

    def execute_query(self, query, params=None):
        self.calls.append((str(query), tuple(params) if params is not None else None, False))
        return 1


@dataclass
class FakeMSSQLConnector:
    responses: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    calls: list[tuple[str, tuple[Any, ...] | None, bool]] = field(default_factory=list)

    def quote_identifier(self, name: str) -> str:
        return "[" + name.replace("]", "]]") + "]"

    def qualified_name(self, schema: str, table: str) -> str:
        return f"{self.quote_identifier(schema)}.{self.quote_identifier(table)}"

    def get_records(self, query, params=None, as_dict=False):
        sql = str(query)
        self.calls.append((sql, tuple(params) if params is not None else None, as_dict))
        for marker, rows in list(self.responses.items()):
            if marker in sql:
                return rows
        return []

    def execute_query(self, query, params=None):
        self.calls.append((str(query), tuple(params) if params is not None else None, False))
        return 1


def test_test_decoding_parser_maps_insert_update_delete_and_values() -> None:
    parser = TestDecodingMessageParser(source_schema="public", source_table="orders")

    insert = parser.parse(
        "0/1",
        "900",
        "table public.orders: INSERT: id[integer]:1 name[text]:'alpha beta' amount[numeric]:10.50 note[text]:null",
    )
    update = parser.parse(
        "0/2",
        "900",
        "table public.orders: UPDATE: old-key: id[integer]:1 new-tuple: id[integer]:1 name[text]:'bravo' active[boolean]:true",
    )
    delete = parser.parse("0/3", "901", "table public.orders: DELETE: id[integer]:1")
    ignored = parser.parse("0/4", "901", "table public.other: INSERT: id[integer]:1")

    assert insert is not None
    assert insert.operation == CDCOperation.INSERT
    assert insert.data == {"id": 1, "name": "alpha beta", "amount": 10.5, "note": None}
    assert update is not None
    assert update.operation == CDCOperation.UPDATE
    assert update.before == {"id": 1}
    assert update.data == {"id": 1, "name": "bravo", "active": True}
    assert delete is not None
    assert delete.operation == CDCOperation.DELETE
    assert ignored is None


def _cstring(value: str) -> bytes:
    return value.encode("utf-8") + b"\x00"


def _u16(value: int) -> bytes:
    return value.to_bytes(2, "big")


def _u32(value: int) -> bytes:
    return value.to_bytes(4, "big")


def _i32(value: int) -> bytes:
    return value.to_bytes(4, "big", signed=True)


def _pgoutput_relation() -> bytes:
    columns = [
        (1, "id", 23, -1),
        (0, "name", 25, -1),
        (0, "amount", 1700, -1),
    ]
    payload = b"R" + _u32(42) + _cstring("public") + _cstring("orders") + b"d" + _u16(len(columns))
    for flags, name, oid, modifier in columns:
        payload += bytes([flags]) + _cstring(name) + _u32(oid) + _i32(modifier)
    return payload


def _pgoutput_tuple(values: list[str | None]) -> bytes:
    payload = _u16(len(values))
    for value in values:
        if value is None:
            payload += b"n"
        else:
            raw = value.encode("utf-8")
            payload += b"t" + _u32(len(raw)) + raw
    return payload


def test_pgoutput_parser_maps_binary_relation_insert_update_delete() -> None:
    parser = PgOutputMessageParser(source_schema="public", source_table="orders")

    assert parser.parse("0/1", "900", _pgoutput_relation()) is None
    insert = parser.parse("0/2", "900", b"I" + _u32(42) + b"N" + _pgoutput_tuple(["1", "alpha", "10.50"]))
    update = parser.parse(
        "0/3",
        "901",
        b"U" + _u32(42) + b"K" + _pgoutput_tuple(["1", None, None]) + b"N" + _pgoutput_tuple(["1", "beta", "11.50"]),
    )
    delete = parser.parse("0/4", "902", b"D" + _u32(42) + b"K" + _pgoutput_tuple(["1", None, None]))

    assert insert is not None
    assert insert.operation == CDCOperation.INSERT
    assert insert.data == {"id": 1, "name": "alpha", "amount": 10.5}
    assert update is not None
    assert update.operation == CDCOperation.UPDATE
    assert update.before == {"id": 1, "name": None, "amount": None}
    assert update.data == {"id": 1, "name": "beta", "amount": 11.5}
    assert delete is not None
    assert delete.operation == CDCOperation.DELETE
    assert delete.data["id"] == 1


def test_postgres_logical_reader_consumes_slot_and_returns_cdc_batch() -> None:
    connector = FakePostgresConnector(
        responses=[
            [
                {"lsn": "0/1", "xid": "900", "data": "BEGIN 900"},
                {"lsn": "0/2", "xid": "900", "data": "table public.orders: INSERT: id[integer]:1 name[text]:'alpha'"},
                {
                    "lsn": "0/3",
                    "xid": "900",
                    "data": "table public.orders: UPDATE: old-key: id[integer]:1 new-tuple: id[integer]:1 name[text]:'beta'",
                },
                {"lsn": "0/4", "xid": "900", "data": "COMMIT 900"},
                {"lsn": "0/5", "xid": "901", "data": "table public.other: INSERT: id[integer]:999"},
            ]
        ]
    )
    reader = PostgresLogicalCDCReader(
        connector,
        PostgresLogicalCDCReaderConfig(
            source_schema="public", source_table="orders", slot_name="dpone_orders", plugin="test_decoding"
        ),
    )

    batch = reader.read_batch(max_changes=100)

    assert [change.operation for change in batch.changes] == [CDCOperation.INSERT, CDCOperation.UPDATE]
    assert batch.high_watermark == "0/5"
    assert batch.next_offset == CDCOffset(backend=CDCBackend.POSTGRES_LOGICAL, token="0/5", snapshot_complete=True)
    rows = batch.to_rows()
    assert rows[0]["_dpone_cdc_operation"] == "insert"
    assert rows[0]["_dpone_cdc_position"] == "0/2"
    assert rows[1]["name"] == "beta"
    assert any("pg_logical_slot_get_changes" in query for query, *_ in connector.calls)


def test_mssql_cdc_reader_maps_all_changes_and_skips_update_before_rows() -> None:
    connector = FakeMSSQLConnector(
        responses={
            "INFORMATION_SCHEMA.COLUMNS": [{"COLUMN_NAME": "id"}, {"COLUMN_NAME": "name"}],
            "fn_cdc_get_all_changes_dbo_orders": [
                {"__dpone__lsn": "0x00000000000000000001", "__dpone__operation": 2, "id": 1, "name": "alpha"},
                {"__dpone__lsn": "0x00000000000000000002", "__dpone__operation": 3, "id": 1, "name": "alpha"},
                {"__dpone__lsn": "0x00000000000000000002", "__dpone__operation": 4, "id": 1, "name": "beta"},
                {"__dpone__lsn": "0x00000000000000000003", "__dpone__operation": 1, "id": 1, "name": "beta"},
            ],
        }
    )
    reader = MSSQLCDCReader(
        connector,
        MSSQLTableCDCReaderConfig(source_schema="dbo", source_table="orders", capture_instance="dbo_orders"),
    )

    batch = reader.read_batch(
        start_offset=CDCOffset(backend=CDCBackend.MSSQL_CDC, token="0x00000000000000000000"),
        max_changes=100,
    )

    assert [change.operation for change in batch.changes] == [
        CDCOperation.INSERT,
        CDCOperation.UPDATE,
        CDCOperation.DELETE,
    ]
    assert batch.next_offset == CDCOffset(
        backend=CDCBackend.MSSQL_CDC, token="0x00000000000000000003", snapshot_complete=True
    )
    assert batch.to_rows()[-1]["_dpone_cdc_deleted"] is True
    assert any("fn_cdc_get_all_changes_dbo_orders" in query for query, *_ in connector.calls)


def test_mssql_change_tracking_reader_uses_current_version_as_offset() -> None:
    connector = FakeMSSQLConnector(
        responses={
            "is_primary_key = 1": [{"name": "id"}],
            "CHANGE_TRACKING_CURRENT_VERSION": [{"version": 42}],
            "CHANGETABLE": [
                {"__dpone__version": 41, "__dpone__operation": "I", "id": 1, "name": "alpha"},
                {"__dpone__version": 42, "__dpone__operation": "D", "id": 2, "name": None},
            ],
        }
    )
    reader = MSSQLChangeTrackingReader(
        connector,
        MSSQLChangeTrackingReaderConfig(source_schema="dbo", source_table="orders"),
    )

    batch = reader.read_batch(start_offset=CDCOffset(backend=CDCBackend.MSSQL_CHANGE_TRACKING, token="40"))

    assert [change.operation for change in batch.changes] == [CDCOperation.INSERT, CDCOperation.DELETE]
    assert batch.next_offset == CDCOffset(backend=CDCBackend.MSSQL_CHANGE_TRACKING, token="42", snapshot_complete=True)
    assert batch.to_rows()[1]["_dpone_cdc_deleted"] is True
    assert any("CHANGETABLE(CHANGES [dbo].[orders]" in query for query, *_ in connector.calls)


def test_cdc_offset_storage_round_trips_mssql_and_postgres() -> None:
    from dpone.runtime.state.cdc import MSSQLCDCOffsetStorage, PostgresCDCOffsetStorage

    mssql = FakeMSSQLConnector(
        responses={
            "SELECT TOP (1) backend": [
                {"backend": "mssql_cdc", "token": "0x00000000000000000003", "snapshot_complete": 1}
            ]
        }
    )
    mssql_storage = MSSQLCDCOffsetStorage(mssql, schema="state", table="cdc_offset")
    mssql_offset = CDCOffset(backend=CDCBackend.MSSQL_CDC, token="0x00000000000000000003", snapshot_complete=True)

    mssql_storage.save_offset("orders-pipe", "dbo", "orders", mssql_offset)
    assert mssql_storage.load_offset("orders-pipe", "dbo", "orders", CDCBackend.MSSQL_CDC) == mssql_offset
    assert any("MERGE [state].[cdc_offset]" in query for query, *_ in mssql.calls)

    postgres = FakePostgresConnector(
        responses=[[{"backend": "postgres_logical", "token": "0/5", "snapshot_complete": True}]]
    )
    pg_storage = PostgresCDCOffsetStorage(postgres, schema="state", table="cdc_offset")
    pg_offset = CDCOffset(backend=CDCBackend.POSTGRES_LOGICAL, token="0/5", snapshot_complete=True)

    pg_storage.save_offset("orders-pipe", "public", "orders", pg_offset)
    assert pg_storage.load_offset("orders-pipe", "public", "orders", CDCBackend.POSTGRES_LOGICAL) == pg_offset


def test_cdc_readiness_change_tracking_sql_and_lazy_exports() -> None:
    import dpone

    config = CDCConfig(
        backend=CDCBackend.MSSQL_CHANGE_TRACKING,
        source_schema="dbo",
        source_table="orders",
    )

    sql = build_mssql_change_tracking_enable_sql(config)

    assert "CHANGE_TRACKING = ON" in sql
    assert "ALTER TABLE [dbo].[orders] ENABLE CHANGE_TRACKING" in sql
    assert dpone.MSSQLCDCReader is MSSQLCDCReader
    assert dpone.PostgresLogicalCDCReader is PostgresLogicalCDCReader
