"""Bounded source consumption and pre-dispatch encoding; no live SQL proof."""

from decimal import Decimal
from types import SimpleNamespace

import pytest

from dpone.app.composition_clickhouse_source import MssqlClickHouseSourceReader, bounded_clickhouse_rows
from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_clickhouse_dispatch import ClickHouseDispatchColumn as Column
from dpone.contracts.composition_snapshot import SnapshotLimits
from dpone.runtime.clickhouse_native import ClickHouseNativeEncoder

COLUMNS = (Column("id", "Int32"), Column("name", "Nullable(String)"))


def limits(*, rows=10, source=4096, wire=4096):
    return SnapshotLimits(rows, source, wire, 4096, 4096, 8192)


class Cursor:
    def __init__(self, rows):
        self.rows = iter(rows)
        self.schema = iter([("id", "int", "NO", 10, 0), ("name", "nvarchar", "YES", None, None)])
        self.closed = False
        self.calls = []
        self.data_reads = 0
        self.statements = []
        self.transaction_ids = iter([7, 7])
        self.in_transaction = False

    def execute(self, sql, *params):
        self.statements.append(sql)
        self.in_transaction = "CURRENT_TRANSACTION_ID()" in sql
        if self.in_transaction:
            self.transaction = iter([(1, 1, next(self.transaction_ids))])
        self.in_schema = "INFORMATION_SCHEMA" in sql
        if self.in_schema:
            self.schema = iter([("id", "int", "NO", 10, 0), ("name", "nvarchar", "YES", None, None)])

    def fetchmany(self, count):
        self.calls.append(count)
        if not self.in_schema and not self.in_transaction:
            self.data_reads += 1
        values = self.transaction if self.in_transaction else self.schema if self.in_schema else self.rows
        value = next(values, None)
        return [] if value is None else [value]

    def fetchall(self):
        raise AssertionError("unbounded fetch forbidden")

    def close(self):
        self.closed = True


def reader(rows, budget):
    cursor = Cursor(rows)
    connection = SimpleNamespace(cursor=lambda: cursor, closed=False, commit=lambda: None, rollback=lambda: None)
    connection.close = lambda: setattr(connection, "closed", True)
    return (
        MssqlClickHouseSourceReader(
            open_connection=lambda: connection,
            table={"database": "db", "schema": "dbo", "name": "data"},
            columns=COLUMNS,
            limits=budget,
        ),
        cursor,
        connection,
    )


def test_source_fetchmany_and_server_projection_bound_giant_values():
    source, cursor, connection = reader([(2**50, None, None), (6, 1, "ok")], limits())
    with pytest.raises(CompositionAdmissionError, match="clickhouse_source_budget"):
        source(None)
    assert cursor.data_reads == 1
    assert set(cursor.calls) == {1}
    assert "TOP (11)" in next(sql for sql in cursor.statements if "__dpone_source_bytes" in sql)
    assert "DATALENGTH" in next(
        sql for sql in cursor.statements if "__dpone_source_bytes" in sql
    ) and "CASE WHEN" in next(sql for sql in cursor.statements if "__dpone_source_bytes" in sql)
    assert cursor.closed and connection.closed


@pytest.mark.parametrize("rows,expected", [([], ()), ([(6, 1, "ok"), (4, 2, None)], ((1, "ok"), (2, None)))])
def test_source_success_empty_and_cleanup(rows, expected):
    source, cursor, connection = reader(rows, limits())
    assert source(None) == expected
    assert cursor.closed and connection.closed


def test_source_stops_on_count_budget_and_closes():
    source, cursor, connection = reader([(4, i, None) for i in range(100)], limits(rows=2))
    with pytest.raises(CompositionAdmissionError):
        source(None)
    assert cursor.data_reads == 3
    assert cursor.closed and connection.closed


@pytest.mark.parametrize("size", [1, 127, 128, 65536, 65537])
def test_wire_accounting_matches_unchanged_native_bytes_at_block_boundaries(size):
    columns = (Column("id", "Int32"),)
    rows = [(i,) for i in range(size)]
    schema = tuple((c.name, c.type_name) for c in columns)
    payload = b"".join(ClickHouseNativeEncoder(schema, target_schema=schema).iter_batches(rows))
    budget = limits(rows=size, source=10000000, wire=len(payload))
    assert tuple(bounded_clickhouse_rows(rows, columns, budget)) == tuple(rows)
    with pytest.raises(CompositionAdmissionError):
        tuple(bounded_clickhouse_rows(rows, columns, limits(rows=size, source=10000000, wire=len(payload) - 1)))


def test_decimal_and_null_native_wire_width_is_exact():
    columns = (Column("amount", "Nullable(Decimal(38, 9))"),)
    rows = [(None,), (Decimal("12345678901234567890123456789.123456789"),)]
    schema = tuple((c.name, c.type_name) for c in columns)
    payload = b"".join(ClickHouseNativeEncoder(schema, target_schema=schema).iter_batches(rows))
    assert tuple(bounded_clickhouse_rows(rows, columns, limits(wire=len(payload)))) == tuple(rows)
    with pytest.raises(CompositionAdmissionError):
        tuple(bounded_clickhouse_rows(rows, columns, limits(wire=len(payload) - 1)))


def test_source_iterator_is_closed_and_not_consumed_after_first_oversize():
    visits = []

    def rows():
        try:
            for i in range(100):
                visits.append(i)
                yield (i, "x" * 1000)
        finally:
            visits.append("closed")

    with pytest.raises(CompositionAdmissionError):
        tuple(bounded_clickhouse_rows(rows(), COLUMNS, limits(source=100)))
    assert visits == [0, "closed"]


def test_discovery_and_identity_checks_share_the_snapshot_connection():
    source, cursor, connection = reader([(6, 1, "ok")], limits())
    source._columns = ()
    calls = []
    source._require_source = lambda current: calls.append(current) or b"service/database-original"
    assert source.read_snapshot() == (COLUMNS, ((1, "ok"),))
    assert calls == [connection, connection]
    assert source.source_identity_original == b"service/database-original"
    assert sum("INFORMATION_SCHEMA" in sql for sql in cursor.statements) == 2
    assert all("[db].INFORMATION_SCHEMA.COLUMNS" in sql for sql in cursor.statements if "INFORMATION_SCHEMA" in sql)
    assert any("SERIALIZABLE" in sql for sql in cursor.statements)
    assert any("TABLOCK,HOLDLOCK" in sql for sql in cursor.statements)


@pytest.mark.parametrize("originals", [(b"before", b"after"), (b"",), (None,), (b"x" * 16385,)])
def test_source_identity_failure_rolls_back_and_closes(originals):
    source, cursor, connection = reader([(6, 1, "ok")], limits())
    values = iter(originals)
    source._require_source = lambda current: next(values)
    events = []
    connection.commit = lambda: events.append("commit")
    connection.rollback = lambda: events.append("rollback")
    with pytest.raises(CompositionAdmissionError, match="clickhouse_source_identity"):
        source.read_snapshot()
    assert events == ["rollback"]
    assert cursor.closed and connection.closed
    if len(originals) == 1:
        assert cursor.data_reads == 0


def test_source_uses_driver_transaction_without_nested_begin():
    source, cursor, _ = reader([], limits())
    source.read_snapshot()
    assert all("BEGIN TRANSACTION" not in sql for sql in cursor.statements)


def test_source_transaction_replacement_rejects_even_with_stable_identity():
    source, cursor, connection = reader([(6, 1, "ok")], limits())
    cursor.transaction_ids = iter([7, 8])
    source._require_source = lambda _: b"unchanged service/database"
    events = []
    connection.commit = lambda: events.append("commit")
    connection.rollback = lambda: events.append("rollback")
    with pytest.raises(CompositionAdmissionError, match="clickhouse_source_transaction"):
        source.read_snapshot()
    assert events == ["rollback"]
    assert cursor.closed and connection.closed
