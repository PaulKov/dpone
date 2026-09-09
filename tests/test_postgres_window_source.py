"""Synthetic connection, cancellation and identity contracts for snapshots."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from dpone.contracts.bounded_window import WindowPlan
from dpone.contracts.process_errors import WindowContractError
from dpone.runtime.sources.postgres_window_source import PostgresWindowSource


class Cursor:
    def __init__(self, connection, name=None):
        self.connection = connection
        self.name = name
        self.query = ""
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.closed = True

    def execute(self, query, parameters=None):
        if self.connection.closed:
            raise RuntimeError("disconnected")
        self.query = query
        self.connection.calls.append((query, parameters))

    def fetchone(self):
        return (self.connection.transaction, "00000001-00000002-1")

    def fetchall(self):
        return [("id", 23, -1, True, 0, "r"), ("at", 1184, -1, False, 0, "r")]

    def fetchmany(self, size):
        result = self.connection.rows[:size]
        self.connection.rows = self.connection.rows[size:]
        return result


class Connection:
    def __init__(self):
        self.closed = False
        self.transaction = 1
        self.calls = []
        self.rows = [(1,), (2,)]
        self.cursors = []

    def cursor(self, name=None):
        result = Cursor(self, name)
        self.cursors.append(result)
        return result

    def close(self):
        self.closed = True


def source(factory=Connection, **kwargs):
    return PostgresWindowSource(
        connection_factory=factory,
        schema_name="public",
        table_name="synthetic",
        columns=("id",),
        window_column="at",
        schema_fingerprint="target",
        **kwargs,
    )


def plan(reader):
    start = datetime(2026, 1, 1, tzinfo=UTC)
    end = start + timedelta(days=1)
    return WindowPlan(
        "route",
        "target",
        start,
        end,
        (start, end),
        reader.schema_fingerprint,
        reader.source_version,
        reader.parameters_fingerprint,
        window_column="at",
    )


@pytest.mark.parametrize("field", ["source_version", "schema_fingerprint", "parameters_fingerprint", "window_column"])
def test_mismatched_identity_never_reads(field):
    with source() as reader:
        frozen = replace(plan(reader), **{field: "mismatch"})
        with pytest.raises(WindowContractError, match="identity"):
            list(reader.read(frozen, frozen.chunks[0]))


def test_workers_are_independent_and_cancel_closes():
    connections = []

    def factory():
        connections.append(Connection())
        return connections[-1]

    with source(factory, batch_rows=1) as reader:
        frozen = plan(reader)
        rows = reader.read(frozen, frozen.chunks[0])
        assert next(rows) == (1,)
        assert len(connections) == 2
        assert not connections[0].closed
        rows.close()
        assert connections[1].closed
        assert all(cursor.closed for cursor in connections[1].cursors)
        query, parameters = connections[1].calls[-1]
        assert '"at" >= %s AND "at" < %s' in query
        assert parameters == (frozen.start, frozen.end)
    assert connections[0].closed


def test_expired_context_does_not_emit_buffered_rows():
    with source(batch_rows=2) as reader:
        frozen = plan(reader)
        rows = reader.read(frozen, frozen.chunks[0])
        assert next(rows) == (1,)
    with pytest.raises(WindowContractError, match="expired"):
        next(rows)


def test_keeper_restarted_transaction_rejects_reads():
    connection = Connection()
    with source(lambda: connection) as reader:
        frozen = plan(reader)
        connection.transaction = 2
        with pytest.raises(WindowContractError, match="unavailable"):
            reader.validate(frozen)


def test_new_context_never_reuses_source_version():
    reader = source()
    with reader:
        old = plan(reader)
    with reader:
        assert old.source_version != reader.source_version
        with pytest.raises(WindowContractError, match="identity"):
            reader.validate(old)


def test_shared_worker_connection_is_rejected():
    connection = Connection()
    with source(lambda: connection) as reader:
        frozen = plan(reader)
        with pytest.raises(WindowContractError, match="independent"):
            list(reader.read(frozen, frozen.chunks[0]))
        assert not connection.closed


@pytest.mark.parametrize("size", [0, -1, True, 1.5])
def test_invalid_batch_size(size):
    with pytest.raises(WindowContractError):
        source(batch_rows=size)


def test_worker_schema_drift_closes_connection(monkeypatch):
    with source() as reader:
        frozen = plan(reader)
        monkeypatch.setattr(
            Cursor, "fetchall", lambda _: [("id", 20, -1, True, 0, "r"), ("at", 1184, -1, False, 0, "r")]
        )
        with pytest.raises(WindowContractError, match="schema drift"):
            list(reader.read(frozen, frozen.chunks[0]))


def test_unknown_chunk_rejected():
    with source() as reader:
        frozen = plan(reader)
        chunk = replace(frozen.chunks[0], chunk_id="unknown")
        with pytest.raises(WindowContractError, match="frozen plan"):
            list(reader.read(frozen, chunk))


def test_missing_column_closes_keeper(monkeypatch):
    connection = Connection()
    monkeypatch.setattr(Cursor, "fetchall", lambda _: [])
    with pytest.raises(WindowContractError, match="unavailable"):
        with source(lambda: connection):
            pass
    assert connection.closed


def test_wrong_window_type_rejected(monkeypatch):
    monkeypatch.setattr(Cursor, "fetchall", lambda _: [("id", 23, -1, True, 0, "r"), ("at", 1114, -1, False, 0, "r")])
    with pytest.raises(WindowContractError, match="time zone"):
        with source():
            pass


def test_identifier_payload_stays_quoted():
    connection = Connection()
    with PostgresWindowSource(
        connection_factory=lambda: connection,
        schema_name='a"; SELECT 1; --',
        table_name="test",
        columns=("id",),
        window_column="at",
        schema_fingerprint="target",
    ):
        assert connection.calls[1][0] == 'LOCK TABLE "a""; SELECT 1; --"."test" IN ACCESS SHARE MODE'


def test_row_shape_mismatch_closes_worker():
    connections = []

    def factory():
        connection = Connection()
        connection.rows = [(1, 2)]
        connections.append(connection)
        return connection

    with source(factory) as reader:
        frozen = plan(reader)
        with pytest.raises(WindowContractError, match="shape"):
            list(reader.read(frozen, frozen.chunks[0]))
        assert connections[-1].closed
