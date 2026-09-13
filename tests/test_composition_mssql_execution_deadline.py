"""Offline driver doubles prove deadline admission, not live ODBC cancellation."""

from dataclasses import replace
from types import SimpleNamespace

import pytest

from dpone.app.composition_mssql_execution_deadline import BudgetedMssqlConnectorFactory
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.runtime_connection import ResolvedBindingConnection, ResolvedConnectionDescriptor
from dpone.runtime.credentials.config import CredentialsConfig


class Clock:
    now = 0.0

    def __call__(self):
        return self.now


class Raw:
    autocommit = False
    timeout = 0

    def __init__(self, clock):
        self.clock = clock
        self.events = []
        self.delays = {}
        self.errors = {}
        self.cursors = []

    def call(self, name):
        self.events.append((name, self.timeout))
        self.clock.now += self.delays.get(name, 0)
        if name in self.errors:
            raise self.errors[name]

    def cursor(self):
        self.call("cursor")
        cursor = Cursor(self)
        self.cursors.append(cursor)
        return cursor

    def commit(self):
        self.call("commit")

    def rollback(self):
        self.call("rollback")

    def close(self):
        self.call("close")


class Cursor:
    description = (("value",),)
    rowcount = 1
    arraysize = 1

    def __init__(self, raw):
        self.raw = raw
        self.initial_timeout = raw.timeout
        self.sql = []

    def execute(self, sql, *parameters):
        self.raw.call("execute")
        self.sql.append((sql, parameters))
        return self

    def fetchone(self):
        self.raw.call("fetchone")
        return (1,)

    def fetchmany(self, size=1):
        self.raw.call("fetchmany")
        return [(1,)] * size

    def fetchall(self):
        self.raw.call("fetchall")
        return [(1,)]

    def close(self):
        self.raw.call("cursor.close")


def binding():
    return ResolvedBindingConnection(
        CredentialsConfig(host="server", database="db", username="user", password="secret"),
        {},
        ResolvedConnectionDescriptor("mssql", {}),
    )


@pytest.fixture
def setup():
    clock = Clock()
    raw = Raw(clock)
    calls = []

    def factory(resolved, *, autocommit):
        calls.append((resolved, autocommit))
        raw.call("connect")
        return SimpleNamespace(connection=raw, close=raw.close)

    build = BudgetedMssqlConnectorFactory(factory, io_deadline=lambda: 30.0, clock=clock)
    return clock, raw, calls, build


def test_lazy_connection_and_stable_cursor_preserve_dbapi(setup):
    clock, raw, calls, build = setup
    connector = build(binding(), autocommit=False)
    assert not calls
    connection = connector.connection
    assert connector.connection is connection
    cursor = connection.cursor()
    assert cursor.execute("SELECT ?", 7) is cursor
    assert cursor.fetchone() == (1,)
    assert cursor.fetchmany(2) == [(1,), (1,)]
    assert cursor.fetchall() == [(1,)]
    assert cursor.description == (("value",),)
    assert cursor.rowcount == 1
    cursor.arraysize = 3
    assert raw.cursors[0].arraysize == cursor.arraysize == 3
    connection.autocommit = False
    assert connection.autocommit is raw.autocommit is False
    clock.now = 27.4
    cursor.execute("SELECT 2")
    assert raw.events[-1] == ("execute", 2)
    assert raw.cursors[0].initial_timeout == 10
    assert len(raw.cursors) == len(calls) == 1
    assert raw.cursors[0].sql == [("SELECT ?", (7,)), ("SELECT 2", ())]
    connection.commit()
    cursor.close()
    connection.close()
    assert [name for name, _ in raw.events][-3:] == ["commit", "cursor.close", "close"]


def test_connect_normalizes_aliases_and_preserves_identity(setup):
    clock, _, calls, build = setup
    original = binding()
    original = replace(original, credentials=replace(original.credentials, additional_params={"LoginTimeout": 100}))
    clock.now = 27.2
    build(original, autocommit=False).connection
    resolved, autocommit = calls[0]
    assert resolved.descriptor == original.descriptor
    assert resolved.credentials.password == original.credentials.password
    assert resolved.credentials.database == "db"
    assert resolved.credentials.connect_timeout == 2
    assert resolved.credentials.query_timeout == original.credentials.query_timeout
    assert resolved.credentials.additional_params == {"connect_timeout": 2}
    assert original.credentials.additional_params == {"LoginTimeout": 100}
    assert autocommit is False


@pytest.mark.parametrize("operation", ["cursor", "execute", "fetchone", "fetchmany", "fetchall", "commit"])
def test_subsecond_remaining_rejects_before_driver(setup, operation):
    clock, raw, _, build = setup
    connection = build(binding(), autocommit=False).connection
    cursor = connection.cursor()
    clock.now = 29.01
    before = list(raw.events)
    with pytest.raises(CompositionAdmissionError, match="mssql_execution_deadline"):
        getattr(connection if operation in {"cursor", "commit"} else cursor, operation)(
            *(("SELECT 1",) if operation == "execute" else ())
        )
    assert raw.events == before


@pytest.mark.parametrize("operation", ["execute", "fetchone", "fetchmany", "fetchall", "commit"])
def test_late_success_is_rejected_without_retry(setup, operation):
    _, raw, _, build = setup
    connection = build(binding(), autocommit=False).connection
    cursor = connection.cursor()
    raw.delays[operation] = 31
    with pytest.raises(CompositionAdmissionError, match="mssql_execution_deadline"):
        getattr(connection if operation == "commit" else cursor, operation)(
            *(("SELECT 1",) if operation == "execute" else ())
        )
    assert sum(name == operation for name, _ in raw.events) == 1


def test_late_connection_closes_once_and_never_retries(setup):
    _, raw, calls, build = setup
    raw.delays["connect"] = 31
    connector = build(binding(), autocommit=False)
    with pytest.raises(CompositionAdmissionError):
        _ = connector.connection
    with pytest.raises(CompositionAdmissionError):
        _ = connector.connection
    assert len(calls) == 1
    assert sum(name == "close" for name, _ in raw.events) == 1


def test_expired_cleanup_still_attempts_all_closes_and_rollback(setup):
    clock, raw, _, build = setup
    connector = build(binding(), autocommit=False)
    connection = connector.connection
    cursor = connection.cursor()
    clock.now = 31
    connection.rollback()
    cursor.close()
    connector.close()
    assert [name for name, _ in raw.events][-3:] == ["rollback", "cursor.close", "close"]
    assert raw.events[-3][1] == 1


def test_driver_exception_keeps_precedence_over_late_result(setup):
    _, raw, _, build = setup
    connection = build(binding(), autocommit=False).connection
    raw.delays["commit"] = 31
    original = RuntimeError("lost acknowledgement")
    raw.errors["commit"] = original
    with pytest.raises(RuntimeError) as caught:
        connection.commit()
    assert caught.value is original


def test_source_guard_blocks_reads_during_cleanup_but_allows_close():
    clock = Clock()
    raw = Raw(clock)
    stopped = False

    def require_execution():
        if stopped:
            raise CompositionAdmissionError("source_execution_stopped")

    build = BudgetedMssqlConnectorFactory(
        lambda resolved, **kwargs: SimpleNamespace(connection=raw, close=raw.close),
        io_deadline=lambda: 60,
        clock=clock,
        require_execution=require_execution,
    )
    connection = build(binding(), autocommit=False).connection
    cursor = connection.cursor()
    stopped = True
    with pytest.raises(CompositionAdmissionError, match="source_execution_stopped"):
        cursor.fetchone()
    connection.rollback()
    connection.close()
    assert "fetchone" not in [name for name, _ in raw.events]


def test_expired_connect_never_constructs_driver(setup):
    clock, raw, calls, build = setup
    connector = build(binding(), autocommit=False)
    clock.now = 29.5
    with pytest.raises(CompositionAdmissionError):
        _ = connector.connection
    assert calls == raw.events == []


def test_late_cursor_is_closed_before_rejecting(setup):
    _, raw, _, build = setup
    connection = build(binding(), autocommit=False).connection
    raw.delays["cursor"] = 31
    with pytest.raises(CompositionAdmissionError):
        connection.cursor()
    assert raw.events[-1][0] == "cursor.close"


def test_cleanup_attempts_operation_even_when_timeout_update_fails(setup):
    clock, raw, _, build = setup
    connection = build(binding(), autocommit=False).connection
    clock.now = 31

    class TimeoutFailure(Raw):
        @property
        def timeout(self):
            return 3

        @timeout.setter
        def timeout(self, value):
            raise RuntimeError("timeout attribute unsupported")

    raw.__class__ = TimeoutFailure
    connection.rollback()
    connection.close()
    assert [name for name, _ in raw.events][-2:] == ["rollback", "close"]


def test_configured_shorter_query_timeout_is_preserved(setup):
    _, raw, _, build = setup
    original = binding()
    original = replace(original, credentials=replace(original.credentials, query_timeout=2))
    connection = build(original, autocommit=False).connection
    connection.cursor().execute("SELECT 1")
    assert raw.timeout == 2


def test_closing_unused_connector_never_opens_or_reopens(setup):
    _, raw, calls, build = setup
    connector = build(binding(), autocommit=False)
    connector.close()
    with pytest.raises(CompositionAdmissionError, match="connection_closed"):
        _ = connector.connection
    assert raw.events == calls == []


def test_non_mssql_is_rejected_without_driver_creation(setup):
    _, raw, calls, build = setup
    resolved = replace(binding(), descriptor=ResolvedConnectionDescriptor("postgres", {}))
    with pytest.raises(CompositionAdmissionError, match="mssql_execution_connector"):
        build(resolved)
    assert raw.events == calls == []


def test_new_cleanup_deadline_does_not_accept_late_execution_result(setup):
    clock, raw, _, _ = setup
    deadline = 10.0

    def current_deadline():
        return deadline

    build = BudgetedMssqlConnectorFactory(
        lambda resolved, **kwargs: SimpleNamespace(connection=raw, close=raw.close),
        io_deadline=current_deadline,
        clock=clock,
    )
    connection = build(binding(), autocommit=False).connection
    cursor = connection.cursor()
    original = raw.call

    def enter_cleanup(name):
        nonlocal deadline
        original(name)
        clock.now = 11
        deadline = 71

    raw.call = enter_cleanup
    with pytest.raises(CompositionAdmissionError, match="mssql_execution_deadline"):
        cursor.execute("SELECT 1")
    assert sum(name == "execute" for name, _ in raw.events) == 1
