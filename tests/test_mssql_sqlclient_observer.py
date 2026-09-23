"""Injected cursor and deadline fault checks, with no network or real credentials."""

import threading
from dataclasses import replace
from datetime import timedelta

import pytest

from dpone.adapters.mssql_sqlclient_observer import SqlClientObservationError, SqlClientWriterObserver
from dpone.adapters.mssql_sqlclient_observer_sql import ADMISSION_SQL, PRINCIPALS_SQL, VISIBILITY_SQL, WRITER_SQL
from tests.test_mssql_sqlclient_observation import (
    NONCE,
    admission,
    catalog_row,
    observation,
    principal_rows,
    writer_row,
)


class Cursor:
    def __init__(self):
        self.calls = []
        self.results = {
            VISIBILITY_SQL: [(16, 3, 1, 1, 7)],
            ADMISSION_SQL: [catalog_row()],
            WRITER_SQL: [writer_row()],
            PRINCIPALS_SQL: principal_rows(),
        }
        self.current = iter([])
        self.failure = False

    def execute(self, sql, *parameters):
        self.calls.append((sql, parameters))
        if self.failure:
            raise RuntimeError("Password=SECRET_CANARY;server=private")
        self.current = iter(self.results[sql])

    def fetchone(self):
        return next(self.current, None)


def observer(cursor, clock=lambda: 1):
    return SqlClientWriterObserver(cursor, admission=admission(), operation_deadline_ns=100, monotonic_ns=clock)


def test_targeted_bindings_and_preflight_before_writer():
    cursor = Cursor()
    handle = observer(cursor)
    handle.preflight()
    assert [sql for sql, _ in cursor.calls] == [VISIBILITY_SQL, ADMISSION_SQL, PRINCIPALS_SQL]
    cursor.calls.clear()
    result = handle.observe(session_id=72, nonce=NONCE)
    assert result == observation()
    assert cursor.calls == [
        (VISIBILITY_SQL, ()),
        (ADMISSION_SQL, (7, "writer")),
        (PRINCIPALS_SQL, (b"\xaa",)),
        (WRITER_SQL, (72,)),
        (PRINCIPALS_SQL, (b"\xaa",)),
    ]
    assert "WHERE c.session_id = ?;" in WRITER_SQL
    assert "IS_SRVROLEMEMBER('sysadmin', s.login_name)" in WRITER_SQL
    for forbidden in ("@@TRANCOUNT", "DB_NAME()", "USER_NAME()", "ORIGINAL_LOGIN()", "IS_SRVROLEMEMBER('sysadmin')"):
        assert forbidden not in WRITER_SQL
    assert "VIEW ANY DEFINITION" not in VISIBILITY_SQL
    assert "VIEW ANY DATABASE" not in VISIBILITY_SQL


def test_distinct_observer_preflight_and_target_writer_authority():
    writer = admission()
    observer_admission = replace(
        writer,
        login=replace(
            writer.login,
            principal_id=301,
            name="observer",
            sid="cc",
            original_name="observer",
            original_sid="cc",
        ),
    )

    class DistinctCursor(Cursor):
        def execute(self, sql, *parameters):
            super().execute(sql, *parameters)
            if sql == ADMISSION_SQL:
                row = list(catalog_row())
                row[8:11] = [301, "observer", b"\xcc"]
                self.current = iter([row])
            elif sql == PRINCIPALS_SQL and parameters == (b"\xcc",):
                self.current = iter([(6, "observer_user", b"\xcc", "SQL_USER", "INSTANCE")])

    cursor = DistinctCursor()
    handle = SqlClientWriterObserver.for_target(
        cursor,
        admission=observer_admission,
        target_admission=writer,
        operation_deadline_ns=100,
        monotonic_ns=lambda: 1,
    )

    assert handle.observe(session_id=72, nonce=NONCE) == observation()
    assert cursor.calls == [
        (VISIBILITY_SQL, ()),
        (ADMISSION_SQL, (7, "observer")),
        (PRINCIPALS_SQL, (b"\xcc",)),
        (WRITER_SQL, (72,)),
        (PRINCIPALS_SQL, (b"\xaa",)),
    ]


@pytest.mark.parametrize("sql", [VISIBILITY_SQL, ADMISSION_SQL, WRITER_SQL, PRINCIPALS_SQL])
def test_ambiguity_poisoned_forever(sql):
    cursor = Cursor()
    cursor.results[sql] *= 2
    handle = observer(cursor)
    with pytest.raises(SqlClientObservationError, match="unavailable"):
        handle.observe(session_id=72, nonce=NONCE)
    cursor.results[sql] = cursor.results[sql][:1]
    with pytest.raises(SqlClientObservationError, match="poisoned"):
        handle.preflight()


def test_secret_driver_exception_reduced_to_constant():
    cursor = Cursor()
    cursor.failure = True
    with pytest.raises(SqlClientObservationError) as caught:
        observer(cursor).preflight()
    assert str(caught.value) == "mssql_native.sqlclient_observer_unavailable"
    assert caught.value.__suppress_context__


@pytest.mark.parametrize("expire_at", [0, 1, 2, 3, 8, 20])
def test_original_deadline_before_after_driver_calls(expire_at):
    calls = 0

    def clock():
        nonlocal calls
        calls += 1
        return 100 if calls > expire_at else 1

    cursor = Cursor()
    handle = observer(cursor, clock)
    with pytest.raises(SqlClientObservationError, match="unavailable"):
        handle.observe(session_id=72, nonce=NONCE)
    if expire_at == 0:
        assert cursor.calls == []
    with pytest.raises(SqlClientObservationError, match="poisoned"):
        handle.preflight()


def test_wrong_thread_poisons_owner_handle():
    handle = observer(Cursor())
    errors = []

    def run():
        try:
            handle.preflight()
        except SqlClientObservationError as error:
            errors.append(str(error))

    worker = threading.Thread(target=run)
    worker.start()
    worker.join()
    assert errors == ["mssql_native.sqlclient_observer_owner_mismatch"]
    with pytest.raises(SqlClientObservationError, match="poisoned"):
        handle.preflight()


def test_wrong_process_poisons_owner_handle(monkeypatch):
    handle = observer(Cursor())
    monkeypatch.setattr("dpone.adapters.mssql_sqlclient_observer.os.getpid", lambda: -1)
    with pytest.raises(SqlClientObservationError, match="owner_mismatch"):
        handle.preflight()


def test_original_incarnation_recheck():
    expected = observation().remote_session
    cursor = Cursor()
    handle = observer(cursor)
    assert handle.observe(session_id=72, nonce=NONCE, expected=expected).remote_session == expected
    wrong = replace(expected, connect_time=expected.connect_time + timedelta(seconds=1))
    with pytest.raises(SqlClientObservationError, match="unavailable"):
        handle.observe(session_id=72, nonce=NONCE, expected=wrong)


def test_expiry_at_every_clock_checkpoint():
    class Clock:
        def __init__(self, expiry=None):
            self.count = 0
            self.expiry = expiry

        def __call__(self):
            self.count += 1
            return 100 if self.count == self.expiry else 1

    healthy = Clock()
    observer(Cursor(), healthy).observe(session_id=72, nonce=NONCE)
    for boundary in range(1, healthy.count + 1):
        with pytest.raises(SqlClientObservationError, match="unavailable"):
            observer(Cursor(), Clock(boundary)).observe(session_id=72, nonce=NONCE)


def test_fetch_failure_constant_and_poisoned():
    class BrokenFetch(Cursor):
        def fetchone(self):
            raise RuntimeError("SECRET_CANARY")

    handle = observer(BrokenFetch())
    with pytest.raises(SqlClientObservationError, match="^mssql_native.sqlclient_observer_unavailable$"):
        handle.preflight()
    with pytest.raises(SqlClientObservationError, match="poisoned"):
        handle.preflight()


@pytest.mark.parametrize("session_id", [True, 0, 32768, "72"])
def test_invalid_locator_before_sql(session_id):
    cursor = Cursor()
    with pytest.raises(SqlClientObservationError):
        observer(cursor).observe(session_id=session_id, nonce=NONCE)
    assert cursor.calls == []


@pytest.mark.parametrize("sql", [VISIBILITY_SQL, ADMISSION_SQL, PRINCIPALS_SQL])
def test_missing_metadata_prevents_source_ready_preflight(sql):
    cursor = Cursor()
    cursor.results[sql] = []
    handle = observer(cursor)
    with pytest.raises(SqlClientObservationError):
        handle.preflight()
    assert WRITER_SQL not in [query for query, _ in cursor.calls]


@pytest.mark.parametrize("operation", ["preflight", "observe"])
@pytest.mark.parametrize("reentry", ["wrong_thread", "same_thread"])
def test_overlapping_rejection_permanently_faults_inflight_operation(operation, reentry):
    """A rejected call cannot be erased when the original driver call returns."""
    rejected = []

    class OverlapCursor(Cursor):
        def execute(self, sql, *parameters):
            super().execute(sql, *parameters)
            if len(self.calls) != 1:
                return

            def overlap():
                try:
                    handle.preflight()
                except SqlClientObservationError as error:
                    rejected.append(str(error))

            if reentry == "wrong_thread":
                thread = threading.Thread(target=overlap, daemon=True)
                thread.start()
                thread.join(timeout=2)
                assert not thread.is_alive(), "state lock must not cover driver calls"
            else:
                overlap()

    cursor = OverlapCursor()
    handle = observer(cursor)
    with pytest.raises(SqlClientObservationError, match="unavailable"):
        if operation == "preflight":
            handle.preflight()
        else:
            handle.observe(session_id=72, nonce=NONCE)
    assert rejected == [
        "mssql_native.sqlclient_observer_" + ("owner_mismatch" if reentry == "wrong_thread" else "poisoned")
    ]
    calls = len(cursor.calls)
    for _ in range(2):
        with pytest.raises(SqlClientObservationError, match="poisoned"):
            handle.preflight()
    assert len(cursor.calls) == calls


@pytest.mark.parametrize("phase", ["fetch", "clock", "finish"])
def test_reentry_near_completion_cannot_restore_health(phase, monkeypatch):
    cursor = Cursor()
    handle = observer(cursor)
    entered = False

    def reenter_once():
        nonlocal entered
        if entered:
            return
        entered = True
        with pytest.raises(SqlClientObservationError, match="poisoned"):
            handle.preflight()

    if phase == "fetch":
        original_fetch = cursor.fetchone

        def fetch():
            reenter_once()
            return original_fetch()

        monkeypatch.setattr(cursor, "fetchone", fetch)
    elif phase == "clock":

        def clock():
            reenter_once()
            return 1

        monkeypatch.setattr(handle, "_clock", clock)
    else:
        original_finish = handle._finish

        def finish():
            reenter_once()
            original_finish()

        monkeypatch.setattr(handle, "_finish", finish)
    with pytest.raises(SqlClientObservationError, match="unavailable"):
        handle.preflight()
    assert entered
    with pytest.raises(SqlClientObservationError, match="poisoned"):
        handle.preflight()
