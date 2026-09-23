"""Real authority SQL construction with a deterministic cursor, not live SQL proof."""

from dataclasses import replace
from time import monotonic
from types import SimpleNamespace
from uuid import UUID

import pytest

from dpone.adapters.mssql_tds_coordinator_connection import TdsSqlConnection
from dpone.adapters.mssql_tds_coordinator_sql import _ACQUIRE, _DATABASE, _LOCK, TdsAuthorityError, TdsCoordinatorSql
from dpone.adapters.mssql_tds_session import _INITIALIZE, _OBSERVE
from dpone.contracts.mssql_tds_coordinator import TdsCoordinatorGrant, coordinator_identity_digest
from dpone.contracts.mssql_tds_coordinator_authority import authority_digest
from dpone.contracts.mssql_tds_create import create_command_digest
from tests.test_mssql_tds_coordinator import PROCESS, identity
from tests.test_mssql_tds_create import request
from tests.test_mssql_tds_directory_journal import OWNER
from tests.test_mssql_tds_session import NONCE, observation


class Cursor:
    def __init__(self):
        self.calls = []
        self.rows = []
        self.session_row = observation()
        self.database = ["db", 5, UUID(int=5), 1, "schema"]
        self.lock = "Exclusive"
        self.lock_code = 0
        self.fail = None
        self.closed = False
        self.transaction = 0
        self.description = None
        self.completion = None

    def execute(self, sql, *args):
        self.calls.append((sql, args))
        self.description = None if sql == _INITIALIZE else (("column",),)
        if self.fail and self.fail in sql:
            raise RuntimeError("driver secret")
        if sql == _INITIALIZE:
            self.session_row[9] = args[0]
            self.rows = []
        elif sql == _OBSERVE:
            row = list(self.session_row)
            row[10:13] = [self.transaction, 1 if self.transaction else 0, 0]
            self.rows = [row]
        elif sql == _DATABASE:
            self.rows = [self.database]
        elif sql == _ACQUIRE:
            self.rows = [(self.lock_code,)]
        elif sql == _LOCK:
            self.rows = [(self.lock,)]
        else:
            raise AssertionError(sql)

    def nextset(self):
        assert not self.rows
        if isinstance(self.completion, BaseException):
            raise self.completion
        return self.completion

    def fetchone(self):
        return self.rows.pop(0) if self.rows else None

    def close(self):
        self.closed = True


def sql_owner(cursor=None):
    cursor = Cursor() if cursor is None else cursor
    connection = TdsSqlConnection(SimpleNamespace(close=lambda: None), cursor)
    operation = replace(identity(), command_sha256=create_command_digest(request()))
    return TdsCoordinatorSql(connection, operation, OWNER, PROCESS)


def grant(sql):
    return TdsCoordinatorGrant(
        coordinator_identity_digest(sql.identity),
        OWNER,
        PROCESS,
        sql.authority.session,
        authority_digest(sql.authority),
        UUID(int=99),
    )


def test_authority_observes_same_database_and_continuous_session_lock():
    sql = sql_owner()
    receipt = sql.acquire(NONCE, deadline=monotonic() + 1)
    assert receipt.session.nonce == NONCE and receipt.database.database_guid == UUID(int=5)
    assert sql.require_authority(deadline=monotonic() + 1) == receipt
    assert sql.cursor.calls[0][0] == _INITIALIZE
    lock_call = next(args for text, args in sql.cursor.calls if text == _ACQUIRE)
    assert lock_call[0] == "dpone.tds.coordinator.ddl.v1" and 0 <= lock_call[1] <= 1000
    assert all("CREATE TABLE" not in text for text, _ in sql.cursor.calls)


@pytest.mark.parametrize("fault", ["lock", "database", "session", "timeout"])
def test_failed_continuity_never_reacquires(fault):
    sql = sql_owner()
    sql.acquire(NONCE, deadline=monotonic() + 1)
    deadline = monotonic() + 1
    if fault == "lock":
        sql.cursor.lock = "NoLock"
    elif fault == "database":
        sql.cursor.database[2] = UUID(int=6)
    elif fault == "session":
        sql.cursor.session_row[0] = UUID(int=7)
    else:
        deadline = monotonic() - 1
    with pytest.raises(TdsAuthorityError):
        sql.require_authority(deadline=deadline)
    calls = len(sql.cursor.calls)
    with pytest.raises(TdsAuthorityError):
        sql.require_authority(deadline=monotonic() + 1)
    with pytest.raises(TdsAuthorityError):
        sql.acquire(NONCE, deadline=monotonic() + 1)
    assert len(sql.cursor.calls) == calls


@pytest.mark.parametrize("code", [-1, -2, -3, -999, True, None])
def test_lock_acquisition_failure_returns_no_authority(code):
    sql = sql_owner()
    sql.cursor.lock_code = code
    with pytest.raises(TdsAuthorityError):
        sql.acquire(NONCE, deadline=monotonic() + 1)
    assert sql.authority is None


def test_missing_or_oversized_database_metadata_is_not_authority():
    for database in (["db", True, UUID(int=5), 1, "schema"], ["x" * 129, 5, UUID(int=5), 1, "schema"]):
        sql = sql_owner()
        sql.cursor.database = database
        with pytest.raises(TdsAuthorityError):
            sql.acquire(NONCE, deadline=monotonic() + 1)
        assert not any(text == _ACQUIRE for text, _ in sql.cursor.calls)


@pytest.mark.parametrize("statement", [_DATABASE, _ACQUIRE, _LOCK])
@pytest.mark.parametrize("fault", [True, 0, [], RuntimeError("driver"), "missing", "description"])
def test_authority_result_completion_failure_stops_and_poisons(statement, fault):
    cursor = Cursor()
    original_execute = cursor.execute

    def execute(sql, *parameters):
        original_execute(sql, *parameters)
        if sql == statement:
            if fault == "missing":
                cursor.nextset = None
            elif fault == "description":
                cursor.description = None
            else:
                cursor.completion = fault

    cursor.execute = execute
    sql = sql_owner(cursor)
    with pytest.raises(TdsAuthorityError):
        sql.acquire(NONCE, deadline=monotonic() + 1)
    assert cursor.calls[-1][0] == statement
    stopped = len(cursor.calls)
    with pytest.raises(TdsAuthorityError):
        sql.acquire(NONCE, deadline=monotonic() + 1)
    with pytest.raises(TdsAuthorityError):
        sql.require_authority(deadline=monotonic() + 1)
    assert len(cursor.calls) == stopped


@pytest.mark.parametrize("completion", [None, False])
def test_authority_accepts_exact_terminal_nextset_values(completion):
    cursor = Cursor()
    cursor.completion = completion
    sql = sql_owner(cursor)
    receipt = sql.acquire(NONCE, deadline=monotonic() + 1)
    assert sql.require_authority(deadline=monotonic() + 1) == receipt
