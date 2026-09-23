"""Shared facade and dispatch leases, with no live driver or SQL server."""

from threading import Thread
from types import SimpleNamespace

import pytest

from dpone.adapters.mssql_sqlclient_grant_catalog import PERMISSIONS_SQL, SqlClientGrantCatalog
from dpone.adapters.mssql_sqlclient_observation_cursor import ObservationCursor
from dpone.adapters.mssql_sqlclient_stage_catalog import SqlClientStageObserver
from tests.test_mssql_sqlclient_stage_catalog import Cursor


def shared():
    cursor = Cursor()
    owner = ObservationCursor(cursor, deadline=100, clock=lambda: 1)
    sql = SimpleNamespace(
        cursor=cursor,
        identity=None,
        execution_owner=None,
        process=None,
        authority=object(),
        check_deadline=lambda **kwargs: None,
        require_authority=lambda **kwargs: None,
    )
    catalog = SqlClientGrantCatalog._sharing(sql, deadline=100, session=owner)
    stage = SqlClientStageObserver._sharing(owner, admission=cursor.context.admission, operation_deadline_ns=100)
    return cursor, owner, sql, catalog, stage


@pytest.mark.parametrize("direction", ["stage_catalog", "catalog_stage"])
@pytest.mark.parametrize("foreign", [False, True])
def test_caught_cross_facade_reentry_poison(direction, foreign):
    cursor, owner, sql, catalog, stage = shared()
    failures = []
    nested = catalog.guard if direction == "stage_catalog" else lambda: stage.observe(cursor.expected)

    def attempt():
        try:
            nested()
        except RuntimeError:
            failures.append("rejected")

    def callback(*args, **kwargs):
        if foreign:
            thread = Thread(target=attempt)
            thread.start()
            thread.join()
        else:
            attempt()

    if direction == "stage_catalog":
        cursor.on_fetch = callback

        def outer():
            return stage.observe(cursor.expected)
    else:
        sql.require_authority = callback
        outer = catalog.guard
    with pytest.raises(RuntimeError):
        outer()
    assert failures == ["rejected"]
    assert owner.faulted and not owner.busy
    calls = list(cursor.calls)
    for operation in (catalog.guard, lambda: stage.observe(cursor.expected)):
        with pytest.raises(RuntimeError):
            operation()
    assert cursor.calls == calls


@pytest.mark.parametrize("boundary", [2, 4, 6])
def test_permission_count_select_count_share_one_lease(boundary):
    cursor, owner, sql, catalog, stage = shared()
    authority_calls = []

    def execute(statement, *parameters):
        cursor.calls.append((statement, parameters))
        cursor.rows = [] if statement == PERMISSIONS_SQL else [(0,)]

    def authority(**kwargs):
        authority_calls.append(1)
        if len(authority_calls) == boundary:
            with pytest.raises(RuntimeError):
                stage.observe(cursor.expected)

    cursor.execute = execute
    sql.require_authority = authority
    with pytest.raises(RuntimeError):
        catalog.permissions(5, 0, limit=10)
    assert len(cursor.calls) == boundary // 2
    assert owner.faulted and not owner.busy


def test_dispatch_holds_lease_during_leading_authority():
    from dpone.adapters.mssql_sqlclient_observe_session import SqlClientObserveSession

    cursor, owner, sql, catalog, stage = shared()
    session = object.__new__(SqlClientObserveSession)
    session._session, session._sql, session._catalog, session._observer = owner, sql, catalog, stage
    session.deadline, session.request = 100, SimpleNamespace(selected_stage=cursor.expected)

    def authority(**kwargs):
        with pytest.raises(RuntimeError):
            stage.observe(cursor.expected)

    sql.require_authority = authority
    with pytest.raises(ValueError):
        session.dispatch({"opcode": "OBSERVE_SELECTED", "arguments": {}})
    assert owner.faulted and cursor.calls == []


def test_full_selected_stage_dispatch_keeps_fifteen_fixed_queries():
    from dpone.adapters.mssql_sqlclient_observe_session import SqlClientObserveSession
    from dpone.adapters.mssql_sqlclient_stage_catalog_sql import SCHEMA_SQL, STAGE_VISIBILITY_SQL
    from tests.test_mssql_sqlclient_grant_catalog import Cursor as GrantCursor
    from tests.test_mssql_sqlclient_observe_handshake import credentials

    class SelectedCursor(GrantCursor):
        def execute(self, statement, *parameters):
            if statement == STAGE_VISIBILITY_SQL:
                rows = [(16, 1, 1, 1)]
            elif statement == SCHEMA_SQL:
                rows = [(self.stage.schema_id, self.stage.schema_name)]
            elif statement.startswith("SELECT CASE WHEN EXISTS"):
                rows = [(1,)]
            else:
                return super().execute(statement, *parameters)
            self.calls.append((statement, parameters))
            self.rows = rows

    request, _, _ = credentials()
    cursor = SelectedCursor()
    owner = ObservationCursor(cursor, deadline=100, clock=lambda: 1)
    sql = SimpleNamespace(
        cursor=cursor,
        identity=None,
        execution_owner=None,
        process=None,
        authority=object(),
        require_authority=lambda **kw: None,
    )
    catalog = SqlClientGrantCatalog._sharing(sql, deadline=100, session=owner)
    stage = SqlClientStageObserver._sharing(owner, admission=request.management_admission, operation_deadline_ns=100)
    session = object.__new__(SqlClientObserveSession)
    session._session, session._sql, session._catalog, session._observer = owner, sql, catalog, stage
    session.deadline, session.request = 100, request
    result = session.dispatch({"opcode": "OBSERVE_SELECTED", "arguments": {}})
    assert len(result) == 1 and len(cursor.calls) == 15
    assert not owner.faulted and not owner.busy


@pytest.mark.parametrize("boundary", ["fetch", "finish"])
def test_shared_owner_rejects_caught_foreign_pid_and_final_callback(monkeypatch, boundary):
    cursor, owner, sql, catalog, stage = shared()

    def attempt():
        with monkeypatch.context() as patch:
            patch.setattr("dpone.adapters.mssql_sqlclient_observation_cursor.os.getpid", lambda: -1)
            with pytest.raises(RuntimeError):
                catalog.guard()

    if boundary == "fetch":
        cursor.on_fetch = attempt
    else:
        finish = owner.finish

        def callback(lease):
            attempt()
            finish(lease)

        monkeypatch.setattr(owner, "finish", callback)
    with pytest.raises(RuntimeError):
        stage.observe(cursor.expected)
    assert owner.faulted and not owner.busy
    calls = len(cursor.calls)
    with pytest.raises(RuntimeError):
        catalog.guard()
    assert len(cursor.calls) == calls


@pytest.mark.parametrize("boundary", ["execute", "fetchone", "nextset", "clock"])
@pytest.mark.parametrize("process_control", [KeyboardInterrupt, SystemExit])
def test_grant_public_error_policy_contains_process_control(boundary, process_control):
    cursor, owner, sql, catalog, stage = shared()

    def fail(*args):
        raise process_control("synthetic_secret_canary")

    if boundary == "clock":
        owner.clock = fail
    else:
        setattr(cursor, boundary, fail)
    with pytest.raises(RuntimeError, match="^mssql_native.sqlclient_grant_catalog_unavailable$"):
        catalog.read_own_incarnation()
    assert owner.faulted and not owner.busy
