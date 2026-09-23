"""Exclusive ownership regression tests using synthetic cursor callbacks only."""

from types import SimpleNamespace

import pytest

from dpone.adapters.mssql_sqlclient_grant_catalog import SqlClientGrantCatalog
from tests.test_mssql_sqlclient_observer import Cursor, observer


def test_guard_caught_reentry_poison_is_sticky():
    calls = []
    sql = SimpleNamespace(
        identity=None,
        execution_owner=None,
        process=None,
        authority=object(),
        cursor=None,
        check_deadline=lambda **kw: None,
    )
    catalog = SqlClientGrantCatalog(sql, deadline=100)

    def authority(**kwargs):
        calls.append(1)
        if len(calls) == 1:
            with pytest.raises(RuntimeError):
                catalog.guard()
        return sql.authority

    sql.require_authority = authority
    with pytest.raises(RuntimeError):
        catalog.guard()
    assert calls == [1]
    with pytest.raises(RuntimeError):
        catalog.guard()
    assert calls == [1]


@pytest.mark.parametrize("error", [KeyboardInterrupt, SystemExit])
def test_writer_process_control_fault_releases_active_lease(error):
    cursor = Cursor()
    handle = observer(cursor)
    cursor.execute = lambda *args: (_ for _ in ()).throw(error())
    with pytest.raises(error):
        handle.preflight()
    assert handle._faulted is True
    assert handle._busy is False


@pytest.mark.parametrize("extra", [0, 0.0, 1, True, "", [], ()])
def test_result_set_false_aliases_poison(extra):
    from dpone.adapters.mssql_sqlclient_observation_cursor import ObservationCursor

    cursor = SimpleNamespace(execute=lambda *args: None, fetchone=lambda: None, nextset=lambda: extra)
    owner = ObservationCursor(cursor, deadline=100, clock=lambda: 1)
    lease = owner.begin()
    with pytest.raises(ValueError):
        owner.rows(lease, "fixed", max_rows=1, nextset_required=True, detach_rows=False)
    assert owner.faulted and not owner.busy
    with pytest.raises(ValueError):
        owner.finish(lease)


@pytest.mark.parametrize("limit", [1, 2, 100, 4096])
@pytest.mark.parametrize("overflow", [False, True])
def test_exact_row_limit_requires_eof_and_never_truncates(limit, overflow):
    from dpone.adapters.mssql_sqlclient_observation_cursor import ObservationCursor

    rows = iter([(1,)] * (limit + overflow) + [None])
    events = []
    cursor = SimpleNamespace(
        execute=lambda *args: events.append("execute"),
        fetchone=lambda: next(rows),
        nextset=lambda: events.append("nextset"),
    )
    owner = ObservationCursor(cursor, deadline=100, clock=lambda: 1)
    lease = owner.begin()
    if overflow:
        with pytest.raises(ValueError):
            owner.rows(lease, "fixed", max_rows=limit, nextset_required=True, detach_rows=False)
        assert events == ["execute"]
    else:
        assert len(owner.rows(lease, "fixed", max_rows=limit, nextset_required=True, detach_rows=False)) == limit
        owner.current(lease)
        owner.finish(lease)
        assert events == ["execute", "nextset"]


@pytest.mark.parametrize("boundary", ["clock", "execute", "fetchone", "nextset"])
@pytest.mark.parametrize("error", [RuntimeError, KeyboardInterrupt, SystemExit])
def test_each_fallible_boundary_poisons_and_propagates(boundary, error):
    from dpone.adapters.mssql_sqlclient_observation_cursor import ObservationCursor

    def fail(*args):
        raise error("synthetic")

    cursor = SimpleNamespace(execute=lambda *args: None, fetchone=lambda: None, nextset=lambda: None)
    owner = ObservationCursor(cursor, deadline=100, clock=fail if boundary == "clock" else lambda: 1)
    if boundary != "clock":
        setattr(cursor, boundary, fail)
    lease = owner.begin()
    with pytest.raises(error):
        owner.rows(lease, "fixed", max_rows=1, nextset_required=True, detach_rows=False)
    assert owner.faulted and not owner.busy
    with pytest.raises(ValueError):
        owner.begin()


@pytest.mark.parametrize("now", [False, True, 0.0, -1, 100, 101])
def test_deadline_exact_integer_and_expiry_before_sql(now):
    from dpone.adapters.mssql_sqlclient_observation_cursor import ObservationCursor

    cursor = SimpleNamespace(execute=lambda *args: pytest.fail("SQL after invalid clock"))
    owner = ObservationCursor(cursor, deadline=100, clock=lambda: now)
    with pytest.raises(ValueError):
        owner.rows(owner.begin(), "fixed", max_rows=1, nextset_required=True, detach_rows=False)
    assert owner.faulted
