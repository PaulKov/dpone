"""Synthetic drivers prove bounded cursor failure, not SQL permission semantics."""

import pytest

from dpone.adapters.mssql_sqlclient_observation_cursor import ObservationCursor
from tests.test_mssql_sqlclient_observe import request


class BrokenCursor:
    def __init__(self):
        self.calls = []

    def execute(self, statement, *parameters):
        self.calls.append((statement, parameters))

    def fetchone(self):
        raise RuntimeError("uncertain fetch/revert")


def test_server_permission_fixed_width_code_is_canonicalized():
    from dpone.adapters.mssql_sqlclient_preparation_catalog import _server_permission_row

    row = (105, 4, 2, 1, "CO  ", "CONNECT", "G", "ENDPOINT", "TSQL Default TCP")
    assert _server_permission_row(row) == (*row[:4], "CO", *row[5:])


@pytest.mark.parametrize("code", ["", "    ", "TOO-LONG"])
def test_server_permission_invalid_code_is_rejected(code):
    from dpone.adapters.mssql_sqlclient_preparation_catalog import _server_permission_row

    with pytest.raises(ValueError):
        _server_permission_row((105, 4, 2, 1, code, "CONNECT", "G", "ENDPOINT", "name"))


def test_effective_fetch_ambiguity_poisoned_without_client_revert_retry():
    from dpone.adapters.mssql_sqlclient_preparation_catalog import SqlClientPreparationCatalog

    cursor = BrokenCursor()
    session = ObservationCursor(cursor, deadline=100, clock=lambda: 1)
    lease = session.begin()
    with pytest.raises(RuntimeError):
        SqlClientPreparationCatalog(session, request()).read_within(lease, "PREP_EFFECTIVE")
    assert session.faulted
    assert len(cursor.calls) == 1
    assert "@preparation_revert_attempted=1" in cursor.calls[0][0]
    with pytest.raises(ValueError):
        session.begin()
    assert len(cursor.calls) == 1


@pytest.mark.parametrize("bound,count,accept", [(8194, 8194, True), (8194, 8195, False), (4096, 4097, False)])
def test_shared_complete_drain_preserves_each_callers_limit(bound, count, accept):
    from dpone.adapters.mssql_sqlclient_observation_cursor import ObservationCursor

    class Cursor:
        def __init__(self):
            self.left, self.executed, self.drained = count, 0, False

        def execute(self, *args):
            self.executed += 1

        def fetchone(self):
            if not self.left:
                return None
            self.left -= 1
            return (1,)

        def nextset(self):
            self.drained = True
            return None

    cursor = Cursor()
    owner = ObservationCursor(cursor, deadline=100, clock=lambda: 1)
    lease = owner.begin()

    def read():
        return owner.rows(lease, "fixed synthetic query", max_rows=bound, nextset_required=True, detach_rows=True)

    if accept:
        assert len(read()) == count and cursor.drained
        owner.finish(lease)
    else:
        with pytest.raises(ValueError):
            read()
        with pytest.raises(ValueError):
            owner.begin()
    assert cursor.executed == 1


@pytest.mark.parametrize("limit", [True, 0, -1, 8195, 1.0])
def test_invalid_shared_bound_rejects_before_execute(limit):
    from types import SimpleNamespace

    from dpone.adapters.mssql_sqlclient_observation_cursor import ObservationCursor

    cursor = SimpleNamespace(execute=lambda *a: pytest.fail("effect before bound validation"))
    owner = ObservationCursor(cursor, deadline=100, clock=lambda: 1)
    with pytest.raises(ValueError):
        owner.rows(owner.begin(), "fixed synthetic query", max_rows=limit, nextset_required=True, detach_rows=True)


@pytest.mark.parametrize("fault", ["caught_reentry", "deadline", "missing_segment"])
def test_effective_ambiguity_poisons_shared_owner_without_second_execute(fault):
    from dpone.adapters.mssql_sqlclient_observation_cursor import ObservationCursor
    from dpone.adapters.mssql_sqlclient_preparation_catalog import SqlClientPreparationCatalog
    from tests.test_mssql_sqlclient_observe import request

    now = [1]

    class Cursor:
        def __init__(self):
            self.executed = 0

        def execute(self, *args):
            self.executed += 1
            if fault == "caught_reentry":
                with pytest.raises(ValueError):
                    owner.begin()
            if fault == "deadline":
                now[0] = 101

        def fetchone(self):
            return None

        def nextset(self):
            return None

    cursor = Cursor()
    owner = ObservationCursor(cursor, deadline=100, clock=lambda: now[0])
    catalog = SqlClientPreparationCatalog(owner, request())
    with pytest.raises(ValueError):
        catalog.read_within(owner.begin(), "PREP_EFFECTIVE")
    with pytest.raises(ValueError):
        owner.begin()
    assert cursor.executed == 1
