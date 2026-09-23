"""Nominal hierarchy, dynamic seams and direct/finite row decoding parity."""

import inspect

import pytest

from dpone.adapters.mssql_sqlclient_create_exclusion import SqlClientCreateExclusionObserver
from dpone.adapters.mssql_sqlclient_create_exclusion_v2 import SqlClientCreateExclusionObserverV2
from dpone.adapters.mssql_sqlclient_observer import SqlClientWriterObserver
from dpone.adapters.mssql_sqlclient_observer_incarnation import (
    capture_observer_incarnation,
    parse_observer_incarnation_rows,
)
from tests.test_mssql_sqlclient_create_exclusion_v2 import V2Cursor, handle, observe
from tests.test_mssql_sqlclient_observer_incarnation_adapter import own_row


def test_exact_nominal_hierarchy_constructor_and_inherited_entrypoints():
    assert SqlClientWriterObserver.__bases__ == (object,)
    assert SqlClientCreateExclusionObserver.__bases__ == (SqlClientWriterObserver,)
    assert SqlClientCreateExclusionObserverV2.__bases__ == (SqlClientCreateExclusionObserver,)
    cursor = V2Cursor()
    subject = handle(cursor)
    assert type(subject).__mro__ == (
        SqlClientCreateExclusionObserverV2,
        SqlClientCreateExclusionObserver,
        SqlClientWriterObserver,
        object,
    )
    assert isinstance(subject, SqlClientWriterObserver)
    assert issubclass(SqlClientCreateExclusionObserverV2, SqlClientWriterObserver)
    assert SqlClientCreateExclusionObserver.__init__ is SqlClientWriterObserver.__init__
    assert list(inspect.signature(SqlClientWriterObserver).parameters) == [
        "cursor",
        "admission",
        "operation_deadline_ns",
        "monotonic_ns",
    ]
    assert list(inspect.signature(SqlClientCreateExclusionObserverV2).parameters) == [
        "cursor",
        "admission",
        "observer_admission",
        "operation_deadline_ns",
        "monotonic_ns",
    ]
    assert SqlClientCreateExclusionObserverV2.observe is SqlClientWriterObserver.observe
    assert SqlClientCreateExclusionObserverV2.observe_departure is SqlClientCreateExclusionObserver.observe_departure


@pytest.mark.parametrize("seam", ["_preflight", "_creator", "_guard", "_query", "_current", "_finish"])
def test_v2_dynamic_override_seams_remain_effective(monkeypatch, seam):
    cursor = V2Cursor()
    subject = handle(cursor)
    original = getattr(subject, seam)
    calls = []

    def intercept(*args, **kwargs):
        calls.append(1)
        return original(*args, **kwargs)

    monkeypatch.setattr(subject, seam, intercept)
    assert observe(subject, cursor) == cursor.fixture
    assert calls
    assert len(cursor.calls) == 27 and cursor.capture_count == 13


def test_decoder_matches_fresh_acquisition_and_retains_exact_raw_types():
    cursor = V2Cursor()
    rows = [own_row(cursor.fixture.observer)]
    assert parse_observer_incarnation_rows(rows, admission=cursor.fixture.admission) == capture_observer_incarnation(
        lambda sql: rows, admission=cursor.fixture.admission
    )
    for ordinal in (19, 28, 39, 43, 45, 51, 62, 66, 67):
        changed = [list(rows[0])]
        changed[0][ordinal] = bool(changed[0][ordinal])
        with pytest.raises(ValueError):
            parse_observer_incarnation_rows(changed, admission=cursor.fixture.admission)


@pytest.mark.parametrize("observer_type", [SqlClientCreateExclusionObserver, SqlClientCreateExclusionObserverV2])
def test_inherited_writer_and_v1_use_creator_admission(observer_type, monkeypatch):
    from dataclasses import replace

    from dpone.adapters import mssql_sqlclient_create_exclusion as v1
    from tests.mssql_sqlclient_departure_v2_fixtures import sample
    from tests.test_mssql_sqlclient_observation import NONCE, admission, observation
    from tests.test_mssql_sqlclient_observer import Cursor

    creator = admission()
    helper = replace(creator, login=replace(creator.login, name="helper", original_name="helper"))
    options = {"observer_admission": helper} if observer_type is SqlClientCreateExclusionObserverV2 else {}
    cursor = Cursor()
    subject = observer_type(cursor, admission=creator, operation_deadline_ns=100, monotonic_ns=lambda: 1, **options)
    subject.preflight()
    assert subject.observe(session_id=72, nonce=NONCE) == observation()
    f = sample()
    for statement in (v1._CONNECTIONS, v1._SESSIONS, v1._REQUESTS, v1._TRANSACTIONS):
        cursor.results[statement] = [(0,)]
    zero = subject._zero
    checks = []

    def checked(*args):
        checks.append(1)
        return zero(*args)

    monkeypatch.setattr(subject, "_zero", checked)
    start = len(cursor.calls)
    assert subject.observe_departure(original=f.original, database=f.database, principal=f.principal).counts == (0,) * 6
    assert len(checks) == 6 and len(cursor.calls) - start == 14
    assert subject._admission is creator


def test_subclass_preflight_and_query_dispatch_uses_actual_instance():
    from tests.test_mssql_sqlclient_observation import admission
    from tests.test_mssql_sqlclient_observer import Cursor

    seen = []

    class Specialized(SqlClientCreateExclusionObserver):
        def _preflight(self):
            seen.append((self, "preflight"))
            super()._preflight()

        def _query(self, statement, *parameters):
            seen.append((self, "query"))
            return super()._query(statement, *parameters)

    subject = Specialized(Cursor(), admission=admission(), operation_deadline_ns=100, monotonic_ns=lambda: 1)
    subject.preflight()
    assert [kind for _, kind in seen] == ["preflight", "query", "query", "query"]
    assert all(instance is subject for instance, _ in seen)


def test_constructor_validates_creator_then_deadline_then_helper_before_sql():
    from tests.test_mssql_sqlclient_observation import admission

    class Untouched:
        def execute(self, *args):
            pytest.fail("constructor touched SQL")

    with pytest.raises(ValueError, match="observer_admission_invalid"):
        SqlClientCreateExclusionObserverV2(
            Untouched(), admission=None, observer_admission=None, operation_deadline_ns=0, monotonic_ns=lambda: 1
        )
    with pytest.raises(ValueError) as invalid_deadline:
        SqlClientCreateExclusionObserverV2(
            Untouched(), admission=admission(), observer_admission=None, operation_deadline_ns=0, monotonic_ns=lambda: 1
        )
    assert "observer_admission_invalid" not in str(invalid_deadline.value)
    with pytest.raises(ValueError, match="observer_admission_invalid"):
        SqlClientCreateExclusionObserverV2(
            Untouched(),
            admission=admission(),
            observer_admission=None,
            operation_deadline_ns=100,
            monotonic_ns=lambda: 1,
        )


def test_finite_remote_own_rows_and_legacy_adapter_use_empty_arguments(monkeypatch):
    from types import SimpleNamespace

    from dpone.adapters.mssql_sqlclient_create_exclusion_v2_sql import OWN_INCARNATION_SQL
    from dpone.adapters.mssql_sqlclient_observe_transport import SqlClientObserveCatalog

    cursor = V2Cursor()
    rows = [own_row(cursor.fixture.observer)]
    remote = SqlClientObserveCatalog(
        None, None, SimpleNamespace(identity=None, execution_owner=None, process=None), None
    )
    calls = []

    def call(opcode, arguments=None):
        calls.append((opcode, {} if arguments is None else arguments))
        return rows

    monkeypatch.setattr(remote, "_call", call)
    assert remote.read_own_incarnation() is rows
    assert remote.own_incarnation(OWN_INCARNATION_SQL) is rows
    assert calls == [("OWN_INCARNATION", {}), ("OWN_INCARNATION", {})]
    assert (
        parse_observer_incarnation_rows(remote.read_own_incarnation(), admission=cursor.fixture.admission)
        == cursor.fixture.observer
    )
    with pytest.raises(ValueError):
        remote.own_incarnation(OWN_INCARNATION_SQL + " ")
    assert remote._failed
