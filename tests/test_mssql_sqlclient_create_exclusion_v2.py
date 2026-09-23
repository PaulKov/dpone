"""Scripted cursor tests establish offline contracts, never live certification."""

from dataclasses import replace

import pytest

from dpone.adapters.mssql_sqlclient_create_exclusion_v2 import SqlClientCreateExclusionObserverV2
from dpone.adapters.mssql_sqlclient_create_exclusion_v2_sql import (
    CONNECTIONS_SQL,
    OWN_INCARNATION_SQL,
    REQUESTS_SQL,
    SESSIONS_SQL,
    TRANSACTIONS_SQL,
    FixedSqlClientCreateExclusionQueries,
)
from dpone.adapters.mssql_sqlclient_observer import SqlClientObservationError
from dpone.contracts.mssql_sqlclient_observation import SqlClientObserverAdmission
from tests.mssql_sqlclient_departure_v2_fixtures import sample
from tests.test_mssql_sqlclient_observer import Cursor
from tests.test_mssql_sqlclient_observer_incarnation_adapter import own_row


class V2Cursor(Cursor):
    def __init__(self, fixture=None, mutate=None):
        super().__init__()
        self.fixture = fixture or sample()
        self.capture_count = 0
        self.mutate = mutate
        self.extra = False
        self.fail_at = None

    def execute(self, sql, *parameters):
        if len(self.calls) == self.fail_at:
            raise RuntimeError("SECRET_CANARY")
        if sql == OWN_INCARNATION_SQL:
            self.capture_count += 1
            row = own_row(self.fixture.observer)
            if self.mutate:
                self.mutate(self.capture_count, row)
            self.results[sql] = [row]
        elif sql in (CONNECTIONS_SQL, SESSIONS_SQL, REQUESTS_SQL, TRANSACTIONS_SQL):
            b = int(self.fixture.observer.session_id == self.fixture.original.session_id)
            request = self.fixture.samples[2].request
            fields = (
                (request.connection_id, request.session_id, request.request_id, request.start_time)
                if request
                else (None,) * 4
            )
            self.results.setdefault(
                sql,
                [
                    {
                        CONNECTIONS_SQL: (b, b, 0),
                        SESSIONS_SQL: (b, b),
                        REQUESTS_SQL: (b, b, 0, *fields),
                        TRANSACTIONS_SQL: (0,),
                    }[sql]
                ],
            )
        super().execute(sql, *parameters)

    def nextset(self):
        return self.extra


def handle(cursor, clock=lambda: 1, expected=None):
    a = cursor.fixture.observer.authority
    expected = expected or SqlClientObserverAdmission(a.server, a.database, a.login, a.transport)
    return SqlClientCreateExclusionObserverV2(
        cursor,
        admission=cursor.fixture.admission,
        observer_admission=expected,
        operation_deadline_ns=100,
        monotonic_ns=clock,
    )


def test_internal_composition_uses_injected_query_provider() -> None:
    cursor = V2Cursor()

    class RecordingQueries:
        def __init__(self):
            self.calls = []
            self.delegate = FixedSqlClientCreateExclusionQueries(SqlClientCreateExclusionObserverV2._sample_kind)

        def queries(self, *, original, observer):
            self.calls.append((original, observer))
            return self.delegate.queries(original=original, observer=observer)

    provider = RecordingQueries()
    authority = cursor.fixture.observer.authority
    observer_admission = SqlClientObserverAdmission(
        authority.server, authority.database, authority.login, authority.transport
    )
    subject = SqlClientCreateExclusionObserverV2._composed(
        cursor,
        admission=cursor.fixture.admission,
        observer_admission=observer_admission,
        operation_deadline_ns=100,
        monotonic_ns=lambda: 1,
        query_provider=provider,
    )

    assert observe(subject, cursor) == cursor.fixture
    assert provider.calls == [(cursor.fixture.original, cursor.fixture.observer)]


def observe(observer, cursor):
    f = cursor.fixture
    return observer.observe_departure_v2(original=f.original, database=f.database, principal=f.principal)


@pytest.mark.parametrize("reused", [False, True])
def test_thirteen_fresh_captures_six_partitions_eight_creator_reads(reused):
    cursor = V2Cursor(sample(reused=reused))
    assert observe(handle(cursor), cursor) == cursor.fixture
    assert len(cursor.calls) == 27 and cursor.capture_count == 13
    middle = [sql for sql, _ in cursor.calls[4:-4]]
    assert middle == [OWN_INCARNATION_SQL] + [
        q
        for p in (CONNECTIONS_SQL, SESSIONS_SQL, REQUESTS_SQL, TRANSACTIONS_SQL, CONNECTIONS_SQL, SESSIONS_SQL)
        for q in (OWN_INCARNATION_SQL, p, OWN_INCARNATION_SQL)
    ]
    assert all(parameters == () for sql, parameters in cursor.calls if sql == OWN_INCARNATION_SQL)
    c = cursor.calls[6]
    f, own = cursor.fixture, cursor.fixture.observer
    old = str(f.original.connection_id)
    assert c[1] == (old, old, f.original.session_id, str(own.connection_id), own.session_id, own.connect_time, old, old)
    assert c[1][5] is own.connect_time
    assert cursor.calls[9][1] == (f.original.session_id, own.session_id, own.login_time)
    assert "CONVERT(datetime, ?)" in CONNECTIONS_SQL and "CONVERT(datetime, ?)" in SESSIONS_SQL
    assert "CURRENT_REQUEST_ID()" in REQUESTS_SQL and "session_id = @@SPID" in REQUESTS_SQL


@pytest.mark.parametrize("capture_index", range(1, 14))
def test_each_capture_reads_actual_visibility_and_poison(capture_index):
    def mutate(index, row):
        if index == capture_index:
            row[13] = 0  # Unused permission drift still belongs to the identity.

    cursor = V2Cursor(mutate=mutate)
    observer = handle(cursor)
    with pytest.raises(SqlClientObservationError):
        observe(observer, cursor)
    assert cursor.capture_count <= max(capture_index, 2)
    count = len(cursor.calls)
    with pytest.raises(SqlClientObservationError, match="poisoned"):
        observe(observer, cursor)
    assert len(cursor.calls) == count


@pytest.mark.parametrize("index", range(27))
@pytest.mark.parametrize("mode", ["driver", "deadline", "extra"])
def test_each_query_failure_poison(index, mode):
    cursor = V2Cursor()
    if mode == "driver":
        cursor.fail_at = index
    if mode == "extra":
        cursor.nextset = lambda: len(cursor.calls) == index + 1

    def clock():
        return 100 if mode == "deadline" and len(cursor.calls) >= index else 1

    observer = handle(cursor, clock)
    with pytest.raises(SqlClientObservationError) as exc:
        observe(observer, cursor)
    assert "SECRET" not in str(exc.value)
    count = len(cursor.calls)
    with pytest.raises(SqlClientObservationError):
        observe(observer, cursor)
    assert len(cursor.calls) == count


@pytest.mark.parametrize(
    "sql,row",
    [
        (CONNECTIONS_SQL, (1, 1, 1)),
        (CONNECTIONS_SQL, (2, 1, 0)),
        (SESSIONS_SQL, (1, 0)),
        (REQUESTS_SQL, (1, 0, 0, None, None, None, None)),
        (REQUESTS_SQL, (2, 2, 0, None, None, None, None)),
        (REQUESTS_SQL, (0, 0, 0, None, 72, None, None)),
        (TRANSACTIONS_SQL, (1,)),
        (TRANSACTIONS_SQL, (False,)),
        (CONNECTIONS_SQL, (True, 1, 0)),
    ],
)
def test_raw_partition_never_hides_competing_rows(sql, row):
    cursor = V2Cursor()
    cursor.results[sql] = [row]
    with pytest.raises(SqlClientObservationError):
        observe(handle(cursor), cursor)


def test_independent_helper_login_allowed_but_not_substituted():
    f = sample()
    a = f.observer.authority
    login = replace(a.login, name="helper", original_name="helper", sid="cc", original_sid="cc", principal_id=301)
    authority = replace(
        a,
        login=login,
        principal_resolution=replace(a.principal_resolution, name="helper_user", sid="cc", principal_id=6),
    )
    own = replace(f.observer, authority=authority)
    # Expected pure result must include the actual helper guard digests.
    from dpone.contracts.mssql_sqlclient_observer_incarnation import observer_incarnation_digest

    digest = observer_incarnation_digest(own)
    f = replace(
        f, observer=own, samples=tuple(replace(s, before_sha256=digest, after_sha256=digest) for s in f.samples)
    )
    cursor = V2Cursor(f)
    assert observe(handle(cursor), cursor) == f
    with pytest.raises(SqlClientObservationError):
        observe(handle(V2Cursor(f), expected=f.admission), V2Cursor(f))


def test_reentry_and_process_control_poison():
    cursor = V2Cursor()
    observer = handle(cursor)

    def mutate(index, row):
        with pytest.raises(SqlClientObservationError, match="poisoned"):
            observe(observer, cursor)

    cursor.mutate = mutate
    with pytest.raises(SqlClientObservationError):
        observe(observer, cursor)
    cursor = V2Cursor()
    observer = handle(cursor)

    def interrupt(index, row):
        raise KeyboardInterrupt

    cursor.mutate = interrupt
    with pytest.raises(KeyboardInterrupt):
        observe(observer, cursor)
    with pytest.raises(SqlClientObservationError, match="poisoned"):
        observer.preflight()


@pytest.mark.parametrize("capture_index", range(1, 14))
@pytest.mark.parametrize("field", range(68))
def test_no_capture_substitutes_any_raw_field(capture_index, field):
    from tests.mssql_sqlclient_departure_v2_fixtures import alias

    def mutate(index, row):
        if index == capture_index:
            row[field] = alias(row[field])

    cursor = V2Cursor(mutate=mutate)
    with pytest.raises(SqlClientObservationError):
        observe(handle(cursor), cursor)
    assert cursor.capture_count == capture_index


def test_wrong_thread_and_process_never_execute(monkeypatch):
    import threading

    import dpone.adapters.mssql_sqlclient_observer as module

    cursor = V2Cursor()
    observer = handle(cursor)
    failures = []

    def call():
        try:
            observe(observer, cursor)
        except SqlClientObservationError as error:
            failures.append(error)

    thread = threading.Thread(target=call)
    thread.start()
    thread.join(5)
    assert not thread.is_alive() and len(failures) == 1 and not cursor.calls
    with pytest.raises(SqlClientObservationError, match="poisoned"):
        observe(observer, cursor)
    cursor = V2Cursor()
    observer = handle(cursor)
    monkeypatch.setattr(module.os, "getpid", lambda: -1)
    with pytest.raises(SqlClientObservationError, match="owner_mismatch"):
        observe(observer, cursor)
    assert not cursor.calls


def test_entry_sampling_batch_is_fresh_literal_and_one_result_per_capture():
    """The declaration samples actual entry state, not a fabricated zero literal."""
    declaration = "DECLARE @dpone_observer_entry_xact smallint = XACT_STATE();"
    assert OWN_INCARNATION_SQL.lstrip().startswith(declaration + "\nSELECT")
    assert OWN_INCARNATION_SQL.count("XACT_STATE()") == 1
    assert OWN_INCARNATION_SQL.count("@dpone_observer_entry_xact") == 2
    assert "@@TRANCOUNT AS trancount, @dpone_observer_entry_xact AS xact_state" in OWN_INCARNATION_SQL
    assert OWN_INCARNATION_SQL.count(";") == 2
    cursor = V2Cursor()
    assert observe(handle(cursor), cursor) == cursor.fixture
    captures = [sql for sql, parameters in cursor.calls if sql == OWN_INCARNATION_SQL]
    assert len(captures) == 13 and all(sql.lstrip().startswith(declaration) for sql in captures)
    assert len(cursor.calls) == 27
    assert sum(sql.count(";") for sql, parameters in cursor.calls) == 40


@pytest.mark.parametrize("entry_state", [1, -1])
@pytest.mark.parametrize("capture_index", range(1, 14))
def test_entry_transaction_nonzero_still_poisoned_at_every_capture(entry_state, capture_index):
    def mutate(index, row):
        if index == capture_index:
            row[9] = entry_state

    cursor = V2Cursor(mutate=mutate)
    observer = handle(cursor)
    with pytest.raises(SqlClientObservationError):
        observe(observer, cursor)
    assert cursor.capture_count == capture_index
    calls = len(cursor.calls)
    with pytest.raises(SqlClientObservationError, match="poisoned"):
        observe(observer, cursor)
    assert len(cursor.calls) == calls


def test_entry_batch_extra_result_is_rejected_without_reader_change():
    cursor = V2Cursor()
    cursor.nextset = lambda: cursor.capture_count > 0
    observer = handle(cursor)
    with pytest.raises(SqlClientObservationError):
        observe(observer, cursor)
    assert cursor.capture_count == 1
    calls = len(cursor.calls)
    with pytest.raises(SqlClientObservationError, match="poisoned"):
        observe(observer, cursor)
    assert len(cursor.calls) == calls
