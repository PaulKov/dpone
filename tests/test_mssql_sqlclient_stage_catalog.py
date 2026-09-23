"""Scripted acquisition/fault matrix; no live SQL or transport certification."""

from copy import deepcopy
from dataclasses import replace
from threading import Thread
from uuid import UUID

import pytest

from dpone.adapters import mssql_sqlclient_stage_catalog as catalog
from dpone.adapters.mssql_sqlclient_create_exclusion_v2_sql import OWN_INCARNATION_SQL
from dpone.adapters.mssql_sqlclient_stage_catalog_sql import (
    COLUMNS_SQL,
    FEATURES_SQL,
    OBJECT_SQL,
    SCHEMA_SQL,
    STAGE_VISIBILITY_SQL,
)
from dpone.contracts.mssql_tds_create import TdsCreateObservedColumn, TdsCreateType
from tests.mssql_sqlclient_departure_v2_fixtures import sample
from tests.test_mssql_sqlclient_observer_incarnation_adapter import own_row
from tests.test_mssql_sqlclient_stage_identity import stage


class Cursor:
    def __init__(self, width=5):
        self.context = sample()
        db = self.context.admission.database
        self.expected = replace(
            stage(),
            database_guid=UUID(db.database_guid),
            database_id=db.database_id,
            database_name=db.database_name,
            columns=tuple(
                TdsCreateObservedColumn(i, f"c{i}", TdsCreateType.BIGINT, True, 8, 19, 0, None)
                for i in range(1, width + 1)
            ),
        )
        self.rows = []
        self.calls = []
        self.mutate = lambda sql, rows: rows
        self.extra = None
        self.on_fetch = lambda: None
        self.empty = 1

    def execute(self, sql, *parameters):
        self.calls.append((sql, parameters))
        e = self.expected
        if sql == OWN_INCARNATION_SQL:
            rows = [own_row(self.context.observer)]
        elif sql == STAGE_VISIBILITY_SQL:
            rows = [(16, 1, 1, 1)]
        elif sql == SCHEMA_SQL:
            rows = [(e.schema_id, e.schema_name)]
        elif sql == OBJECT_SQL:
            rows = [(e.object_id, e.table_name, e.create_date, e.owner_binding, str(e.object_nonce), 0, 0, 0, 0)]
        elif sql == FEATURES_SQL:
            rows = [(0,) * 12]
        elif sql == COLUMNS_SQL:
            rows = [
                (
                    c.ordinal,
                    c.name,
                    c.type.value,
                    c.nullable,
                    c.max_length,
                    c.precision,
                    c.scale,
                    c.collation,
                    0,
                    False,
                    False,
                )
                for c in e.columns
            ]
        else:
            assert sql.startswith("SELECT CASE WHEN EXISTS")
            rows = [(self.empty,)]
        self.rows = [list(row) for row in self.mutate(sql, rows)]

    def fetchone(self):
        self.on_fetch()
        return self.rows.pop(0) if self.rows else None

    def nextset(self):
        return self.extra


def observer(cursor, clock=lambda: 0):
    return catalog.SqlClientStageObserver(
        cursor, admission=cursor.context.admission, operation_deadline_ns=100, monotonic_ns=clock
    )


def rejected(cursor):
    subject = observer(cursor)
    with pytest.raises(catalog.SqlClientStageObservationError):
        subject.observe(cursor.expected)
    calls = len(cursor.calls)
    with pytest.raises(catalog.SqlClientStageObservationError):
        subject.observe(cursor.expected)
    assert len(cursor.calls) == calls


@pytest.mark.parametrize("width", [1, 5, 100])
def test_actual_acquisition_and_recheck(width):
    c = Cursor(width)
    result = observer(c).observe(c.expected)
    assert result.before == result.after == c.expected
    assert result.before is not c.expected
    assert result.empty == 1 and result.metadata_permissions == (1, 1, 1)
    assert result.management_before == result.management_after == c.context.observer
    assert len(c.calls) == 15
    assert not any(s.startswith(("GRANT", "DROP", "CREATE", "INSERT")) for s, _ in c.calls)


@pytest.mark.parametrize("sql", [OBJECT_SQL, SCHEMA_SQL, STAGE_VISIBILITY_SQL, FEATURES_SQL, OWN_INCARNATION_SQL])
@pytest.mark.parametrize("mode", ["missing", "duplicate", "nullrow"])
def test_cardinality_and_missing_rows_poison(sql, mode):
    c = Cursor()
    c.mutate = lambda s, r: (
        ([] if mode == "missing" else r * 2 if mode == "duplicate" else [[None] * len(r[0])]) if s == sql else r
    )
    rejected(c)


@pytest.mark.parametrize("index", range(12))
@pytest.mark.parametrize("value", [1, True, None, 0.0])
def test_each_unsupported_feature_and_rls_fails(index, value):
    c = Cursor()

    def mutate(sql, rows):
        if sql == FEATURES_SQL:
            rows = [list(rows[0])]
            rows[0][index] = value
        return rows

    c.mutate = mutate
    rejected(c)


@pytest.mark.parametrize("index", [1, 2, 3])
@pytest.mark.parametrize("value", [0, None, True, 1.0])
def test_permissions_require_exact_effective_one(index, value):
    c = Cursor()

    def mutate(sql, rows):
        if sql == STAGE_VISIBILITY_SQL:
            rows = [list(rows[0])]
            rows[0][index] = value
        return rows

    c.mutate = mutate
    rejected(c)


@pytest.mark.parametrize("version", [13, 14, 15, 18, None, True, 16.0])
def test_unsupported_version_rejected_before_feature_query(version):
    c = Cursor()
    c.mutate = lambda sql, rows: [(version, 1, 1, 1)] if sql == STAGE_VISIBILITY_SQL else rows
    rejected(c)
    assert not any(s == FEATURES_SQL for s, _ in c.calls)


@pytest.mark.parametrize(
    "index,value",
    [
        (0, 11),
        (1, "different"),
        (2, None),
        (3, "b" * 64),
        (4, str(UUID(int=17))),
        (5, 1),
        (6, 1),
        (7, 1),
        (8, 1),
        (0, True),
        (3, None),
        (4, None),
    ],
)
def test_object_identity_flags_and_properties(index, value):
    c = Cursor()

    def mutate(sql, rows):
        if sql == OBJECT_SQL:
            rows = [list(rows[0])]
            rows[0][index] = value
        return rows

    c.mutate = mutate
    rejected(c)


@pytest.mark.parametrize(
    "index,value",
    [
        (0, 2),
        (1, "other"),
        (2, "float"),
        (2, None),
        (3, 1),
        (3, False),
        (4, 4),
        (5, 18),
        (6, 1),
        (7, "other"),
        (8, 1),
        (9, True),
        (10, True),
    ],
)
def test_column_drift_and_hidden_type(index, value):
    c = Cursor()

    def mutate(sql, rows):
        if sql == COLUMNS_SQL:
            rows = deepcopy(rows)
            rows[0] = list(rows[0])
            rows[0][index] = value
        return rows

    c.mutate = mutate
    rejected(c)


@pytest.mark.parametrize("empty", [0, None, True, 1.0])
def test_nonempty_and_untyped_results(empty):
    c = Cursor()
    c.empty = empty
    rejected(c)


def test_bound101_rejects_overflow_instead_of_truncating():
    c = Cursor(100)
    c.mutate = lambda sql, rows: rows + [rows[-1]] if sql == COLUMNS_SQL else rows
    rejected(c)


@pytest.mark.parametrize("extra", [True, 0, 1, "", []])
def test_extra_results_not_eof(extra):
    c = Cursor()
    c.extra = extra
    rejected(c)


def test_schema_table_quoted_individually():
    c = Cursor()
    c.expected = replace(c.expected, schema_name="s].x", table_name="t].x")
    observer(c).observe(c.expected)
    sql = next(s for s, _ in c.calls if s.startswith("SELECT CASE"))
    assert "[s]].x].[t]].x] WITH(READCOMMITTEDLOCK)" in sql
    assert all(params == ("[s]].x].[t]].x]",) for s, params in c.calls if s == STAGE_VISIBILITY_SQL)


def test_after_empty_identity_drift_rejected():
    c = Cursor()
    reads = 0

    def mutate(sql, rows):
        nonlocal reads
        if sql == OBJECT_SQL:
            reads += 1
            if reads == 2:
                rows = [list(rows[0])]
                rows[0][0] += 1
        return rows

    c.mutate = mutate
    rejected(c)


def test_swallowed_reentry_poisons_outer_call():
    c = Cursor()
    subject = observer(c)
    fired = False

    def reentry():
        nonlocal fired
        if not fired:
            fired = True
            with pytest.raises(catalog.SqlClientStageObservationError):
                subject.observe(c.expected)

    c.on_fetch = reentry
    with pytest.raises(catalog.SqlClientStageObservationError):
        subject.observe(c.expected)


def test_wrong_thread_poisons_before_sql():
    c = Cursor()
    subject = observer(c)
    errors = []

    def run():
        try:
            subject.observe(c.expected)
        except catalog.SqlClientStageObservationError:
            errors.append(True)

    t = Thread(target=run)
    t.start()
    t.join()
    assert errors == [True] and not c.calls
    with pytest.raises(catalog.SqlClientStageObservationError):
        subject.observe(c.expected)


def test_wrong_process_at_final_boundary_fails(monkeypatch):
    c = Cursor()
    subject = observer(c)
    original = c.nextset

    def nextset():
        if len(c.calls) == 15:
            monkeypatch.setattr(catalog.os, "getpid", lambda: -1)
        return original()

    c.nextset = nextset
    with pytest.raises(catalog.SqlClientStageObservationError):
        subject.observe(c.expected)


def test_deadline_checked_after_driver_and_no_new_budget():
    c = Cursor()
    now = [0]
    subject = observer(c, lambda: now[0])
    c.on_fetch = lambda: now.__setitem__(0, 100)
    with pytest.raises(catalog.SqlClientStageObservationError):
        subject.observe(c.expected)
    assert len(c.calls) == 1


def test_expected_detached_before_driver_mutates_original():
    c = Cursor()
    original = c.expected
    subject = observer(c)

    def mutate(sql, rows):
        object.__setattr__(original, "object_id", 11)
        return rows

    c.mutate = mutate
    with pytest.raises(catalog.SqlClientStageObservationError):
        subject.observe(original)


@pytest.mark.parametrize("method", ["execute", "fetchone", "nextset"])
@pytest.mark.parametrize("error", [RuntimeError, KeyboardInterrupt])
def test_all_driver_failures_poison_including_process_control(method, error):
    c = Cursor()
    subject = observer(c)

    def fail(*args):
        raise error("private driver detail")

    setattr(c, method, fail)
    expected = error if error is KeyboardInterrupt else catalog.SqlClientStageObservationError
    with pytest.raises(expected):
        subject.observe(c.expected)
    with pytest.raises(catalog.SqlClientStageObservationError):
        subject.observe(c.expected)


@pytest.mark.parametrize("field", ["object_id", "schema_id", "database_id"])
@pytest.mark.parametrize("value", [True, 1.0, None])
def test_original_stage_alias_rejected_before_driver(field, value):
    c = Cursor()
    object.__setattr__(c.expected, field, value)
    rejected(c)
    assert not c.calls


@pytest.mark.parametrize(
    "field,value",
    [
        ("schema_name", "different"),
        ("database_id", 999),
        ("database_guid", UUID(int=999)),
        ("database_name", "different"),
    ],
)
def test_full_schema_database_binding(field, value):
    c = Cursor()
    expected = replace(c.expected, **{field: value})
    with pytest.raises(catalog.SqlClientStageObservationError):
        observer(c).observe(expected)


def test_current_management_drift_after_emptiness():
    c = Cursor()
    reads = 0

    def mutate(sql, rows):
        nonlocal reads
        if sql == OWN_INCARNATION_SQL:
            reads += 1
            if reads == 4:
                rows[0][20] = UUID(int=199)
        return rows

    c.mutate = mutate
    rejected(c)


def test_incomplete_column_eof_cannot_hide_extra_row():
    c = Cursor(100)
    original = c.fetchone

    def fetch():
        if c.calls[-1][0] == COLUMNS_SQL and not c.rows:
            raise RuntimeError("incomplete result")
        return original()

    c.fetchone = fetch
    rejected(c)


def test_columns_left_join_preserves_unknown_type_row():
    assert "LEFT JOIN sys.types" in COLUMNS_SQL
    assert "TOP (101)" in COLUMNS_SQL


def test_admission_snapshot_is_not_mutable_via_caller():
    c = Cursor()
    subject = observer(c)
    old_name = c.context.admission.login.name
    object.__setattr__(c.context.admission.login, "name", "mutated")
    assert subject._admission.login.name == old_name
    with pytest.raises(catalog.SqlClientStageObservationError):
        subject.observe(c.expected)
