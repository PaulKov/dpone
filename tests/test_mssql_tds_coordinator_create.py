"""Concrete CREATE SQL sequence/faults; scripted cursor is not live certification."""

from dataclasses import replace
from datetime import datetime
from time import monotonic

import pytest

from dpone.adapters.mssql_tds_coordinator_create import (
    _COLUMNS,
    _OBJECT,
    _PROPERTY,
    TdsCoordinatorCreate,
    TdsCreateUnknown,
)
from dpone.contracts.mssql_tds_create import TdsCreateColumn, TdsCreateType, create_command_digest
from tests.test_mssql_tds_coordinator_sql import Cursor, grant, sql_owner
from tests.test_mssql_tds_create import request
from tests.test_mssql_tds_session import NONCE


class CreateCursor(Cursor):
    def __init__(self):
        super().__init__()
        self.created = False
        self.properties = {}
        self.committed = False
        self.metadata_fault = None
        self.after_commit_fault = False
        self.empty = 1
        self.expected = request()

    def execute(self, sql, *args):
        if sql not in (_OBJECT, _COLUMNS, _PROPERTY) and not sql.startswith(
            (
                "SELECT COUNT(*) FROM sys.objects",
                "BEGIN TRANSACTION",
                "CREATE TABLE",
                "COMMIT TRANSACTION",
                "SELECT CASE WHEN EXISTS",
            )
        ):
            return super().execute(sql, *args)
        self.calls.append((sql, args))
        self.rows = []
        if self.fail and self.fail in sql:
            raise RuntimeError("secret SQL failure")
        if sql.startswith("SELECT COUNT(*)"):
            self.rows = [(int(self.created),)]
        elif sql.startswith("BEGIN"):
            self.transaction = 1
        elif sql.startswith("CREATE"):
            self.created = True
        elif sql == _PROPERTY:
            self.properties[args[0]] = args[1]
        elif sql.startswith("COMMIT"):
            self.committed = True
            self.transaction = 0
            if self.after_commit_fault:
                raise RuntimeError("commit ACK lost")
        elif sql == _OBJECT:
            self.rows = [
                (
                    10,
                    self.expected.parent.table,
                    datetime(2026, 1, 1),
                    self.properties.get("dpone_native_owner"),
                    self.properties.get("dpone_tds_incarnation"),
                    0,
                    0,
                    0,
                    0,
                )
            ]
        elif sql == _COLUMNS:
            profiles = {
                TdsCreateType.BIGINT: ("bigint", 8, 19, 0, None),
                TdsCreateType.FLOAT53: ("float", 8, 53, 0, None),
                TdsCreateType.NVARCHARMAX: ("nvarchar", -1, 0, 0, "Latin1_General_100_CI_AS_SC"),
                TdsCreateType.DATETIME2_6: ("datetime2", 8, 26, 6, None),
            }
            for index, col in enumerate(self.expected.columns, 1):
                name, length, precision, scale, collation = profiles[col.type]
                row = [index, col.name, name, col.nullable, length, precision, scale, collation, 0, False, False]
                if self.metadata_fault:
                    row[self.metadata_fault[0]] = self.metadata_fault[1]
                self.rows.append(row)
        else:
            self.rows = [(self.empty,)]


def setup():
    cursor = CreateCursor()
    sql = sql_owner(cursor)
    sql.acquire(NONCE, deadline=monotonic() + 1)
    return cursor, sql, TdsCoordinatorCreate(sql)


def test_atomic_create_and_two_properties_one_commit_retains_full_evidence():
    cursor, sql, creator = setup()
    result = creator.execute(request(), grant(sql), deadline=monotonic() + 1)
    commands = [text for text, _ in cursor.calls]
    assert commands.count("BEGIN TRANSACTION;") == commands.count("COMMIT TRANSACTION;") == 1
    assert (
        commands.index("BEGIN TRANSACTION;")
        < next(i for i, text in enumerate(commands) if text.startswith("CREATE TABLE"))
        < commands.index(_PROPERTY)
        < commands.index("COMMIT TRANSACTION;")
    )
    assert set(cursor.properties) == {"dpone_native_owner", "dpone_tds_incarnation"}
    assert result == creator.evidence and result.committed and result.empty
    assert result.columns[0].nullable is True
    creator.close()
    assert creator.evidence == result


@pytest.mark.parametrize("kind", list(TdsCreateType))
@pytest.mark.parametrize("nullable", [False, True])
def test_all_exact_nullable_types_are_rendered_and_observed(kind, nullable):
    cursor = CreateCursor()
    cursor.expected = replace(request(), columns=(TdsCreateColumn("x]dot.name", kind, nullable),))
    sql = sql_owner(cursor)
    sql.identity = replace(sql.identity, command_sha256=create_command_digest(cursor.expected))
    sql.acquire(NONCE, deadline=monotonic() + 1)
    evidence = TdsCoordinatorCreate(sql).execute(cursor.expected, grant(sql), deadline=monotonic() + 1)
    assert evidence.columns[0].type is kind and evidence.columns[0].nullable is nullable
    assert any("[x]]dot.name]" in text for text, _ in cursor.calls)


@pytest.mark.parametrize("binding", ["request", "owner", "process", "session", "authority"])
def test_wrong_grant_or_request_never_executes_business_sql(binding):
    cursor, sql, creator = setup()
    saved_grant = grant(sql)
    value = request()
    if binding == "request":
        value = replace(value, columns=(replace(value.columns[0], nullable=False),))
    elif binding == "owner":
        saved_grant = replace(saved_grant, ownership=replace(saved_grant.ownership, owner="other"))
    elif binding == "process":
        saved_grant = replace(saved_grant, process=replace(saved_grant.process, pid=999))
    elif binding == "session":
        saved_grant = replace(saved_grant, session=replace(saved_grant.session, session_id=999))
    else:
        saved_grant = replace(saved_grant, authority_sha256="e" * 64)
    before = len(cursor.calls)
    with pytest.raises(TdsCreateUnknown):
        creator.execute(value, saved_grant, deadline=monotonic() + 1)
    assert len(cursor.calls) == before


@pytest.mark.parametrize("fault", ["CREATE TABLE", "sp_addextendedproperty", "COMMIT TRANSACTION", "column_id"])
def test_fault_never_replays_or_compensates(fault):
    cursor, sql, creator = setup()
    cursor.fail = fault
    with pytest.raises(TdsCreateUnknown) as raised:
        creator.execute(request(), grant(sql), deadline=monotonic() + 1)
    assert raised.value.evidence is None
    count = len(cursor.calls)
    with pytest.raises(TdsCreateUnknown):
        creator.execute(request(), grant(sql), deadline=monotonic() + 1)
    assert len(cursor.calls) == count and not any("DROP" in text or "ROLLBACK" in text for text, _ in cursor.calls)


def test_lost_commit_ack_retains_unknown_without_recreate():
    cursor, sql, creator = setup()
    cursor.after_commit_fault = True
    with pytest.raises(TdsCreateUnknown):
        creator.execute(request(), grant(sql), deadline=monotonic() + 1)
    assert cursor.committed and creator.evidence is None
    assert sum(text.startswith("COMMIT") for text, _ in cursor.calls) == 1


@pytest.mark.parametrize("fault", [(0, True), (1, "x" * 129), (3, 1), (4, 9), (5, 20), (6, 1), (8, 1), (9, True)])
def test_bad_catalog_cannot_be_accepted(fault):
    cursor, sql, creator = setup()
    cursor.metadata_fault = fault
    with pytest.raises(TdsCreateUnknown):
        creator.execute(request(), grant(sql), deadline=monotonic() + 1)
    assert not cursor.committed and creator.evidence is None


def test_full_evidence_survives_final_check_and_close_failure(monkeypatch):
    cursor, sql, creator = setup()
    original = sql.require_authority

    def check(**kwargs):
        if creator.evidence is not None:
            raise RuntimeError("late authority failure")
        return original(**kwargs)

    monkeypatch.setattr(sql, "require_authority", check)
    with pytest.raises(TdsCreateUnknown) as raised:
        creator.execute(request(), grant(sql), deadline=monotonic() + 1)
    assert raised.value.evidence is creator.evidence and creator.evidence is not None
    monkeypatch.setattr(sql, "close", lambda: (_ for _ in ()).throw(RuntimeError("close")))
    with pytest.raises(TdsCreateUnknown) as closed:
        creator.close()
    assert closed.value.evidence is creator.evidence


def test_late_create_return_cannot_start_property_write(monkeypatch):
    cursor, sql, creator = setup()
    now = [0.0]
    sql._clock = lambda: now[0]
    execute = cursor.execute

    def late(text, *args):
        result = execute(text, *args)
        if text.startswith("CREATE TABLE"):
            now[0] = 11.0
        return result

    monkeypatch.setattr(cursor, "execute", late)
    with pytest.raises(TdsCreateUnknown):
        creator.execute(request(), grant(sql), deadline=10.0)
    assert cursor.created and not cursor.properties and not cursor.committed


def test_existing_object_and_nonempty_post_commit_do_not_claim_success():
    cursor, sql, creator = setup()
    cursor.created = True
    with pytest.raises(TdsCreateUnknown):
        creator.execute(request(), grant(sql), deadline=monotonic() + 1)
    assert not any(text.startswith("BEGIN") for text, _ in cursor.calls)
    cursor, sql, creator = setup()
    cursor.empty = 0
    with pytest.raises(TdsCreateUnknown):
        creator.execute(request(), grant(sql), deadline=monotonic() + 1)
    assert cursor.committed and creator.evidence is None


def test_catalog_read_is_bounded_and_extra_column_rejected(monkeypatch):
    cursor, sql, creator = setup()
    execute = cursor.execute

    def extra(text, *args):
        result = execute(text, *args)
        if text == _COLUMNS:
            cursor.rows.append(list(cursor.rows[0]))
        return result

    monkeypatch.setattr(cursor, "execute", extra)
    with pytest.raises(TdsCreateUnknown):
        creator.execute(request(), grant(sql), deadline=monotonic() + 1)
    assert "TOP (101)" in _COLUMNS and not cursor.committed


def test_observed_incarnation_change_after_commit_retains_unknown(monkeypatch):
    cursor, sql, creator = setup()
    execute = cursor.execute

    def changed(text, *args):
        result = execute(text, *args)
        if text == _OBJECT and cursor.committed:
            row = list(cursor.rows[0])
            row[0] = 11
            cursor.rows = [row]
        return result

    monkeypatch.setattr(cursor, "execute", changed)
    with pytest.raises(TdsCreateUnknown):
        creator.execute(request(), grant(sql), deadline=monotonic() + 1)
    assert cursor.committed and creator.evidence is None
