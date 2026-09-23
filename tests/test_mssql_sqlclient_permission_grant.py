"""Actual retained SQL owner with scripted catalog/transaction boundaries."""

from copy import copy
from dataclasses import replace
from datetime import datetime
from time import monotonic
from uuid import UUID

import pytest

from dpone.adapters.mssql_sqlclient_permission_grant import (
    MANAGEMENT_SQL,
    OWNERS_SQL,
    PERMISSIONS_SQL,
    WRITER_SQL,
    SqlClientPermissionGrant,
    SqlClientPermissionGrantUnknown,
)
from dpone.adapters.mssql_sqlclient_stage_catalog_sql import (
    COLUMNS_SQL,
    FEATURES_SQL,
    OBJECT_SQL,
    SCHEMA_SQL,
    STAGE_VISIBILITY_SQL,
)
from dpone.adapters.mssql_tds_coordinator_connection import TdsSqlConnection
from dpone.adapters.mssql_tds_coordinator_sql import TdsCoordinatorSql
from dpone.contracts.mssql_sqlclient_observation import SqlClientLoginAuthority, SqlClientPrincipalResolution
from dpone.contracts.mssql_sqlclient_permission_grant import (
    EVIDENCE_LIMIT,
    REQUEST_LIMIT,
    SqlClientPermissionGrantRequest,
    decode_permission_grant_evidence,
    decode_permission_grant_request,
    encode_permission_grant_evidence,
    encode_permission_grant_request,
    permission_grant_digest,
)
from dpone.contracts.mssql_sqlclient_stage_identity import SqlClientStageIdentity
from dpone.contracts.mssql_tds_coordinator import coordinator_identity_digest
from dpone.contracts.mssql_tds_coordinator_authority import authority_digest
from dpone.contracts.mssql_tds_create import TdsCreateObservedColumn, TdsCreateType
from dpone.contracts.mssql_tds_directory import TdsCoordinatorCommand
from tests.test_mssql_tds_coordinator import PROCESS, identity
from tests.test_mssql_tds_coordinator_sql import Cursor, grant
from tests.test_mssql_tds_directory_journal import OWNER
from tests.test_mssql_tds_session import NONCE


class TextAlias(str):
    pass


class RawConnection:
    def __init__(self):
        self.closed = 0
        self.fail_close = False

    def close(self):
        self.closed += 1
        if self.fail_close:
            raise RuntimeError("PRIVATE_CLOSE_ERROR")


def stage():
    return SqlClientStageIdentity(
        database_guid=UUID(int=5),
        database_id=5,
        database_name="db",
        schema_id=1,
        schema_name="schema",
        table_name="stage",
        object_id=10,
        create_date=datetime(2026, 1, 1),
        owner_binding="a" * 64,
        object_nonce=UUID(int=2),
        columns=(TdsCreateObservedColumn(1, "value", TdsCreateType.BIGINT, True, 8, 19, 0, None),),
    )


def request():
    selected = stage()
    parent = replace(identity().parent, database="db", schema="schema", table="stage", owner_binding="a" * 64)
    return SqlClientPermissionGrantRequest(
        parent=parent,
        stage=selected,
        writer=SqlClientPrincipalResolution("mapped_user", 5, "writer", b"writer".hex()),
        writer_login=SqlClientLoginAuthority(30, "writer", b"writer".hex(), "writer", b"writer".hex(), 1, False),
        management=SqlClientPrincipalResolution("sysadmin_dbo", 1, "dbo", b"dbo".hex()),
        management_login=SqlClientLoginAuthority(20, "sa", b"sa".hex(), "sa", b"sa".hex(), 1, True),
        preparation_sha256="d" * 64,
    )


class PermissionCursor(Cursor):
    def __init__(self):
        super().__init__()
        self.granted = self.committed = False
        self.fail_on = None
        self.after_effect = False
        self.permission_fault = self.writer_fault = self.owner_fault = None
        self.object_fault = None
        self.reenter = None

    def permission_rows(self):
        if not self.granted:
            return []
        rows = [
            [1, 10, 0, None, "G", permission, 5, "writer", b"writer", "SQL_USER", 1, "dbo", b"dbo", "SQL_USER"]
            for permission in ("INSERT", "SELECT", "VIEW DEFINITION")
        ]
        if self.permission_fault is not None:
            kind, value = self.permission_fault
            if kind == "field":
                rows[0][value[0]] = value[1]
            elif kind == "extra":
                rows.append(list(rows[0]))
            elif kind == "missing":
                rows.pop()
        return rows

    def execute(self, sql, *args):
        business = (
            STAGE_VISIBILITY_SQL,
            SCHEMA_SQL,
            OBJECT_SQL,
            FEATURES_SQL,
            COLUMNS_SQL,
            WRITER_SQL,
            MANAGEMENT_SQL,
            OWNERS_SQL,
            PERMISSIONS_SQL,
        )
        if sql not in business and not sql.startswith(("BEGIN TRANSACTION", "GRANT INSERT", "COMMIT TRANSACTION")):
            return super().execute(sql, *args)
        self.calls.append((sql, args))
        self.rows = []
        self.description = None if sql.startswith(("BEGIN", "GRANT", "COMMIT")) else (("column",),)
        if self.fail_on and self.fail_on in sql and not self.after_effect:
            raise RuntimeError("PRIVATE_DRIVER_ERROR")
        if sql == STAGE_VISIBILITY_SQL:
            self.rows = [(16, 1, 1, 1)]
        elif sql == SCHEMA_SQL:
            self.rows = [(1, "schema")]
        elif sql == OBJECT_SQL:
            row = [10, "stage", datetime(2026, 1, 1), "a" * 64, str(UUID(int=2)), 0, 0, 0, 0]
            if self.object_fault:
                row[self.object_fault[0]] = self.object_fault[1]
            self.rows = [tuple(row)]
        elif sql == FEATURES_SQL:
            self.rows = [tuple(0 for _ in range(12))]
        elif sql == COLUMNS_SQL:
            self.rows = [(1, "value", "bigint", True, 8, 19, 0, None, 0, False, False)]
        elif sql == WRITER_SQL:
            self.rows = [(5, "writer", b"writer", "SQL_USER", "INSTANCE", 30, "writer", b"writer", "SQL_LOGIN", 0)]
            if self.writer_fault:
                row = list(self.rows[0])
                row[self.writer_fault[0]] = self.writer_fault[1]
                self.rows = [tuple(row)]
        elif sql == MANAGEMENT_SQL:
            self.rows = [(20, "sa", b"sa", "sa", b"sa", "SQL_LOGIN", 0, 1, 1, "dbo", b"dbo", "SQL_USER", 1)]
        elif sql == OWNERS_SQL:
            self.rows = [(b"dbo", 1, b"dbo", "dbo", "SQL_USER", None, None, None, None)]
            if self.owner_fault:
                row = list(self.rows[0])
                row[self.owner_fault[0]] = self.owner_fault[1]
                self.rows = [tuple(row)]
        elif sql == PERMISSIONS_SQL:
            self.rows = self.permission_rows()
        elif sql.startswith("BEGIN"):
            self.transaction = 1
        elif sql.startswith("GRANT"):
            self.granted = True
        else:
            self.committed = True
            self.transaction = 0
        if self.reenter is not None:
            callback, self.reenter = self.reenter, None
            try:
                callback()
            except SqlClientPermissionGrantUnknown:
                pass
        if self.fail_on and self.fail_on in sql and self.after_effect:
            raise RuntimeError("PRIVATE_DRIVER_ERROR")


def setup():
    req = request()
    cursor, raw = PermissionCursor(), RawConnection()
    operation = replace(
        identity(), parent=req.parent, command=TdsCoordinatorCommand.GRANT, command_sha256=permission_grant_digest(req)
    )
    sql = TdsCoordinatorSql(TdsSqlConnection(raw, cursor), operation, OWNER, PROCESS)
    sql.acquire(NONCE, deadline=monotonic() + 5)
    return cursor, raw, sql, req, SqlClientPermissionGrant(sql)


def test_request_and_evidence_are_canonical_closed_records():
    _, _, sql, req, producer = setup()
    evidence = producer.execute(req, grant(sql), deadline=monotonic() + 5)
    request_bytes, evidence_bytes = encode_permission_grant_request(req), encode_permission_grant_evidence(evidence)
    assert decode_permission_grant_request(request_bytes) == req
    assert decode_permission_grant_evidence(evidence_bytes) == evidence
    assert len(request_bytes) <= REQUEST_LIMIT and len(evidence_bytes) <= EVIDENCE_LIMIT
    with pytest.raises(ValueError):
        decode_permission_grant_request(request_bytes + b" ")
    with pytest.raises(ValueError):
        decode_permission_grant_evidence(evidence_bytes + b" ")
    with pytest.raises(ValueError):
        decode_permission_grant_request(b" " * (REQUEST_LIMIT + 1))
    forged = request()
    object.__setattr__(forged.stage, "object_id", True)
    with pytest.raises(ValueError):
        encode_permission_grant_request(forged)


@pytest.mark.parametrize("field", ["database_guid", "object_nonce"])
def test_stage_uuid_integer_alias_is_rejected_before_serialization(field):
    forged = request()
    object.__setattr__(getattr(forged.stage, field), "int", True)
    with pytest.raises(ValueError):
        encode_permission_grant_request(forged)


def test_session_uuid_integer_alias_is_rejected_before_binding_digest():
    _, _, sql, req, producer = setup()
    execution = grant(sql)
    connection_id = UUID(str(execution.session.connection_id))
    object.__setattr__(connection_id, "int", True)
    execution = replace(execution, session=replace(execution.session, connection_id=connection_id))
    before = len(producer._cursor.calls)
    with pytest.raises(SqlClientPermissionGrantUnknown):
        producer.execute(req, execution, deadline=monotonic() + 5)
    assert len(producer._cursor.calls) == before


@pytest.mark.parametrize("field", ["resource", "principal", "owner", "mode"])
def test_lock_text_alias_is_rejected_before_binding_digest(field):
    _, _, sql, req, producer = setup()
    object.__setattr__(sql.authority.lock, field, TextAlias(getattr(sql.authority.lock, field)))
    before = len(producer._cursor.calls)
    with pytest.raises(SqlClientPermissionGrantUnknown):
        producer.execute(req, grant(sql), deadline=monotonic() + 5)
    assert len(producer._cursor.calls) == before


def test_one_transaction_retains_exact_permissions_and_live_authority():
    cursor, raw, sql, req, producer = setup()
    evidence = producer.execute(req, grant(sql), deadline=monotonic() + 5)
    commands = [text for text, _ in cursor.calls]
    assert sum(text.startswith("GRANT INSERT") for text in commands) == 1
    assert commands.count("BEGIN TRANSACTION;") == commands.count("COMMIT TRANSACTION;") == 1
    assert evidence is producer.evidence and len(evidence.direct_permissions) == 3
    assert sql.connection._closed is False and raw.closed == 0 and cursor.closed is False
    assert producer.require_held(deadline=producer._deadline) is evidence
    producer.close()
    assert raw.closed == 1 and cursor.closed is True
    with pytest.raises(SqlClientPermissionGrantUnknown):
        producer.close()
    assert raw.closed == 1


@pytest.mark.parametrize("binding", ["request", "operation", "grant", "authority"])
def test_wrong_original_binding_never_starts_business_sql(binding):
    cursor, _, sql, req, producer = setup()
    execution = grant(sql)
    if binding == "request":
        req = replace(req, preparation_sha256="e" * 64)
    elif binding == "operation":
        sql.identity = replace(sql.identity, operation_id=UUID(int=88))
    elif binding == "grant":
        execution = replace(execution, authority_sha256="e" * 64)
    else:
        sql.authority = replace(sql.authority, implementation_sha256="e" * 64)
    before = len(cursor.calls)
    with pytest.raises(SqlClientPermissionGrantUnknown):
        producer.execute(req, execution, deadline=monotonic() + 5)
    assert len(cursor.calls) == before


def test_shallow_sql_owner_copy_cannot_bypass_original_poison():
    cursor, _, sql, req, producer = setup()
    substitute = copy(sql)
    sql._poisoned = True
    producer.sql = substitute
    before = len(cursor.calls)
    with pytest.raises(SqlClientPermissionGrantUnknown):
        producer.execute(req, grant(substitute), deadline=monotonic() + 5)
    assert len(cursor.calls) == before and sql._poisoned is True


@pytest.mark.parametrize(
    "statement",
    [STAGE_VISIBILITY_SQL, WRITER_SQL, OWNERS_SQL, "BEGIN TRANSACTION", "GRANT INSERT", "COMMIT TRANSACTION"],
)
def test_fault_is_sticky_and_never_replays_or_compensates(statement):
    cursor, _, sql, req, producer = setup()
    cursor.fail_on = statement
    with pytest.raises(SqlClientPermissionGrantUnknown):
        producer.execute(req, grant(sql), deadline=monotonic() + 5)
    count = len(cursor.calls)
    with pytest.raises(SqlClientPermissionGrantUnknown):
        producer.execute(req, grant(sql), deadline=monotonic() + 5)
    assert len(cursor.calls) == count
    assert sum(text.startswith("GRANT INSERT") for text, _ in cursor.calls) <= 1
    assert not any("REVOKE" in text or "ROLLBACK" in text for text, _ in cursor.calls)


def test_lost_commit_ack_retains_unknown_without_regrant():
    cursor, _, sql, req, producer = setup()
    cursor.fail_on, cursor.after_effect = "COMMIT TRANSACTION", True
    with pytest.raises(SqlClientPermissionGrantUnknown) as caught:
        producer.execute(req, grant(sql), deadline=monotonic() + 5)
    assert cursor.committed and caught.value.retained is producer
    assert producer.commit_attempted and producer.evidence is None
    assert sum(text.startswith("GRANT INSERT") for text, _ in cursor.calls) == 1


@pytest.mark.parametrize(
    "fault",
    [
        ("permission", ("missing", None)),
        ("permission", ("extra", None)),
        ("permission", ("field", (4, "W"))),
        ("permission", ("field", (2, 1))),
        ("writer", (2, b"other")),
        ("writer", (9, 1)),
        ("owner", (0, b"writer")),
        ("owner", (1, 5)),
    ],
)
def test_changed_permission_principal_or_owner_cannot_commit(fault):
    cursor, _, sql, req, producer = setup()
    target, value = fault
    setattr(cursor, target + "_fault", value)
    with pytest.raises(SqlClientPermissionGrantUnknown):
        producer.execute(req, grant(sql), deadline=monotonic() + 5)
    assert not cursor.committed


def test_preexisting_direct_permission_rejects_before_transaction():
    cursor, _, sql, req, producer = setup()
    cursor.granted = True
    with pytest.raises(SqlClientPermissionGrantUnknown):
        producer.execute(req, grant(sql), deadline=monotonic() + 5)
    assert not any(text.startswith("BEGIN") for text, _ in cursor.calls)


@pytest.mark.parametrize("fault", [(0, 11), (2, datetime(2026, 1, 1, 0, 0, 0, 1)), (4, str(UUID(int=3)))])
def test_recreated_or_changed_stage_is_rejected_before_transaction(fault):
    cursor, _, sql, req, producer = setup()
    cursor.object_fault = fault
    with pytest.raises(SqlClientPermissionGrantUnknown):
        producer.execute(req, grant(sql), deadline=monotonic() + 5)
    assert not any(text.startswith("BEGIN") for text, _ in cursor.calls)


def test_resultset_uncertainty_and_caught_reentry_poison_original():
    cursor, _, sql, req, producer = setup()
    cursor.completion = True
    with pytest.raises(SqlClientPermissionGrantUnknown):
        producer.execute(req, grant(sql), deadline=monotonic() + 5)
    cursor, _, sql, req, producer = setup()
    cursor.reenter = lambda: producer.require_held(deadline=monotonic() + 1)
    with pytest.raises(SqlClientPermissionGrantUnknown):
        producer.execute(req, grant(sql), deadline=monotonic() + 5)
    assert not cursor.committed


def test_nonrenewable_deadline_and_postcommit_drift_block_forward_use():
    cursor, _, sql, req, producer = setup()
    evidence = producer.execute(req, grant(sql), deadline=monotonic() + 5)
    with pytest.raises(SqlClientPermissionGrantUnknown):
        producer.require_held(deadline=producer._deadline + 1)
    assert producer.evidence is evidence
    cursor, _, sql, req, producer = setup()
    producer.execute(req, grant(sql), deadline=monotonic() + 5)
    cursor.permission_fault = ("field", (4, "D"))
    with pytest.raises(SqlClientPermissionGrantUnknown):
        producer.require_held(deadline=producer._deadline)


def test_validated_evidence_survives_final_authority_failure(monkeypatch):
    _, _, sql, req, producer = setup()
    original = sql.require_authority

    def fail_after_evidence(**kwargs):
        if producer.evidence is not None:
            raise RuntimeError("PRIVATE_AUTHORITY_ERROR")
        return original(**kwargs)

    monkeypatch.setattr(sql, "require_authority", fail_after_evidence)
    with pytest.raises(SqlClientPermissionGrantUnknown) as caught:
        producer.execute(req, grant(sql), deadline=monotonic() + 5)
    assert caught.value.retained is producer and producer.evidence is not None
    assert producer.phase == "evidence_retained" and cursor_has_single_grant(producer)


def test_field_drift_closes_only_original_resources_once():
    cursor, raw, sql, _, producer = setup()
    replacement_cursor, replacement_raw = PermissionCursor(), RawConnection()
    sql.connection = TdsSqlConnection(replacement_raw, replacement_cursor)
    producer.close()
    assert cursor.closed and raw.closed == 1 and not replacement_cursor.closed and replacement_raw.closed == 0
    with pytest.raises(SqlClientPermissionGrantUnknown):
        producer.close()
    assert raw.closed == 1


def test_close_failure_is_retained_and_never_retried():
    cursor, raw, _, _, producer = setup()
    raw.fail_close = True
    with pytest.raises(SqlClientPermissionGrantUnknown) as caught:
        producer.close()
    assert caught.value.retained is producer and cursor.closed and raw.closed == 1
    with pytest.raises(SqlClientPermissionGrantUnknown):
        producer.close()
    assert raw.closed == 1


def test_operation_and_authority_are_bound_to_request_digest():
    _, _, sql, req, _ = setup()
    execution = grant(sql)
    assert sql.identity.command_sha256 == permission_grant_digest(req)
    assert execution.operation_sha256 == coordinator_identity_digest(sql.identity)
    assert execution.authority_sha256 == authority_digest(sql.authority)


def cursor_has_single_grant(producer):
    return sum(text.startswith("GRANT INSERT") for text, _ in producer._cursor.calls) == 1
