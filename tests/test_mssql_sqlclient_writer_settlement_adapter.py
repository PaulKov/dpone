"""P10f adapter keeps departure and typed readback on one bounded cursor."""

from dataclasses import replace
from hashlib import sha256

import pytest

from dpone.adapters.mssql_sqlclient_create_exclusion_v2_sql import (
    CONNECTIONS_SQL,
    REQUESTS_SQL,
    SESSIONS_SQL,
    TRANSACTIONS_SQL,
)
from dpone.adapters.mssql_sqlclient_writer_settlement import SqlClientWriterSettlementObserver
from dpone.contracts.mssql_native_chunks import TdsInputReceipt
from dpone.contracts.mssql_sqlclient_input import SqlClientFileIdentity
from dpone.contracts.mssql_sqlclient_observation import (
    SqlClientObserverAdmission,
    SqlClientPrincipalResolution,
    SqlClientSessionAuthority,
)
from dpone.contracts.mssql_sqlclient_writer_settlement import SqlClientStageContentExpectation
from dpone.runtime.mssql_native_chunks_files import native_multiset_digest
from tests.mssql_sqlclient_departure_v2_fixtures import sample
from tests.mssql_sqlclient_evidence_fixtures import registration, writer
from tests.test_mssql_sqlclient_stage_identity import stage


class Cursor:
    def __init__(self, rows):
        self.data_rows = rows
        self.rows = []
        self.closed = 0

    def execute(self, sql, *parameters):
        del parameters
        if sql == CONNECTIONS_SQL:
            self.rows = [(0, 0, 0)]
        elif sql == SESSIONS_SQL:
            self.rows = [(0, 0)]
        elif sql == REQUESTS_SQL:
            self.rows = [(0, 0, 0, None, None, None, None)]
        elif sql == TRANSACTIONS_SQL:
            self.rows = [(0,)]
        elif "HASHBYTES('SHA2_256'" in sql:
            hashes = tuple(sha256(int(row[0]).to_bytes(8, "little", signed=True)).digest() for row in self.data_rows)
            words = tuple(
                sum(int.from_bytes(value[offset : offset + 4], "big") for value in hashes) for offset in range(0, 32, 4)
            )
            self.rows = [(len(hashes), len(hashes), *words)]
        elif "COUNT_BIG" in sql:
            self.rows = [(len(self.data_rows),)]
        elif sql.startswith("SELECT ["):
            self.rows = list(self.data_rows)
        else:
            self.rows = []
        return self

    def fetchone(self):
        return self.rows.pop(0) if self.rows else None

    def nextset(self):
        return False

    def close(self):
        self.closed += 1


def _inputs(value=7):
    record = writer()
    original = registration().input
    column = original.columns[0]
    payload = int(value).to_bytes(8, "little", signed=True)
    typed = native_multiset_digest(1, int.from_bytes(sha256(payload).digest(), "big"))
    descriptor = replace(
        original,
        columns=(column,),
        expected=TdsInputReceipt(1, 8, "8" * 64),
        file_identity=SqlClientFileIdentity(1, 2, 8, 3, 4),
    )
    observed_stage = stage()
    observed_stage = replace(
        observed_stage,
        database_guid=__import__("uuid").UUID(record.authority.database.database_guid),
        database_id=record.authority.database.database_id,
        database_name=record.authority.database.database_name,
        columns=(
            replace(
                observed_stage.columns[0],
                name=column.name,
                nullable=False,
            ),
        ),
    )
    expectation = SqlClientStageContentExpectation(1, "8" * 64, typed)
    writer_admission = SqlClientObserverAdmission(
        record.authority.server,
        record.authority.database,
        record.authority.login,
        record.authority.transport,
    )
    base = sample(reused=False).observer
    management_login = replace(
        base.authority.login,
        principal_id=base.authority.login.principal_id + 1,
        name="management",
        sid="cc",
        original_name="management",
        original_sid="cc",
    )
    management_authority = SqlClientSessionAuthority(
        record.authority.server,
        record.authority.database,
        management_login,
        base.authority.transport,
        SqlClientPrincipalResolution("mapped_user", 301, "management", "cc"),
    )
    management = replace(
        base,
        authority=management_authority,
        visibility=replace(base.visibility, database_id=record.authority.database.database_id),
    )
    management_admission = SqlClientObserverAdmission(
        management.authority.server,
        management.authority.database,
        management.authority.login,
        management.authority.transport,
    )
    return record, writer_admission, management_admission, management, observed_stage, descriptor, expectation


def test_happy_path_computes_exact_target_local_digest_under_transaction(monkeypatch):
    record, writer_admission, management_admission, management, observed_stage, descriptor, expectation = _inputs()
    cursor = Cursor([(7,)])
    monkeypatch.setattr(
        "dpone.adapters.mssql_sqlclient_writer_settlement.capture_observer_incarnation",
        lambda query, admission: management,
    )
    monkeypatch.setattr(
        "dpone.adapters.mssql_sqlclient_stage_catalog.SqlClientStageObserver.catalog_in_existing_session",
        lambda self, expected, **kwargs: (expected, (1, 1, 1)),
    )
    observer = SqlClientWriterSettlementObserver(
        cursor,
        management_admission=management_admission,
        operation_deadline_ns=100,
        operation_deadline=1.0,
        monotonic_ns=lambda: 1,
        finalize_digest=native_multiset_digest,
        close_connection=cursor.close,
    )

    result = observer.observe(
        writer_observation=record,
        writer_admission=writer_admission,
        stage=observed_stage,
        input_descriptor=descriptor,
        expectation=expectation,
        operation_deadline=1.0,
    )

    assert result.row_count == 1
    assert result.typed_digest == expectation.typed_digest
    assert result.typed_sum == int.from_bytes(sha256((7).to_bytes(8, "little", signed=True)).digest(), "big")
    assert result.departure.original == record.remote_session
    observer.close()
    assert cursor.closed == 1


def test_content_mismatch_is_fail_closed(monkeypatch):
    record, writer_admission, management_admission, management, observed_stage, descriptor, expectation = _inputs()
    cursor = Cursor([(9,)])
    monkeypatch.setattr(
        "dpone.adapters.mssql_sqlclient_writer_settlement.capture_observer_incarnation",
        lambda query, admission: management,
    )
    monkeypatch.setattr(
        "dpone.adapters.mssql_sqlclient_stage_catalog.SqlClientStageObserver.catalog_in_existing_session",
        lambda self, expected, **kwargs: (expected, (1, 1, 1)),
    )
    observer = SqlClientWriterSettlementObserver(
        cursor,
        management_admission=management_admission,
        operation_deadline_ns=100,
        operation_deadline=1.0,
        monotonic_ns=lambda: 1,
        finalize_digest=native_multiset_digest,
        close_connection=cursor.close,
    )

    with pytest.raises(RuntimeError):
        observer.observe(
            writer_observation=record,
            writer_admission=writer_admission,
            stage=observed_stage,
            input_descriptor=descriptor,
            expectation=expectation,
            operation_deadline=1.0,
        )
    observer.close()


def test_replay_and_second_close_are_rejected(monkeypatch):
    record, writer_admission, management_admission, management, observed_stage, descriptor, expectation = _inputs()
    cursor = Cursor([(7,)])
    monkeypatch.setattr(
        "dpone.adapters.mssql_sqlclient_writer_settlement.capture_observer_incarnation",
        lambda query, admission: management,
    )
    monkeypatch.setattr(
        "dpone.adapters.mssql_sqlclient_stage_catalog.SqlClientStageObserver.catalog_in_existing_session",
        lambda self, expected, **kwargs: (expected, (1, 1, 1)),
    )
    observer = SqlClientWriterSettlementObserver(
        cursor,
        management_admission=management_admission,
        operation_deadline_ns=100,
        operation_deadline=1.0,
        monotonic_ns=lambda: 1,
        finalize_digest=native_multiset_digest,
        close_connection=cursor.close,
    )
    kwargs = dict(
        writer_observation=record,
        writer_admission=writer_admission,
        stage=observed_stage,
        input_descriptor=descriptor,
        expectation=expectation,
        operation_deadline=1.0,
    )
    observer.observe(**kwargs)
    with pytest.raises(RuntimeError):
        observer.observe(**kwargs)
    observer.close()
    with pytest.raises(RuntimeError):
        observer.close()
