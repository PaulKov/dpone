"""Real SQL Server atomicity under UoW; authority providers remain test doubles."""

import hashlib
import os
from contextlib import closing
from dataclasses import replace
from datetime import timedelta
from uuid import uuid4

import pytest

from dpone.adapters.mssql_r1_v3_effect import MssqlR1BatchEffectV3
from dpone.adapters.mssql_r1_v3_receipt import MssqlR1V3ReceiptBuilder
from dpone.contracts.mssql_r1_v3_receipt import MssqlR1ReceiptObservationV3
from dpone.contracts.mssql_r1_v3_transaction import MssqlR1SqlServerSessionIdentityV3
from dpone.runtime.sinks.strategies.mssql.mssql_r1_v3_effect_uow import (
    MssqlR1V3EffectOutcome,
    MssqlR1V3EffectUnitOfWork,
)
from tests.test_postgres_mssql_r1_v3_contracts import NOW, _batch_request, _transaction_binding
from tests.test_postgres_mssql_r1_v3_runtime import _Attestor, _Authority, _Candidate, _Fence, _Quality, _Stages

pytestmark = [pytest.mark.integration_live, pytest.mark.integration_mssql]


def connect():
    import pyodbc

    assert os.environ["DPONE_IT_MSSQL_HOST"] == "mssql"
    assert os.environ["DPONE_IT_MSSQL_DATABASE"] == "dpone_it"

    def escaped(value):
        return "{" + value.replace("}", "}}") + "}"

    connection = pyodbc.connect(
        "DRIVER={ODBC Driver 18 for SQL Server};SERVER=mssql,1433;DATABASE=dpone_it;"
        "UID=sa;PWD=" + escaped(os.environ["DPONE_IT_MSSQL_PASSWORD"]) + ";TrustServerCertificate=yes;",
        autocommit=True,
        timeout=5,
    )
    connection.timeout = 5
    try:
        connection.execute("SET LOCK_TIMEOUT 3000; SET XACT_ABORT ON")
    except BaseException:
        connection.close()
        raise
    return connection


class Transaction:
    def __init__(self, connection):
        self.connection = connection
        self.closed = False
        self.dispatched = False
        row = connection.execute(
            "SELECT @@SPID, DB_ID(), CAST(SERVERPROPERTY('ServerName') AS nvarchar(128))"
        ).fetchone()
        recovery = connection.execute(
            "SELECT database_guid, family_guid, recovery_fork_guid FROM sys.database_recovery_status WHERE database_id=DB_ID()"
        ).fetchone()
        from uuid import UUID

        transaction_id = uuid4()
        connection.execute("EXEC sys.sp_set_session_context @key=N'private_uow_id', @value=?", str(transaction_id))
        session = MssqlR1SqlServerSessionIdentityV3(
            hashlib.sha256(row[2].encode()).digest(),
            row[1],
            *(UUID(str(value)) for value in recovery),
            row[0],
            transaction_id,
        )
        self.binding = replace(
            _transaction_binding(),
            transaction_id=transaction_id,
            session_identity_bytes=session.canonical_bytes,
            session_identity_digest=session.digest,
        )
        self.spid = row[0]

    @property
    def transaction_id(self):
        return self.binding.transaction_id

    def assert_active(self):
        assert not self.closed and not self.dispatched
        self.binding.assert_active()
        row = self.connection.execute(
            "SELECT @@SPID, @@TRANCOUNT, XACT_STATE(), CAST(SESSION_CONTEXT(N'private_uow_id') AS nvarchar(36))"
        ).fetchone()
        assert tuple(row) == (self.spid, 1, 1, str(self.transaction_id))


class Session:
    def __init__(self, events):
        self.events = events
        self.connection = connect()
        self.transaction = None
        self.close_succeeded = False

    def begin(self):
        self.events.append("begin")
        self.connection.execute("BEGIN TRANSACTION")
        self.transaction = Transaction(self.connection)
        return self.transaction

    def assert_active(self, transaction):
        assert transaction is self.transaction
        transaction.assert_active()

    def dispatch_commit(self, transaction):
        self.assert_active(transaction)
        self.events.append("commit")
        transaction.dispatched = True
        self.connection.execute("COMMIT TRANSACTION")
        assert tuple(self.connection.execute("SELECT @@TRANCOUNT, XACT_STATE()").fetchone()) == (0, 0)

    def rollback(self, transaction):
        assert transaction is self.transaction and not transaction.closed and not transaction.dispatched
        observed = self.connection.execute("SELECT @@SPID, @@TRANCOUNT, XACT_STATE()").fetchone()
        assert observed[0] == transaction.spid and observed[1] in (0, 1) and observed[2] in (-1, 0, 1)
        self.rollback_observed_state = observed[2]
        self.rollback_observed_count = observed[1]
        self.events.append("rollback")
        if observed[1]:
            self.connection.execute("ROLLBACK TRANSACTION")
        assert self.connection.execute("SELECT @@TRANCOUNT").fetchval() == 0

    def close(self):
        self.events.append("close")
        if self.transaction is not None:
            self.transaction.closed = True
        self.connection.close()
        self.close_succeeded = True


class Sessions:
    def __init__(self, events):
        self.events = events
        self.opened = []

    def open(self):
        session = Session(self.events)
        self.opened.append(session)
        return session


class SqlMutation:
    def __init__(self, table, events, failure):
        self.table, self.events, self.failure = table, events, failure

    def mutate(self, transaction, attempt):
        transaction.assert_active()
        self.events.append("mutation")
        transaction.connection.execute(f"DELETE FROM {self.table}")
        for ordinal in range(attempt.request.artifacts[0].observed_row_count):
            transaction.connection.execute(f"INSERT INTO {self.table} (id, payload) VALUES (?, ?)", ordinal, b"new")

        if self.failure == "sql_auto_abort":
            # A genuine SQL constraint error may be rolled back by SQL Server itself.
            transaction.connection.execute(f"INSERT INTO {self.table} SELECT TOP (1) id, payload FROM {self.table}")

    def update_row_hashes(self, transaction, attempt):
        transaction.assert_active()


class Quality(_Quality):
    def __init__(self, events, attempt, table, failure):
        super().__init__(events, attempt)
        self.table, self.failure = table, failure

    def evaluate(self, transaction, attempt):
        transaction.assert_active()
        assert (
            transaction.connection.execute(f"SELECT COUNT(*) FROM {self.table}").fetchval()
            == attempt.request.artifacts[0].observed_row_count
        )
        if self.failure == "quality":
            raise RuntimeError("injected quality failure after SQL mutation")
        return super().evaluate(transaction, attempt)


class Appender:
    def __init__(self, table, failure):
        self.table, self.failure, self.receipt = table, failure, None

    def append(self, transaction, receipt):
        transaction.assert_active()
        transaction.connection.execute(f"INSERT INTO {self.table} (payload) VALUES (?)", receipt.canonical_bytes)
        self.receipt = receipt
        if self.failure == "evidence":
            raise RuntimeError("injected evidence failure after SQL insert")


class Observations:
    def observe(self, transaction, attempt, quality):
        transaction.assert_active()
        return MssqlR1ReceiptObservationV3(NOW + timedelta(minutes=2), 2)


class NoFreshProof:
    def open(self):
        raise AssertionError("fresh proof forbidden before commit dispatch or after successful commit")


@pytest.mark.parametrize("failure", ["quality", "evidence", "sql_auto_abort", None])
def test_real_sql_effect_and_evidence_atomicity(failure):
    if os.environ.get("DPONE_RUN_INTEGRATION_LIVE") != "1":
        pytest.skip("Explicit local Docker integration environment required")
    name = "private_uow_" + uuid4().hex
    business, evidence = f"[dbo].[{name}_business]", f"[dbo].[{name}_evidence]"
    with closing(connect()) as control:
        try:
            control.execute(f"CREATE TABLE {business} (id int PRIMARY KEY, payload varbinary(64) NOT NULL)")
            control.execute(f"CREATE TABLE {evidence} (payload varbinary(max) NOT NULL)")
            control.execute(f"INSERT INTO {business} VALUES (100, 0x6f6c64), (101, 0x6f6c64)")
            baseline = [(100, b"old"), (101, b"old")]
            attempt = _batch_request()[1]
            events = []
            sessions = Sessions(events)
            authority, stages = _Authority(events), _Stages(events)
            appender = Appender(evidence, failure)
            uow = MssqlR1V3EffectUnitOfWork(
                sessions=sessions,
                stage_attestor=_Attestor(events),
                generation_authority=authority,
                stages=stages,
                writer_fence=_Fence(events),
                candidate=_Candidate(events, authority, stages),
                mode=MssqlR1BatchEffectV3(
                    SqlMutation(business, events, failure), Quality(events, attempt, business, failure)
                ),
                receipt_builder=MssqlR1V3ReceiptBuilder(Observations()),
                receipt_appender=appender,
                fresh_proofs=NoFreshProof(),
            )
            if failure == "sql_auto_abort":
                import pyodbc

                with pytest.raises(pyodbc.IntegrityError):
                    uow.execute(attempt)
                assert sessions.opened[0].rollback_observed_count == 0
            elif failure:
                with pytest.raises(RuntimeError, match="injected " + failure + " failure"):
                    uow.execute(attempt)
            else:
                result = uow.execute(attempt)
                assert result.outcome is MssqlR1V3EffectOutcome.COMMITTED
            assert len(sessions.opened) == 1 and sessions.opened[0].transaction.closed
            assert sessions.opened[0].close_succeeded
            assert events.count("mutation") == events.count("close") == 1
            assert events.count("commit") == (0 if failure else 1)
            assert events.count("rollback") == (1 if failure else 0)
            with closing(connect()) as observer:
                actual = [tuple(row) for row in observer.execute(f"SELECT id, payload FROM {business} ORDER BY id")]
                receipts = [bytes(row[0]) for row in observer.execute(f"SELECT payload FROM {evidence}")]
                if failure:
                    assert actual == baseline and receipts == []
                else:
                    assert actual == [(i, b"new") for i in range(attempt.request.artifacts[0].observed_row_count)]
                    assert receipts == [result.receipt.canonical_bytes] == [appender.receipt.canonical_bytes]
        finally:
            try:
                control.execute(f"DROP TABLE IF EXISTS {evidence}")
            finally:
                control.execute(f"DROP TABLE IF EXISTS {business}")
