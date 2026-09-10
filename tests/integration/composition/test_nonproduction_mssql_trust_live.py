"""Nine real SQL trust-storage cases for the owned Linux component runner.

This profile exercises original bytes, append/replay, ABA, concurrent next
revisions, drift, permissions and real read-commit ACK loss. Inert documents are
storage fixtures: no signatures, grant consumption, enrollment or worker
execution are certified. Each fault uses only this test's new owned schema.
"""

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
from hashlib import sha256
from threading import Barrier
from uuid import uuid4

import pytest
from tests.integration.composition import nonproduction_mssql_trust_live_support as support
from tests.integration.composition.mssql_gate_live_provisioning import SqlFailure, execute
from tests.nonproduction_signature_helpers import github_policy, trust

from dpone.adapters.composition_mssql_attempts import composition_control_transaction
from dpone.adapters.composition_mssql_schema import COMPOSITION_MSSQL_LEDGER_LOCK
from dpone.adapters.composition_mssql_store_queries import CompositionMssqlLedger
from dpone.adapters.nonproduction_mssql_schema import nonproduction_trust_trigger_sql
from dpone.contracts.nonproduction_scope import NonproductionAuthorityError

pytestmark = [pytest.mark.integration_live, pytest.mark.integration_mssql]
trust_case = support.trust_case


def test_external_trust_ddl_and_original_byte_readback(trust_case):
    case = trust_case
    assert case.rows() == ()
    with pytest.raises(NonproductionAuthorityError):
        case.provider().read_revision()
    assert case.rows() == ()
    case.append(1)
    revision = case.provider().read_revision()
    assert (revision.service_id, revision.environment_id, revision.revision) == (
        case.service_id,
        case.environment_id,
        1,
    )
    assert revision.snapshot == case.snapshot
    assert case.rows() == (
        (
            1,
            case.snapshot.policy_bytes,
            case.snapshot.policy_sha256,
            case.snapshot.verifier_policy_bytes,
            case.snapshot.verifier_policy_sha256,
            case.snapshot.current_revocation_epoch,
        ),
    )
    module = case.sql(
        "SELECT HASHBYTES('SHA2_256',definition),DATALENGTH(definition) FROM sys.sql_modules WHERE object_id=OBJECT_ID(?);",
        case.trigger(),
    )
    expected = nonproduction_trust_trigger_sql(case.schema).encode("utf-16le")
    assert module == ((sha256(expected).digest(), len(expected)),)
    for pins in ({"service_id": str(uuid4())}, {"environment_id": str(uuid4())}):
        with pytest.raises(NonproductionAuthorityError):
            case.provider(**pins).read_revision()
    assert case.provider().read_revision() == revision
    case.record(
        "readback",
        {
            "rows": len(case.rows()),
            "revision": revision.revision,
            "policy_bytes": len(revision.snapshot.policy_bytes),
            "verifier_bytes": len(revision.snapshot.verifier_policy_bytes),
            "policy_sha256": revision.snapshot.policy_sha256,
            "verifier_policy_sha256": revision.snapshot.verifier_policy_sha256,
            "trigger_sha256": "sha256:" + module[0][0].hex(),
            "trigger_bytes": module[0][1],
        },
    )


def test_append_only_revision_rejects_update_delete_and_replay(trust_case):
    case = trust_case
    case.append(1)
    before = case.rows()
    operations = (
        lambda: case.sql(f"UPDATE {case.table()} SET revision=2;"),
        lambda: case.sql(f"DELETE FROM {case.table()};"),
        lambda: case.append(1),
        lambda: case.append(0),
        lambda: case.append(3),
    )
    rejected = []
    for operation in operations:
        rejected.append(support.require_sql_rejection(operation, {51000}))
        assert case.rows() == before
    case.append(2)
    assert tuple(row[0] for row in case.rows()) == (1, 2)
    assert case.provider().read_revision().revision == 2
    case.record("append", {"rows": len(case.rows()), "denied": len(rejected)})


def test_trust_revision_change_blocks_same_ledger_compare(trust_case):
    case = trust_case
    case.append(1)
    before = case.provider().read_revision()
    changed = trust(github_policy(environment_id=case.environment_id, revoked_grant_ids=(str(uuid4()),)))
    case.append(2, changed)
    assert case.provider().read() == changed
    case.append(3)
    current = case.provider().read_revision()
    assert current.snapshot == before.snapshot and current.revision == 3
    with composition_control_transaction(case.database.connect, case.schema, case.service_id) as ledger:
        with pytest.raises(NonproductionAuthorityError, match="trust_revision_changed"):
            case.provider().require_revision_in(ledger, before)
        assert case.provider().require_revision_in(ledger, current) == current
        ledger.cursor.execute("SELECT @@TRANCOUNT,XACT_STATE();")
        assert tuple(tuple(row) for row in ledger.cursor.fetchall()) == ((1, 1),)
        # The external append trigger participates in this same lock domain.
        assert support.require_sql_rejection(lambda: case.append(4), {51000}) == 51000
        assert case.provider().require_revision_in(ledger, current) == current
    assert tuple(row[0] for row in case.rows()) == (1, 2, 3)
    case.record("revision", {"rows": len(case.rows()), "revision": current.revision, "denied": 2})


def test_concurrent_appends_admit_one_next_revision(trust_case):
    case = trust_case
    case.append(1)
    variants = tuple(
        trust(github_policy(environment_id=case.environment_id, revoked_grant_ids=(str(uuid4()),))) for _ in range(2)
    )
    barrier = Barrier(2)

    def append(index):
        with closing(case.database.connect()) as connection:
            barrier.wait(timeout=10)
            try:
                case.append(2, variants[index], connection=connection)
                return index, None
            except SqlFailure as error:
                return index, error.code

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = tuple(pool.map(append, range(2)))
    winners = [index for index, code in outcomes if code is None]
    assert len(winners) == 1
    assert [code for _, code in outcomes if code is not None] == [51000]
    actual = case.provider().read_revision()
    assert actual.revision == 2 and actual.snapshot == variants[winners[0]]
    assert tuple(row[0] for row in case.rows()) == (1, 2)
    case.record("concurrency", {"rows": len(case.rows()), "winners": len(winners), "attempted": len(outcomes)})


def test_invalid_original_hash_or_epoch_cannot_append(trust_case):
    case = trust_case
    case.append(1)
    before = case.rows()
    invalid = (
        replace(case.snapshot, policy_sha256="sha256:" + "0" * 64),
        replace(case.snapshot, verifier_policy_sha256="sha256:" + "0" * 64),
        replace(case.snapshot, current_revocation_epoch=-1),
        replace(case.snapshot, current_revocation_epoch=case.snapshot.current_revocation_epoch - 1),
    )
    for value in invalid:
        support.require_sql_rejection(lambda value=value: case.append(2, value), {51000})
        assert case.rows() == before
    newer = trust(
        github_policy(environment_id=case.environment_id, revocation_epoch=case.snapshot.current_revocation_epoch + 1)
    )
    case.append(2, newer)
    assert case.provider().read_revision().snapshot == newer
    case.record("invalid", {"rows": len(case.rows()), "denied": len(invalid)})


def test_read_rejects_changed_trigger_or_schema(trust_case):
    case = trust_case
    case.append(1)
    before = case.provider().read_revision()
    original = nonproduction_trust_trigger_sql(case.schema)

    def restore_definition():
        case.sql(f"DROP TRIGGER {case.trigger()};")
        case.sql(original)

    faults = (
        (
            lambda: case.sql(f"DISABLE TRIGGER {case.trigger()} ON {case.table()};"),
            lambda: case.sql(f"ENABLE TRIGGER {case.trigger()} ON {case.table()};"),
        ),
        (
            lambda: case.sql(original.replace("CREATE TRIGGER", "ALTER TRIGGER", 1) + "\n-- owned drift"),
            restore_definition,
        ),
        (
            lambda: case.sql(f"ALTER TABLE {case.table()} ADD owned_fault int NULL;"),
            lambda: case.sql(f"ALTER TABLE {case.table()} DROP COLUMN owned_fault;"),
        ),
    )
    restored = 0
    for change, restore in faults:
        with case.fault(change, restore):
            with pytest.raises(NonproductionAuthorityError):
                case.provider().read_revision()
        assert case.provider().read_revision() == before
        restored += 1
    case.record("drift", {"rows": len(case.rows()), "denied": len(faults), "restored": restored})


class DoomedTransactionCursor:
    """Run the untouched first provider query inside a real constraint CATCH.

    SQL Server rolls back doomed transactions at batch end. This one-shot fault
    therefore owns a complete 0→0 transaction inside one parameterized RPC.
    Only acquisition/fault results are consumed here; provider rows are returned
    unchanged, and completion requires draining through explicit rollback.
    See learn.microsoft.com/en-us/sql/t-sql/functions/xact-state-transact-sql.
    """

    def __init__(self, cursor, record):
        self.cursor, self.record = cursor, record
        self.executed = self.complete = self._ready = False
        self.fault_rows: tuple[tuple[object, ...], ...] = ()
        self.precondition_rows: tuple[tuple[object, ...], ...] = ()

    def __getattr__(self, name):
        return getattr(self.cursor, name)

    def execute(self, sql, *parameters):
        if self.executed:
            raise RuntimeError("fault_cursor_reuse")
        if parameters != (COMPOSITION_MSSQL_LEDGER_LOCK,) or parameters[0] != "dpone:composition-control:v1":
            raise RuntimeError("fault_cursor_subject")
        self.executed = True
        prefix = (
            "IF @@TRANCOUNT <> 0 THROW 51000, N'fault_outer_transaction', 1; "
            "DECLARE @fault_options int = @@OPTIONS; SET NOCOUNT ON; SET XACT_ABORT ON; "
            "BEGIN TRANSACTION; DECLARE @fault_lock int; EXEC @fault_lock=sys.sp_getapplock "
            "@Resource=N'dpone:composition-control:v1',@LockOwner=N'Transaction',"
            "@LockMode=N'Exclusive',@LockTimeout=0; "
            "SELECT @fault_lock,@@TRANCOUNT,XACT_STATE(),"
            "APPLOCK_MODE(N'public',N'dpone:composition-control:v1',N'Transaction'); "
            "CREATE TABLE #dpone_trust_fault (id int NOT NULL PRIMARY KEY); "
            "INSERT INTO #dpone_trust_fault VALUES (1); "
            "BEGIN TRY INSERT INTO #dpone_trust_fault VALUES (1); END TRY BEGIN CATCH "
            "SELECT ERROR_NUMBER(),@@TRANCOUNT,XACT_STATE(); "
        )
        suffix = (
            " IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION; END CATCH; "
            "IF (@fault_options & 16384) = 0 SET XACT_ABORT OFF; "
            "IF (@fault_options & 512) = 0 SET NOCOUNT OFF;"
        )
        # The SQL comes directly from the provider; parameters stay DBAPI-bound.
        self.cursor.execute(prefix + sql + suffix, *parameters)
        support.require_result_set(self.cursor)
        acquired = tuple(tuple(row) for row in self.cursor.fetchall())
        support.record_rows(
            self.record,
            "transaction_exclusive",
            acquired,
            ("lock_result", "transaction_count", "xact_state", "lock_mode"),
        )
        if len(acquired) != 1 or acquired[0][1:] != (1, 1, "Exclusive") or acquired[0][0] not in (0, 1):
            raise RuntimeError("fault_lock_not_acquired")
        support.require_result_set(self.cursor, advance=True)
        self.fault_rows = tuple(tuple(row) for row in self.cursor.fetchall())
        support.record_rows(
            self.record, "transaction_fault", self.fault_rows, ("sql_error", "transaction_count", "xact_state")
        )
        if self.fault_rows != ((2627, 1, -1),):
            raise RuntimeError("fault_not_doomed")
        support.require_result_set(self.cursor, advance=True)
        self._ready = True
        return self

    def fetchall(self):
        if not self._ready:
            raise RuntimeError("fault_cursor_read")
        self._ready = False
        rows = self.cursor.fetchall()
        self.precondition_rows = tuple(tuple(row) for row in rows)
        support.record_rows(self.record, "transaction_doomed", rows, ("transaction_count", "xact_state", "lock_mode"))
        while self.cursor.nextset():
            if self.cursor.description is not None:
                raise RuntimeError("fault_extra_result")
        self.complete = True
        return rows


def test_insufficient_transaction_lock_never_returns_trust(trust_case):
    case = trust_case
    case.append(1)
    refusals = 0
    with closing(case.database.connect()) as connection, closing(connection.cursor()) as cursor:
        ledger = CompositionMssqlLedger(cursor, case.schema)
        assert case.record_transaction("transaction_initial", connection) == ((0, 0, "NoLock"),)
        with pytest.raises(NonproductionAuthorityError) as refused:
            case.provider().read_revision_in(ledger)
        assert refused.value.reason == "trust_ledger_lock"
        refusals += 1
        for mode in ("Shared", "Exclusive"):
            try:
                if mode == "Exclusive":
                    observed = case.record_transaction("transaction_before_fault", connection)
                    assert observed == ((0, 0, "NoLock"),)
                    options = execute(connection, "SELECT @@OPTIONS;")
                    boundary = DoomedTransactionCursor(cursor, case.record)
                    doomed = CompositionMssqlLedger(boundary, case.schema)
                    with pytest.raises(NonproductionAuthorityError) as refused:
                        case.provider().read_revision_in(doomed)
                    assert refused.value.reason == "trust_ledger_lock"
                    assert boundary.complete and boundary.fault_rows == ((2627, 1, -1),)
                    assert boundary.precondition_rows == ((1, -1, "NoLock"),)
                    assert execute(connection, "SELECT @@OPTIONS;") == options
                    assert execute(connection, "SELECT OBJECT_ID(N'tempdb..#dpone_trust_fault');") == ((None,),)
                    observed = case.record_transaction("transaction_after_fault", connection)
                    assert observed == ((0, 0, "NoLock"),)
                else:
                    locked = support.acquire_shared_transaction(connection)
                    assert len(locked) == 1 and locked[0][0] >= 0
                    assert case.record_transaction("transaction_shared", connection) == ((1, 1, "Shared"),)
                    with pytest.raises(NonproductionAuthorityError) as refused:
                        case.provider().read_revision_in(ledger)
                    assert refused.value.reason == "trust_ledger_lock"
                refusals += 1
            finally:
                execute(connection, "IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;")
        case.record_transaction("transaction_final", connection)
        assert execute(connection, "SELECT @@TRANCOUNT,XACT_STATE();") == ((0, 0),)
    assert case.provider().read_revision().revision == 1
    case.record("transaction", {"rows": len(case.rows()), "denied": refusals})


def test_lost_read_ack_returns_no_trusted_revision(trust_case):
    case = trust_case
    case.append(1)
    before = case.rows()
    wrappers = []

    def factory():
        value = support.LostReadAcknowledgement(case.database.connect())
        wrappers.append(value)
        return value

    with pytest.raises(NonproductionAuthorityError, match="trust_read_unavailable"):
        case.provider(factory).read_revision()
    assert len(wrappers) == 1 and wrappers[0].committed is True
    assert case.rows() == before
    actual = case.provider().read_revision()
    assert actual.revision == 1 and actual.snapshot == case.snapshot
    case.record(
        "ack",
        {"rows": len(case.rows()), "commits": sum(value.committed for value in wrappers), "connections": len(wrappers)},
    )


def test_provisioner_can_append_without_schema_bypass(trust_case):
    case = trust_case
    denied = []
    with case.as_provisioner() as connection:
        privileges = execute(
            connection,
            "SELECT HAS_PERMS_BY_NAME(?,'OBJECT','SELECT'),"
            "HAS_PERMS_BY_NAME(?,'OBJECT','INSERT'),HAS_PERMS_BY_NAME(?,'OBJECT','ALTER'),"
            "HAS_PERMS_BY_NAME(?,'OBJECT','UPDATE'),HAS_PERMS_BY_NAME(?,'OBJECT','DELETE');",
            *(case.table(),) * 5,
        )
        assert privileges == ((1, 1, 0, 0, 0),)
        case.append(1, connection=connection)
        assert execute(connection, f"SELECT revision,policy_document FROM {case.table()};") == (
            (1, case.snapshot.policy_bytes),
        )
        for statement in (
            f"ALTER TABLE {case.table()} ADD forbidden int NULL;",
            f"TRUNCATE TABLE {case.table()};",
            f"DISABLE TRIGGER {case.trigger()} ON {case.table()};",
        ):
            denied.append(
                support.require_sql_rejection(
                    lambda statement=statement: execute(connection, statement), {229, 1088, 15151, 15247}
                )
            )
            case.require_schema()
            assert case.provider().read_revision().revision == 1
        support.require_sql_rejection(lambda: case.append(1, connection=connection), {51000})
    assert case.provider().read_revision().snapshot == case.snapshot
    case.record("permissions", {"rows": len(case.rows()), "allowed": 2, "denied": len(denied)})
