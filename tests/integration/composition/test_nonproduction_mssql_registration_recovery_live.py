"""Nine real recovery/concurrency/permission cases; no worker or signing claim."""

import os
import secrets
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
from hashlib import sha256
from threading import Barrier
from uuid import uuid4

import pytest
from tests.integration.composition import nonproduction_mssql_registration_live_support as support
from tests.integration.composition.mssql_gate_live_provisioning import execute
from tests.integration.composition.nonproduction_mssql_trust_live_support import require_sql_rejection
from tests.integration.composition.test_nonproduction_mssql_registration_live import expanded
from tests.nonproduction_authority_helpers import limits, qualification

from dpone.adapters.nonproduction_mssql_registration_schema import nonproduction_registration_trigger_sql
from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.nonproduction_registration import original_sha256, workload_document
from dpone.contracts.nonproduction_scope import NonproductionAuthorityError

pytestmark = [pytest.mark.integration_live, pytest.mark.integration_mssql]
registration_case = support.registration_case


def require_restricted_identity(case, connection, login):
    """Record raw effective-role/permission results; NULL never means denial."""
    rows = execute(
        connection,
        "SELECT SUSER_SNAME(),USER_NAME(),IS_SRVROLEMEMBER('sysadmin'),IS_MEMBER('db_owner'),"
        "HAS_PERMS_BY_NAME(NULL,NULL,'CONTROL SERVER'),HAS_PERMS_BY_NAME(NULL,NULL,'ADMINISTER BULK OPERATIONS');",
    )
    payload = {"result_rows": len(rows)}
    if len(rows) == 1 and len(rows[0]) == 6:
        payload.update(login_matches=rows[0][0] == login, user_matches=rows[0][1] == login)
        payload.update(zip(("sysadmin", "db_owner", "control_server", "bulk_operations"), rows[0][2:], strict=True))
    case.record("permissions", payload)
    assert rows == ((login, login, 0, 0, 0, 0),), "restricted_principal_identity"


class BoundaryConnection:
    """Faults delegate real SQL; completed commits and partial rows are observed."""

    def __init__(self, case, mode):
        assert mode in {"before_commit", "after_commit", "first_member", "read"}
        self.raw, self.case, self.mode = case.connect(), case, mode
        self.commits, self.commit_calls, self.partial = 0, 0, ()

    @property
    def autocommit(self):
        return self.raw.autocommit

    @autocommit.setter
    def autocommit(self, value):
        self.raw.autocommit = value

    def __getattr__(self, name):
        return getattr(self.raw, name)

    def cursor(self):
        return BoundaryCursor(self.raw.cursor(), self)

    def commit(self):
        self.commit_calls += 1
        if self.mode == "before_commit":
            raise ConnectionError("injected_before_commit")
        self.raw.commit()
        self.commits += 1
        if self.mode == "after_commit":
            raise ConnectionError("injected_commit_ack_lost")


class BoundaryCursor:
    def __init__(self, raw, connection):
        self.raw, self.connection = raw, connection

    def __getattr__(self, name):
        return getattr(self.raw, name)

    def execute(self, sql, *parameters):
        result = self.raw.execute(sql, *parameters)
        boundary = self.connection
        if boundary.mode == "first_member" and sql.startswith("INSERT INTO " + boundary.case.table("memberships")):
            while self.raw.nextset():
                pass
            boundary.partial = execute(
                boundary.raw,
                f"SELECT (SELECT COUNT(*) FROM {boundary.case.table('grants')}), "
                f"(SELECT COUNT(*) FROM {boundary.case.table('memberships')});",
            )
            raise ConnectionError("injected_after_real_membership")
        if boundary.mode == "read" and sql.startswith("SELECT TOP (2)"):
            raise ConnectionError("injected_read_ack_lost")
        return self if result is self.raw else result


def race(case, grants):
    barrier = Barrier(2, timeout=15)

    def contender(grant):
        reason = None
        barrier.wait()
        try:
            with case.transaction() as ledger:
                method = (
                    case.store().register_execution_in
                    if grant.phase == "execution"
                    else case.store().consume_qualification_in
                )
                try:
                    method(ledger, **support.inputs(grant, case.policy), expected_revision=case.expected)
                except NonproductionAuthorityError as error:
                    reason = error.reason
                    raise
            return True, grant
        except CompositionAdmissionError as error:
            assert (reason or error.reason) in {
                "ledger_lock",
                "qualification_consumed",
                "registration_subject_reuse",
                "membership_budget",
            }
            return False, grant

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(contender, grant) for grant in grants]
        return tuple(future.result(timeout=30) for future in futures)


def test_concurrent_qualification_has_one_durable_consumption(registration_case):
    case = registration_case
    first = qualification(case.policy)
    second = qualification(case.policy, grant_id=str(uuid4()), qualification_run_id=str(uuid4()))
    for pair in ((first, first), (second, replace(second, grant_id=str(uuid4())))):
        results = race(case, pair)
        assert sum(won for won, _ in results) == 1
        assert case.read(next(grant for won, grant in results if won)) is not None
    assert (len(case.snapshot()[0]), len(case.snapshot()[1])) == (2, 0)
    case.record("race", {"attempted": 4, "winners": 2, "rows": 2, "members": 0})


def test_concurrent_execution_preserves_membership_ceiling(registration_case):
    case = registration_case
    first = case.grant()
    results = race(case, (first, first))
    acknowledged = sum(won for won, _ in results)
    assert acknowledged >= 1 and case.read(first) is not None
    assert (len(case.snapshot()[0]), len(case.snapshot()[1])) == (1, 3)
    case.append_policy(replace(case.policy, limits=limits(max_workloads=4)))
    left, right = expanded(case.grant(), 4), expanded(case.grant(), 4)
    right = replace(
        right,
        workloads=tuple(
            replace(row, workload_id="b_extra_other") if row.workload_id == "b_extra_00" else row
            for row in right.workloads
        ),
    )
    results = race(case, (left, right))
    assert sum(won for won, _ in results) == 1
    assert case.read(next(grant for won, grant in results if won)) is not None
    assert (len(case.snapshot()[0]), len(case.snapshot()[1])) == (2, 4)
    case.record("race", {"attempted": 4, "rows": 2, "members": 4, "commits": acknowledged + 1})


def test_partial_write_failure_rolls_back_complete_registration(registration_case):
    case = registration_case
    grant = case.grant()
    boundary = BoundaryConnection(case, "first_member")
    with pytest.raises(CompositionAdmissionError, match="control_operation_unknown"):
        case.register(grant, factory=lambda: boundary)
    assert boundary.partial == ((1, 1),) and boundary.commit_calls == 0
    assert case.read(grant) is None and case.snapshot() == ((), ())
    case.record("fault", {"rows": 0, "members": 0, "commits": 0, "connections": 1})


def test_lost_commit_ack_reconciles_exact_durable_originals(registration_case):
    case = registration_case
    grant = case.grant()
    boundary = BoundaryConnection(case, "after_commit")
    with pytest.raises(CompositionAdmissionError, match="control_operation_unknown"):
        case.register(grant, factory=lambda: boundary)
    assert boundary.commit_calls == boundary.commits == 1
    actual = case.read(grant)
    assert actual is not None and actual.originals.grant_bytes == grant.to_bytes()
    values = support.inputs(grant, case.policy)
    assert (
        actual.originals.bundle_bytes,
        actual.originals.signature_subject_bytes,
        actual.originals.request_bytes,
    ) == (values["signature_bundle"], values["signature_subject_bytes"], values["request_bytes"])
    assert (len(case.snapshot()[0]), len(case.snapshot()[1])) == (1, 3)
    case.record("fault", {"commits": 1, "connections": 1, "rows": 1, "members": 3})


def test_failed_commit_and_unavailable_readback_return_no_ack(registration_case):
    case = registration_case
    grant = case.grant()
    before = BoundaryConnection(case, "before_commit")
    with pytest.raises(CompositionAdmissionError, match="control_operation_unknown"):
        case.register(grant, factory=lambda: before)
    assert before.commit_calls == 1 and before.commits == 0 and case.read(grant) is None
    committed = BoundaryConnection(case, "after_commit")
    with pytest.raises(CompositionAdmissionError, match="control_operation_unknown"):
        case.register(grant, factory=lambda: committed)
    unavailable = BoundaryConnection(case, "read")
    with pytest.raises(CompositionAdmissionError, match="control_operation_unknown"):
        case.read(grant, factory=lambda: unavailable)
    assert committed.commits == 1 and unavailable.commit_calls == 0
    assert case.read(grant).originals.grant_bytes == grant.to_bytes() and len(case.snapshot()[1]) == 3
    case.record("fault", {"commits": 1, "connections": 3, "rows": 1, "members": 3})


def test_membership_corruption_and_overflow_fail_closed(registration_case):
    case = registration_case
    grant = expanded(case.grant(), 64)
    case.register(grant)
    qualified = qualification(case.policy)
    case.register(qualified)
    member = case.snapshot()[1][0]
    insert = f"INSERT INTO {case.table('memberships')} VALUES (?, ?, 'execution', ?, ?, ?);"
    values = (case.trust.environment_id, grant.scope.campaign_id, *member)
    delete = f"DELETE FROM {case.table('memberships')} WHERE workload_sha256=?;"
    update = f"UPDATE {case.table('memberships')} SET first_grant_sha256=? WHERE workload_sha256=?;"
    extra = workload_document("z_unapproved_member")
    extra_values = (
        case.trust.environment_id,
        grant.scope.campaign_id,
        original_sha256(extra),
        extra,
        grant.consumption_subject_sha256,
    )
    faults = (
        (lambda: case.rewrite("memberships", delete, member[0]), lambda: case.sql(insert, *values)),
        (
            lambda: case.rewrite("memberships", update, qualified.consumption_subject_sha256, member[0]),
            lambda: case.rewrite("memberships", update, member[2], member[0]),
        ),
        (lambda: case.sql(insert, *extra_values), lambda: case.rewrite("memberships", delete, extra_values[2])),
    )
    for (change, restore), reason in zip(
        faults, ("registration_membership", "registration_membership", "registration_membership_overflow"), strict=True
    ):
        with case.fault(change, restore):
            case.reject_read(grant, reason)
        assert case.read(grant) is not None
    assert len(case.snapshot()[1]) == 64
    case.record("state", {"rows": 2, "members": 64, "denied": 3, "restored": 3})


def test_installed_catalog_drift_blocks_registration(registration_case):
    case = registration_case
    table = case.table("grants")

    def index(predicate):
        case.sql(
            f"DROP INDEX [uq_np_qualification_run] ON {table}; CREATE UNIQUE INDEX [uq_np_qualification_run] "
            f"ON {table}(phase_subject_id) WHERE {predicate};"
        )

    def trigger(suffix):
        case.sql(f"DROP TRIGGER {case.trigger('grants')};")
        case.sql(nonproduction_registration_trigger_sql(case.schema, "grants") + suffix)

    faults = (
        (
            lambda: index("([phase]='execution')"),
            lambda: index("([phase]='qualification')"),
            "registration_schema_keys",
        ),
        (lambda: trigger("\n-- fixture drift"), lambda: trigger(""), "registration_schema_trigger"),
        (
            lambda: case.sql(f"ALTER TABLE {table} ADD fixture_drift int NULL;"),
            lambda: case.sql(f"ALTER TABLE {table} DROP COLUMN fixture_drift;"),
            "registration_schema_columns",
        ),
    )
    for change, restore, reason in faults:
        with case.fault(change, restore):
            case.reject(case.grant(), reason)
        assert case.snapshot() == ((), ())
    case.record("catalog", {"rows": 0, "denied": 3, "restored": 3})


def test_restricted_login_cannot_bypass_registration_storage(registration_case):
    case = registration_case
    path = support.require_bulk_fixture(os.environ)
    probe, login = f"[{case.schema}].[registration_bulk_probe]", "registration_" + uuid4().hex
    case.sql(f"CREATE TABLE {probe} (value int NOT NULL);")
    bulk = f"BULK INSERT {probe} FROM '{path}' WITH (ROWTERMINATOR='0x0a');"
    created = False
    try:
        assert case.sql(
            f"SELECT DATALENGTH(BulkColumn),HASHBYTES('SHA2_256',BulkColumn) "
            f"FROM OPENROWSET(BULK '{path}',SINGLE_BLOB) AS fixture;"
        ) == ((2, sha256(b"1\n").digest()),)
        case.sql(bulk)
        assert case.sql(f"SELECT value FROM {probe};") == ((1,),)
        case.sql(f"DELETE FROM {probe};")
        case.sql(f"CREATE LOGIN [{login}] WITH PASSWORD='Dp1!{secrets.token_hex(24)}', CHECK_POLICY=ON;")
        created = True
        case.sql(
            f"CREATE USER [{login}] FOR LOGIN [{login}]; GRANT VIEW DEFINITION TO [{login}]; "
            f"GRANT SELECT ON SCHEMA::[{case.schema}] TO [{login}]; "
            + " ".join(
                f"GRANT INSERT ON OBJECT::{table} TO [{login}];"
                for table in (probe, case.table("grants"), case.table("memberships"))
            )
        )
        with closing(case.connect()) as connection:
            try:
                execute(connection, f"EXECUTE AS LOGIN=N'{login}';")
                require_restricted_identity(case, connection, login)
                tokens = execute(
                    connection, "SELECT COUNT(*) FROM sys.login_token; SELECT COUNT(*) FROM sys.user_token;"
                )
                assert len(tokens) == 2 and all(row[0] >= 1 for row in tokens)
                assert execute(
                    connection,
                    "SELECT HAS_PERMS_BY_NAME(?,'OBJECT','INSERT'),HAS_PERMS_BY_NAME(?,'OBJECT','ALTER');",
                    probe,
                    probe,
                ) == ((1, 0),)
                denied = [require_sql_rejection(lambda: execute(connection, bulk), {229, 4834})]
                for statement in (
                    f"UPDATE {case.table('grants')} SET schema_version=2;",
                    f"DELETE FROM {case.table('memberships')};",
                    f"ALTER TABLE {case.table('grants')} ADD forbidden int;",
                    f"TRUNCATE TABLE {case.table('memberships')};",
                    f"DISABLE TRIGGER {case.trigger('grants')} ON {case.table('grants')};",
                    "EXECUTE AS LOGIN=N'sa';",
                ):
                    denied.append(
                        require_sql_rejection(
                            lambda statement=statement: execute(connection, statement), {229, 1088, 15151, 15247}
                        )
                    )
                execute(connection, f"INSERT INTO {probe} VALUES (2);")
            finally:
                execute(connection, "REVERT;")
                assert execute(connection, "SELECT SUSER_SNAME();") == (("sa",),)
        assert case.sql(f"SELECT value FROM {probe};") == ((2,),)
        case.require_schema()
        assert case.snapshot() == ((), ())
        grant = case.grant()
        case.register(grant, factory=lambda: case.impersonate(login))
        assert case.read(grant) is not None and len(case.snapshot()[1]) == 3
        case.record(
            "state",
            {"denied": len(denied), "allowed": 3, "token_rows": sum(row[0] for row in tokens), "sql_error": denied[0]},
        )
    finally:
        if created:
            case.sql(f"IF USER_ID(N'{login}') IS NOT NULL DROP USER [{login}]; DROP LOGIN [{login}];")
            assert case.sql("SELECT COUNT(*) FROM sys.server_principals WHERE name=?;", login) == ((0,),)
        case.sql(f"DROP TABLE {probe};")


def test_session_options_reject_new_writes_without_mutation(registration_case):
    case = registration_case
    historical = case.grant()
    original = case.register(historical)
    before = case.snapshot()
    for option in ("SET ARITHABORT OFF;", "SET NUMERIC_ROUNDABORT ON;"):
        observed = None
        with pytest.raises(CompositionAdmissionError, match="control_operation_unknown"):
            with case.transaction() as ledger:
                ledger.cursor.execute(option)
                ledger.cursor.execute("SELECT @@OPTIONS;")
                invalid = ledger.cursor.fetchone()[0]
                assert invalid & 4472 != 4472 or invalid & 8192
                assert (
                    case.store().read_in(ledger, historical.consumption_subject_sha256, expected_revision=case.expected)
                    == original
                )
                try:
                    case.store().register_execution_in(
                        ledger, **support.inputs(case.grant(), case.policy), expected_revision=case.expected
                    )
                except NonproductionAuthorityError as error:
                    ledger.cursor.execute("SELECT @@OPTIONS;")
                    observed = (error.reason, ledger.cursor.fetchone()[0])
                    raise
        assert observed == ("registration_session", invalid)
        assert case.snapshot() == before
    assert case.read(case.grant()) is None
    fresh = case.grant()
    case.register(fresh)
    assert case.read(fresh) is not None
    case.record("state", {"rows": 2, "members": 3, "denied": 2})
