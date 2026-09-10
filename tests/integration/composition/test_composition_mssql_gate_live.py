"""Live gate provisioning/authority matrix on one newly disposable SQL instance.

Every test uses actual SQL. Synthetic metadata is not native-v2, route, Airflow,
dbt or ClickHouse execution evidence. Permission denials require recognized SQL
errors and independent absence-of-mutation checks, not arbitrary driver failures.
"""

from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
from threading import Event

import pytest
from tests.integration.composition.mssql_gate_live_provisioning import SqlFailure, execute
from tests.integration.composition.mssql_gate_live_support import commit_factory, require_denied

from dpone.adapters.composition_mssql_attempts import composition_control_transaction
from dpone.adapters.composition_mssql_gate_schema import (
    GATE_READER,
    GATE_TRIGGER,
    login_trigger_sql,
    module_sha256,
    monotonic_trigger_sql,
)
from dpone.adapters.composition_mssql_issuance import require_gate_policy
from dpone.contracts.composition_control import CompositionAdmissionError

pytestmark = [pytest.mark.integration_live, pytest.mark.integration_mssql]
pytest_plugins = ["tests.integration.composition.mssql_gate_live_support"]


def test_installed_gate_policy_and_reader_permissions(gate_case):
    case, env = gate_case, gate_case.environment
    with composition_control_transaction(env.connect, env.schema, env.service_id) as ledger:
        require_gate_policy(ledger, env.database.database)
    rows = case.sql(
        "SELECT t.is_disabled,HASHBYTES('SHA2_256',CONVERT(varbinary(max),m.definition)) FROM sys.server_triggers t JOIN sys.server_sql_modules m ON m.object_id=t.object_id WHERE t.name=?;",
        GATE_TRIGGER,
    )
    assert rows == ((False, module_sha256(login_trigger_sql(env.database.database, env.schema))),)
    assert case.sql(
        "SELECT t.is_disabled,HASHBYTES('SHA2_256',CONVERT(varbinary(max),m.definition)) FROM sys.triggers t JOIN sys.sql_modules m ON m.object_id=t.object_id WHERE t.object_id=OBJECT_ID(?);",
        f"[{env.schema}].[composition_login_gate_monotonic]",
    ) == ((False, module_sha256(monotonic_trigger_sql(env.schema))),)
    assert case.sql("SELECT type,is_disabled FROM sys.server_principals WHERE name=?;", GATE_READER) == (("S", True),)
    case.attempts.admit_once(case.attempt)
    try:
        env.recover(f"REVOKE SELECT ON OBJECT::{case.table('login_gates')} FROM [{GATE_READER}];")
        with pytest.raises(CompositionAdmissionError, match="login_gate_reader_visibility"):
            case.gate.issue_once(case.attempt)
        case.no_issuance()
    finally:
        env.recover(f"GRANT SELECT ON OBJECT::{case.table('login_gates')} TO [{GATE_READER}];")
    case.record(
        "policy", {"logon_sha256": rows[0][1].hex(), "reader_disabled": True, "missing_reader_select_rejected": True}
    )


def test_issued_principal_has_only_managed_writer_scope(gate_case):
    case = gate_case
    credentials = case.issue()
    worker = case.worker()
    assert execute(worker, "SELECT ORIGINAL_LOGIN(),SUSER_SID(),IS_SRVROLEMEMBER('sysadmin');") == (
        (credentials.login_name, credentials.login_sid, 0),
    )
    execute(
        worker,
        "CREATE TABLE [managed].[scratch] (id int NOT NULL); INSERT INTO [managed].[scratch] VALUES(1); UPDATE [managed].[scratch] SET id=2;",
    )
    assert execute(worker, "SELECT id FROM [managed].[scratch];") == ((2,),)
    execute(worker, "DELETE FROM [managed].[scratch]; DROP TABLE [managed].[scratch];")
    denied = []
    for sql in (
        "CREATE TABLE [unmanaged].[forbidden] (id int);",
        f"UPDATE [{case.environment.database.database}].{case.table('login_gates')} SET gate_state='CLOSING';",
        f"ALTER SERVER ROLE [sysadmin] ADD MEMBER [{credentials.login_name}];",
        "EXECUTE AS LOGIN='sa';",
    ):
        denied.append(require_denied(lambda sql=sql: execute(worker, sql)))
    assert case.target_sql("SELECT COUNT(*) FROM sys.tables WHERE name IN ('scratch','forbidden');") == ((0,),)
    assert case.gate_row()[2] == "READY"
    assert case.sql(
        "SELECT COUNT(*) FROM sys.server_role_members r JOIN sys.server_principals p ON r.member_principal_id=p.principal_id WHERE p.sid=?;",
        credentials.login_sid,
    ) == ((0,),)
    case.record("scope", {"issued_sid": credentials.login_sid.hex(), "permission_error_codes": denied})
    case.close_and_prove()


def test_gate_transitions_are_monotonic_and_reconnect_is_denied(gate_case):
    case = gate_case
    case.issue()
    worker = case.worker()
    assert execute(worker, "SELECT 1;") == ((1,),)
    assert case.gate_row()[2] == "READY"
    closed = case.gate.close(case.attempt)
    assert case.gate_row()[2:] == ("CLOSED", closed.evidence_sha256)
    with pytest.raises(SqlFailure) as caught:
        case.sql(
            f"UPDATE {case.table('login_gates')} SET gate_state='READY' WHERE attempt_sha256=?;",
            case.attempt.attempt_sha256,
        )
    assert caught.value.code == 51000
    assert case.gate_row()[2] == "CLOSED"
    require_denied(lambda: case.worker())
    with pytest.raises(CompositionAdmissionError, match="login_issuance_replay"):
        case.gate.issue_once(case.attempt)
    assert case.gate.close(case.attempt) == closed
    case.close_and_prove()


def test_stale_and_replayed_attempts_never_issue_credentials(gate_case):
    case = gate_case
    stale = replace(case.attempt, guard_epochs=((case.attempt.guard_epochs[0][0], 2),))
    with pytest.raises(CompositionAdmissionError, match="attempt_guard_epochs"):
        case.attempts.admit_once(stale)
    case.no_issuance()
    case.attempts.admit_once(case.attempt)
    with pytest.raises(CompositionAdmissionError, match="attempt_replay"):
        case.attempts.admit_once(case.attempt)
    case.store.begin_retirement(case.request)
    with pytest.raises(CompositionAdmissionError, match="occurrence_state"):
        case.gate.issue_once(case.attempt)
    case.no_issuance()
    assert case.sql(
        f"SELECT COUNT(*) FROM {case.table('attempts')} WHERE activation_id=?;", case.request.activation_id
    ) == ((1,),)


def test_recreated_target_database_blocks_issuance(gate_case):
    case = gate_case
    case.attempts.admit_once(case.attempt)
    case.environment.recover(f"DROP DATABASE [{case.target}]; CREATE DATABASE [{case.target}];")
    current = case.sql(
        "SELECT LOWER(CONVERT(char(36),database_guid)) FROM sys.database_recovery_status WHERE database_id=DB_ID(?);",
        case.target,
    )
    assert current != ((case.pins[1],),)
    with pytest.raises(CompositionAdmissionError, match="login_database_enrollment"):
        case.gate.issue_once(case.attempt)
    case.no_issuance()


def test_owner_and_role_permission_drift_block_issuance(gate_case):
    case, env = gate_case, gate_case.environment
    case.attempts.admit_once(case.attempt)
    owner = env.auxiliary_owner()
    try:
        env.recover(f"ALTER AUTHORIZATION ON DATABASE::[{case.target}] TO [{owner}];")
        with pytest.raises(CompositionAdmissionError, match="login_database_enrollment"):
            case.gate.issue_once(case.attempt)
        case.no_issuance()
    finally:
        env.recover(f"ALTER AUTHORIZATION ON DATABASE::[{case.target}] TO [{env.controller_name}];")
    try:
        case.target_sql(f"GRANT CONTROL TO [{env.writer_role}];")
        with pytest.raises(CompositionAdmissionError, match="login_role_permissions"):
            case.gate.issue_once(case.attempt)
        case.no_issuance()
    finally:
        case.target_sql(f"REVOKE CONTROL FROM [{env.writer_role}];")


def test_concurrent_issuance_returns_credentials_once(gate_case):
    case = gate_case
    case.attempts.admit_once(case.attempt)
    journaled, release = Event(), Event()

    def after_commit():
        journaled.set()
        assert release.wait(15), "journal_interlock_timeout"

    factory, _ = commit_factory(case.environment, 1, after_commit)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(case.login_gate(factory).issue_once, case.attempt)
        try:
            assert journaled.wait(10), "journal_not_observed"
            assert case.gate_row()[2] == "JOURNALED"
            with pytest.raises(CompositionAdmissionError, match="login_issuance_replay"):
                case.gate.issue_once(case.attempt)
        finally:
            release.set()
        case.credentials = future.result(timeout=20)
    assert case.gate_row()[0] == case.credentials.login_sid
    assert case.sql(
        f"SELECT COUNT(*) FROM {case.table('issued_authorities')} WHERE attempt_sha256=?;", case.attempt.attempt_sha256
    ) == ((1,),)
    with closing(case.worker()) as worker:
        assert execute(worker, "SELECT SUSER_SID();") == ((case.credentials.login_sid,),)
    case.connections.clear()
    case.close_and_prove()


def test_missing_or_recreated_sid_cannot_prove_closed(gate_case):
    case = gate_case
    credentials = case.issue()
    case.environment.recover(f"DROP LOGIN [{credentials.login_name}];")
    try:
        with pytest.raises(CompositionAdmissionError):
            case.gate.close(case.attempt)
        assert case.gate_row()[2] == "CLOSING"
        case.environment.recover(
            "DECLARE @name sysname=?, @password nvarchar(128)=?; DECLARE @sql nvarchar(max)=N'CREATE LOGIN '+QUOTENAME(@name)+N' WITH PASSWORD='+QUOTENAME(@password,'''')+N', CHECK_POLICY=ON'; EXEC sys.sp_executesql @sql;",
            credentials.login_name,
            credentials.password,
        )
        replacement = case.sql("SELECT sid FROM sys.server_principals WHERE name=?;", credentials.login_name)[0][0]
        assert replacement != credentials.login_sid
        with pytest.raises(CompositionAdmissionError):
            case.gate.close(case.attempt)
        assert case.sql(
            f"SELECT COUNT(*) FROM {case.table('proofs')} WHERE attempt_sha256=?;", case.attempt.attempt_sha256
        ) == ((0,),)
    finally:
        case.environment.recover(
            "DECLARE @name sysname=?; IF EXISTS(SELECT 1 FROM sys.server_principals WHERE name=@name) BEGIN DECLARE @sql nvarchar(max)=N'ALTER LOGIN '+QUOTENAME(@name)+N' DISABLE'; EXEC sys.sp_executesql @sql; END;",
            credentials.login_name,
        )
