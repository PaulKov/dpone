"""Owned SQL trust storage fixtures; no signature, enrollment or worker authority.

Only the disposable Linux runner can enable these fixtures. Each case installs
the real renderers into a new schema in its newly owned database. Inert policy
and public-root bytes test storage identity, never cryptographic verification.
The runner removes the entire database/container; faults and impersonation are
restored here first, and cleanup failure remains a test failure.
"""

from __future__ import annotations

import json
import os
import platform
import re
from contextlib import closing, contextmanager
from pathlib import Path
from uuid import uuid4

import pytest
from tests.integration.composition.mssql_gate_live_provisioning import SqlFailure, execute
from tests.integration.composition.mssql_store_live_support import OwnedDatabase
from tests.nonproduction_signature_helpers import github_policy, trust

from dpone.adapters.composition_mssql_schema import COMPOSITION_MSSQL_LEDGER_LOCK, render_composition_mssql_schema
from dpone.adapters.nonproduction_mssql_schema import (
    render_nonproduction_mssql_schema,
    require_nonproduction_mssql_schema,
)
from dpone.adapters.nonproduction_mssql_trust import MssqlNonproductionTrustProvider
from dpone.contracts.nonproduction_scope import NonproductionAuthorityError

LIVE_MODULE = "tests/integration/composition/test_nonproduction_mssql_trust_live.py"
_OBSERVATIONS = {
    "readback",
    "append",
    "revision",
    "concurrency",
    "invalid",
    "drift",
    "transaction",
    "transaction_initial",
    "transaction_shared",
    "transaction_exclusive",
    "transaction_fault",
    "transaction_before_fault",
    "transaction_doomed",
    "transaction_after_fault",
    "transaction_final",
    "ack",
    "permissions",
}
_COUNTS = {
    "rows",
    "revision",
    "denied",
    "commits",
    "connections",
    "policy_bytes",
    "verifier_bytes",
    "trigger_bytes",
    "attempted",
    "winners",
    "restored",
    "allowed",
    "result_rows",
    "transaction_count",
}
_HASHES = {"policy_sha256", "verifier_policy_sha256", "trigger_sha256"}
_REASONS = {
    "trust_read_unavailable",
    "trust_revision_changed",
    "trust_ledger_lock",
    "trust_schema_trigger",
    "trust_schema_columns",
    "trust_control_authority",
    "trust_environment",
}
_FAILURE_DIAGNOSTICS = pytest.StashKey[list[tuple[str, str]]]()


def require_owned_database(env, *, system, machine):
    """Opt-out precedes parsing; an opted-in foreign endpoint is a hard failure."""
    if env.get("DPONE_RUN_COMPOSITION_MSSQL_TRUST_LIVE") != "1":
        pytest.skip("requires explicit disposable SQL trust runner profile")
    if system != "Linux" or machine not in {"x86_64", "AMD64"}:
        pytest.skip("requires disposable Linux x86_64 SQL trust runner")
    return OwnedDatabase.from_environment(env)


def observation_document(payload):
    """Closed flat counters/digests only; no arbitrary labels, SQL or exceptions."""
    if type(payload) is not dict or not payload or len(payload) > 16:
        raise ValueError("unsafe_trust_observation")
    for key, value in payload.items():
        if key in _COUNTS and type(value) is int and 0 <= value < 2**63:
            continue
        if key in _HASHES and type(value) is str and re.fullmatch(r"sha256:[0-9a-f]{64}", value):
            continue
        if key == "sql_error" and type(value) is int and 1 <= value <= 999999:
            continue
        if key == "xact_state" and type(value) is int and value in {-1, 0, 1}:
            continue
        if key == "lock_result" and type(value) is int and value in {-999, -3, -2, -1, 0, 1}:
            continue
        if key == "lock_mode" and type(value) is str and value in {"NoLock", "Shared", "Exclusive", "Update"}:
            continue
        raise ValueError("unsafe_trust_observation")
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    if len(raw) > 2048:
        raise ValueError("trust_observation_budget")
    return raw


def require_sql_rejection(operation, codes):
    """Count only the expected server refusal; syntax/connection errors fail."""
    with pytest.raises(SqlFailure) as caught:
        operation()
    assert caught.value.code in codes, "unexpected_sql_rejection"
    return caught.value.code


def acquire_shared_transaction(connection):
    """Keep BEGIN outside pyodbc's prepared parameterized lock RPC."""
    execute(connection, "BEGIN TRANSACTION;")
    return execute(
        connection,
        "DECLARE @r int; EXEC @r=sys.sp_getapplock @Resource=?,"
        "@LockOwner=N'Transaction',@LockMode=N'Shared',@LockTimeout=0; SELECT @r;",
        COMPOSITION_MSSQL_LEDGER_LOCK,
    )


def record_rows(record, name, rows, columns):
    """Flatten an actual bounded result shape through the closed sanitizer."""
    payload = {"result_rows": len(rows)}
    if len(rows) == 1 and len(rows[0]) == len(columns):
        payload.update(zip(columns, rows[0], strict=True))
    record(name, payload)


def require_result_set(cursor, *, advance=False):
    """Position on a real result, rejecting a missing expected batch result."""
    if advance and not cursor.nextset():
        raise RuntimeError("fault_missing_result")
    while cursor.description is None:
        if not cursor.nextset():
            raise RuntimeError("fault_missing_result")


class LostReadAcknowledgement:
    """Delegate real reads/commit, then discard only the successful client ACK."""

    def __init__(self, connection):
        self.connection = connection
        self.committed = False

    @property
    def autocommit(self):
        return self.connection.autocommit

    @autocommit.setter
    def autocommit(self, value):
        self.connection.autocommit = value

    def __getattr__(self, name):
        return getattr(self.connection, name)

    def commit(self):
        self.connection.commit()
        self.committed = True
        raise ConnectionError("synthetic_read_ack_lost")


class TrustCase:
    """One new control schema, independent administrator and storage-only pins."""

    def __init__(self, database, record_property):
        self.database, self.record_property = database, record_property
        self.schema = "trust_" + uuid4().hex
        self.service_id, self.environment_id = str(uuid4()), str(uuid4())
        self.snapshot = trust(github_policy(environment_id=self.environment_id))
        self.admin = None
        self.provisioner = None

    def table(self):
        return f"[{self.schema}].[composition_nonproduction_trust]"

    def trigger(self):
        return f"[{self.schema}].[composition_nonproduction_trust_append]"

    def install(self):
        """Explicit external provisioning, never runtime repair or enrollment."""
        self.admin = self.database.connect()
        self.sql(render_composition_mssql_schema(self.schema))
        self.sql(f"INSERT INTO [{self.schema}].[composition_authority] VALUES (1, 1, ?);", self.service_id)
        self.sql(render_nonproduction_mssql_schema(self.schema))
        self.require_schema()

    def sql(self, statement, *parameters):
        return execute(self.admin, statement, *parameters)

    def provider(self, factory=None, *, service_id=None, environment_id=None):
        return MssqlNonproductionTrustProvider(
            factory or self.database.connect,
            expected_service_id=service_id or self.service_id,
            expected_environment_id=environment_id or self.environment_id,
            control_schema=self.schema,
        )

    def append(self, revision, snapshot=None, *, connection=None):
        value = self.snapshot if snapshot is None else snapshot
        statement = (
            f"INSERT INTO {self.table()} (environment_id,revision,schema_version,policy_document,policy_sha256,"
            "verifier_policy_document,verifier_policy_sha256,current_revocation_epoch) VALUES (?, ?, 1, ?, ?, ?, ?, ?);"
        )
        parameters = (
            self.environment_id,
            revision,
            value.policy_bytes,
            value.policy_sha256,
            value.verifier_policy_bytes,
            value.verifier_policy_sha256,
            value.current_revocation_epoch,
        )
        if connection is not None:
            return execute(connection, statement, *parameters)
        with closing(self.database.connect()) as opened:
            return execute(opened, statement, *parameters)

    def rows(self):
        """Independent complete bounded history; exceeding the fixture bound fails."""
        rows = self.sql(
            f"SELECT TOP (17) revision,policy_document,policy_sha256,verifier_policy_document,"
            f"verifier_policy_sha256,current_revocation_epoch FROM {self.table()} ORDER BY revision;"
        )
        assert len(rows) <= 16, "fixture_history_budget"
        return rows

    def require_schema(self):
        assert self.admin is not None, "fixture_admin_required"
        with closing(self.admin.cursor()) as cursor:
            require_nonproduction_mssql_schema(cursor, self.schema)

    @contextmanager
    def fault(self, change, restore):
        """Always attempt exact owned fault restoration; never suppress failure."""
        try:
            change()
            yield
        finally:
            restore()
            self.require_schema()

    @contextmanager
    def as_provisioner(self):
        """Exercise a real restricted database principal, without login credentials.

        EXECUTE AS USER tests effective database permissions only; it does not
        claim authentication or create a contained/Windows writer fallback.
        """
        assert self.provisioner is None, "fixture_provisioner_reuse"
        name = "trust_provisioner_" + uuid4().hex
        self.provisioner = name
        self.sql(f"CREATE USER [{name}] WITHOUT LOGIN; GRANT SELECT, INSERT ON OBJECT::{self.table()} TO [{name}];")
        with closing(self.database.connect()) as connection:
            try:
                execute(connection, f"EXECUTE AS USER = N'{name}';")
                assert execute(connection, "SELECT USER_NAME(), IS_MEMBER('db_owner');") == ((name, 0),)
                yield connection
            finally:
                execute(connection, "REVERT;")
                assert execute(connection, "SELECT USER_NAME();") == (("dbo",),)

    def record(self, name, payload):
        if name not in _OBSERVATIONS:
            raise ValueError("unsafe_trust_observation_name")
        self.record_property("dpone.trust." + name, observation_document(payload))

    def record_transaction(self, name, connection):
        """Observe real state; NoLock is a sentinel when lock lookup is unsafe."""
        rows = execute(
            connection,
            "DECLARE @transaction_count int = @@TRANCOUNT, "
            "@transaction_state smallint = XACT_STATE(), @lock_mode nvarchar(32) = N'NoLock'; "
            "IF @transaction_count > 0 AND @transaction_state = 1 "
            "SET @lock_mode = APPLOCK_MODE(N'public', ?, N'Transaction'); "
            "SELECT @transaction_count, @transaction_state, @lock_mode;",
            COMPOSITION_MSSQL_LEDGER_LOCK,
        )
        record_rows(self.record, name, rows, ("transaction_count", "xact_state", "lock_mode"))
        return rows

    def cleanup(self):
        if self.admin is not None:
            try:
                if self.provisioner is not None:
                    self.sql(f"DROP USER [{self.provisioner}];")
                    assert self.sql("SELECT COUNT(*) FROM sys.database_principals WHERE name=?;", self.provisioner) == (
                        (0,),
                    )
            finally:
                self.admin.close()


@pytest.fixture
def trust_case(record_property):
    database = require_owned_database(os.environ, system=platform.system(), machine=platform.machine())
    if os.environ.get("PYTEST_XDIST_WORKER"):
        raise RuntimeError("serial_trust_profile_required")
    case = TrustCase(database, record_property)
    try:
        case.install()
        yield case
    finally:
        case.cleanup()


def sanitize_report(item, call, report):
    """Change only this module's diagnostics, preserving its actual result state."""
    if item.nodeid.split("::", 1)[0] != LIVE_MODULE:
        return
    stash = getattr(item, "stash", None)
    diagnostics = stash.get(_FAILURE_DIAGNOSTICS, []) if stash is not None else []
    safe = []
    seen = set()
    for name, value in report.user_properties:
        if type(name) is not str or name in seen or name not in {"dpone.trust." + suffix for suffix in _OBSERVATIONS}:
            continue
        try:
            if type(value) is str and len(value) <= 2048:
                safe.append((name, observation_document(json.loads(value))))
                seen.add(name)
        except (ValueError, TypeError, RecursionError):
            pass
    if report.failed:
        error = call.excinfo.value if call.excinfo else None
        reason = "assertion_or_fixture_failure"
        if isinstance(error, SqlFailure) and type(error.code) is int and 1 <= error.code <= 999999:
            reason = f"sql_error_{error.code}"
        elif isinstance(error, NonproductionAuthorityError) and type(error.reason) is str and error.reason in _REASONS:
            reason = error.reason
        report.longrepr = "SQL trust component failed: " + reason
        diagnostics = [("dpone.trust.failure", reason)]
        location = _failure_location(error)
        if location is not None:
            diagnostics.append(("dpone.trust.failure_location", location))
        if stash is not None:
            stash[_FAILURE_DIAGNOSTICS] = diagnostics
    elif report.skipped:
        report.longrepr = (LIVE_MODULE, 0, "requires explicit disposable Linux SQL trust runner")
    # JUnit reads properties from teardown, after the failure report was emitted.
    report.user_properties = safe + diagnostics
    report.sections = []


def _failure_location(error):
    """Expose only an owned basename and line, never source, locals or traceback."""
    sources = {Path(__file__).resolve(), Path(__file__).with_name(Path(LIVE_MODULE).name).resolve()}
    frame = error.__traceback__ if isinstance(error, BaseException) else None
    location = None
    for _ in range(64):
        if frame is None:
            break
        source = Path(frame.tb_frame.f_code.co_filename).resolve()
        if source in sources and 1 <= frame.tb_lineno <= 999999:
            location = f"{source.name}:{frame.tb_lineno}"
        frame = frame.tb_next
    return location


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    result = yield
    sanitize_report(item, call, result.get_result())
