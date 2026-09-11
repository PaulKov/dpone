"""Owned SQL registration fixtures and closed evidence; never executor authority.

The Linux runner owns the database/container. TrustCase supplies independent
external pins and installation. All fault edits target this case's new schema;
restoration failures remain failures. Documents and signature bundles are inert
storage inputs, not authenticated grants or physical-enrollment observations.
"""

from __future__ import annotations

import json
import os
import platform
import re
from contextlib import closing, contextmanager
from dataclasses import replace
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

import pytest
from tests.integration.composition.mssql_gate_live_provisioning import SqlFailure, execute
from tests.integration.composition.mssql_store_live_support import OwnedDatabase
from tests.integration.composition.nonproduction_mssql_trust_live_support import TrustCase
from tests.nonproduction_authority_helpers import NOW, digest, execution
from tests.nonproduction_signature_helpers import BUNDLE, github_policy, trust
from tests.test_nonproduction_activation import CELLS, request, resources

from dpone.adapters.composition_mssql_attempts import composition_control_transaction
from dpone.adapters.nonproduction_mssql_registration import MssqlNonproductionRegistrationStore
from dpone.adapters.nonproduction_mssql_registration_schema import (
    REGISTRATION_COLUMNS,
    render_nonproduction_mssql_registration_schema,
    require_nonproduction_registration_schema,
)
from dpone.contracts.composition_activation import CompositionAdmissionError, CompositionWorkloadAdmission
from dpone.contracts.nonproduction_registration import signature_subject_bytes
from dpone.contracts.nonproduction_scope import NonproductionAuthorityError

LIVE_MODULES = tuple(
    "tests/integration/composition/" + name + ".py"
    for name in ("test_nonproduction_mssql_registration_live", "test_nonproduction_mssql_registration_recovery_live")
)
OPTIONS = (
    "SET ANSI_NULLS ON; SET ANSI_PADDING ON; SET ANSI_WARNINGS ON; SET ARITHABORT ON; "
    "SET CONCAT_NULL_YIELDS_NULL ON; SET QUOTED_IDENTIFIER ON; SET NUMERIC_ROUNDABORT OFF;"
)
BULK_PATH = "/tmp/dpone-composition-registration-bulk.txt"
DENIAL_OPERATIONS = ("update", "delete", "alter", "truncate", "disable_trigger", "impersonate")
_OBSERVATIONS = {"state", "bytes", "catalog", "transaction", "race", "fault", "permissions", "history", "cleanup"}
_OBSERVATIONS.update("denial_" + str(index) for index in range(1, 7))
_OBSERVATIONS.add("impersonation_target")
_COUNTS = {
    "rows",
    "members",
    "revision",
    "denied",
    "commits",
    "connections",
    "bytes",
    "attempted",
    "winners",
    "restored",
    "allowed",
    "result_rows",
    "transaction_count",
    "options",
    "token_rows",
    "sql_error",
}
_REASONS = {
    "registration_session",
    "registration_rebind",
    "registration_membership",
    "registration_membership_overflow",
    "registration_schema_keys",
    "registration_schema_trigger",
    "registration_unavailable",
    "membership_budget",
    "qualification_consumed",
    "registration_subject_reuse",
    "trust_revision_changed",
    "trust_ledger_lock",
    "control_operation_unknown",
    "ledger_lock",
    "control_authority",
    "registration_clock",
}
_FAILURES = pytest.StashKey[list[tuple[str, str]]]()


def require_owned_database(env, *, system, machine):
    if env.get("DPONE_RUN_COMPOSITION_MSSQL_REGISTRATION_LIVE") != "1":
        pytest.skip("requires explicit disposable SQL registration runner")
    if system != "Linux" or machine not in {"x86_64", "AMD64"}:
        pytest.skip("requires disposable Linux x86_64 SQL registration runner")
    return OwnedDatabase.from_environment(env)


def require_bulk_fixture(env):
    """Only the runner's exact two public bytes and fixed owned-container path."""
    if (
        env.get("DPONE_COMPOSITION_BULK_FIXTURE_PATH"),
        env.get("DPONE_COMPOSITION_BULK_FIXTURE_BYTES"),
        env.get("DPONE_COMPOSITION_BULK_FIXTURE_SHA256"),
    ) != (BULK_PATH, "2", "sha256:" + sha256(b"1\n").hexdigest()):
        raise RuntimeError("owned_bulk_fixture_required")
    return BULK_PATH


def inputs(grant, policy):
    values = dict(
        grant_bytes=grant.to_bytes(),
        signature_bundle=BUNDLE,
        signature_subject_bytes=signature_subject_bytes(grant.signature_subject(policy)),
    )
    if grant.phase == "execution":
        # Reuse canonical request/resource producers; expand only the fixture's
        # complete workload list, without claiming actual physical enrollment.
        base = request(replace(grant, workloads=(grant.workloads[0], grant.workloads[1], grant.workloads[-1])))
        workloads = tuple(
            CompositionWorkloadAdmission(
                row.workload_id,
                row.constituent_id,
                row.pack_sha256,
                CELLS[2] if row.constituent_id == "standalone" else CELLS[0 if index == 0 else 1],
                (digest(row.workload_id + " write"),),
            )
            for index, row in enumerate(grant.workloads)
        )
        values["request_bytes"] = replace(
            base, workloads=workloads, resources=resources(workloads), execution_grant_sha256=grant.grant_sha256
        ).to_bytes()
    return values


class RegistrationCase:
    """One owned schema, real transactions, independent reconciliation."""

    def __init__(self, database, record_property):
        self.database, self.record_property = database, record_property
        self.trust = TrustCase(database, record_property)
        self.schema, self.service_id = self.trust.schema, self.trust.service_id
        self.policy = github_policy(environment_id=self.trust.environment_id)
        self.clock = lambda: NOW

    def table(self, kind):
        if kind not in {"grants", "memberships"}:
            raise ValueError("fixture_table")
        return f"[{self.schema}].[composition_nonproduction_{kind}]"

    def trigger(self, kind):
        self.table(kind)
        return f"[{self.schema}].[composition_nonproduction_{kind}_append]"

    def connect(self):
        connection = self.database.connect()
        try:
            execute(connection, OPTIONS)
            return connection
        except BaseException:
            connection.close()
            raise

    def sql(self, statement, *parameters):
        with closing(self.connect()) as connection:
            return execute(connection, statement, *parameters)

    def install(self):
        self.trust.install()
        self.trust.sql(render_nonproduction_mssql_registration_schema(self.schema))
        self.trust.append(1)
        self.expected = self.trust.provider().read_revision()
        self.require_schema()

    def require_schema(self):
        self.trust.require_schema()
        with closing(self.connect()) as connection, closing(connection.cursor()) as cursor:
            require_nonproduction_registration_schema(cursor, self.schema)

    def store(self, *, provider=None):
        return MssqlNonproductionRegistrationStore(provider or self.trust.provider(), clock=self.clock)

    def registration_method(self, grant):
        """Use the same phase selection for successful and refused fixture calls."""
        store = self.store()
        return store.register_execution_in if grant.phase == "execution" else store.consume_qualification_in

    def grant(self, **changes):
        return execution(self.policy, grant_id=str(uuid4()), **changes)

    def transaction(self, factory=None):
        return composition_control_transaction(factory or self.connect, self.schema, self.service_id)

    def impersonate(self, login):
        """Fixture-created server principal only; closing the connection restores context."""
        if re.fullmatch(r"registration_[0-9a-f]{32}", login) is None:
            raise ValueError("fixture_login")
        connection = self.connect()
        try:
            execute(connection, f"EXECUTE AS LOGIN=N'{login}';")
            return connection
        except BaseException:
            connection.close()
            raise

    def register(self, grant, *, factory=None, expected=None, **changes):
        """Commit once via existing owner; caller separately reconciles success."""
        values = inputs(grant, self.policy) | changes
        with self.transaction(factory) as ledger:
            method = self.registration_method(grant)
            result = method(ledger, **values, expected_revision=expected or self.expected)
        return result

    def read(self, grant, *, factory=None, expected=None):
        with self.transaction(factory) as ledger:
            result = self.store().read_in(
                ledger, grant.consumption_subject_sha256, expected_revision=expected or self.expected
            )
        return result

    def reject(self, grant, reason, **changes):
        """Observe inner refusal but propagate it through the committing owner."""
        observed = None
        with pytest.raises(CompositionAdmissionError, match="control_operation_unknown"):
            with self.transaction() as ledger:
                method = self.registration_method(grant)
                try:
                    method(ledger, **(inputs(grant, self.policy) | changes), expected_revision=self.expected)
                except NonproductionAuthorityError as error:
                    observed = error.reason
                    raise
        assert observed == reason, "unexpected_registration_refusal"

    def reject_read(self, grant, reason, *, expected=None):
        observed = None
        with pytest.raises(CompositionAdmissionError, match="control_operation_unknown"):
            with self.transaction() as ledger:
                try:
                    self.store().read_in(
                        ledger, grant.consumption_subject_sha256, expected_revision=expected or self.expected
                    )
                except NonproductionAuthorityError as error:
                    observed = error.reason
                    raise
        assert observed == reason, "unexpected_history_refusal"

    def original_row(self, key):
        rows = self.sql(
            f"SELECT {', '.join(REGISTRATION_COLUMNS)} FROM {self.table('grants')} WHERE consumption_subject_sha256=?;",
            key,
        )
        assert len(rows) == 1, "fixture_original_count"
        return rows[0]

    def snapshot(self):
        grants = self.sql(
            f"SELECT TOP (17) consumption_subject_sha256,grant_sha256,bundle_sha256,request_sha256 "
            f"FROM {self.table('grants')} ORDER BY consumption_subject_sha256;"
        )
        members = self.sql(
            f"SELECT TOP (66) workload_sha256,workload_document,first_grant_sha256 "
            f"FROM {self.table('memberships')} ORDER BY workload_sha256;"
        )
        assert len(grants) <= 16 and len(members) <= 65, "fixture_history_bound"
        return grants, members

    def append_policy(self, policy):
        self.trust.append(self.expected.revision + 1, trust(policy))
        self.policy = policy
        self.expected = self.trust.provider().read_revision()

    def rewrite(self, kind, statement, *parameters):
        """Administrator fault only; restore enabled trigger before any read."""
        try:
            self.sql(f"DISABLE TRIGGER {self.trigger(kind)} ON {self.table(kind)};")
            self.sql(statement, *parameters)
        finally:
            self.sql(f"ENABLE TRIGGER {self.trigger(kind)} ON {self.table(kind)};")

    @contextmanager
    def fault(self, change, restore):
        try:
            change()
            yield
        finally:
            restore()
            self.require_schema()

    def record(self, name, payload):
        if name not in _OBSERVATIONS:
            raise ValueError("registration_observation_name")
        self.record_property("dpone.registration." + name, observation_document(payload))

    def cleanup(self):
        self.trust.cleanup()


@pytest.fixture
def registration_case(record_property):
    database = require_owned_database(os.environ, system=platform.system(), machine=platform.machine())
    if os.environ.get("PYTEST_XDIST_WORKER"):
        raise RuntimeError("serial_registration_profile_required")
    case = RegistrationCase(database, record_property)
    try:
        case.install()
        yield case
    finally:
        case.cleanup()


def observation_document(payload):
    if type(payload) is not dict or not payload or len(payload) > 16:
        raise ValueError("unsafe_registration_observation")
    for key, value in payload.items():
        if key in {"sysadmin", "db_owner", "control_server", "bulk_operations"} and (
            value is None or type(value) is int and value in {0, 1}
        ):
            continue
        if (
            key in {"login_matches", "user_matches", "unexpected_success", "unclassified_sql_error"}
            and type(value) is bool
        ):
            continue
        if key == "operation_index" and type(value) is int and 1 <= value <= 6:
            continue
        if key == "operation" and type(value) is str and value in DENIAL_OPERATIONS:
            continue
        if (
            key in _COUNTS
            and type(value) is int
            and 0 <= value < 2**63
            and (key != "sql_error" or 1 <= value <= 999999)
        ):
            continue
        if key == "digest" and type(value) is str and re.fullmatch(r"sha256:[0-9a-f]{64}", value):
            continue
        if key == "xact_state" and type(value) is int and value in {-1, 0, 1}:
            continue
        if key == "lock_mode" and type(value) is str and value in {"NoLock", "Shared", "Exclusive"}:
            continue
        raise ValueError("unsafe_registration_observation")
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def sanitize_report(item, call, report):
    """Preserve outcomes and teardown diagnostics; discard all arbitrary text."""
    if item.nodeid.split("::", 1)[0] not in LIVE_MODULES:
        return
    stash = getattr(item, "stash", None)
    diagnostics = stash.get(_FAILURES, []) if stash is not None else []
    safe, seen = [], set()
    allowed = {"dpone.registration." + suffix for suffix in _OBSERVATIONS}
    for name, value in report.user_properties:
        if type(name) is not str or name in seen or name not in allowed:
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
        elif isinstance(error, (NonproductionAuthorityError, CompositionAdmissionError)):
            if type(error.reason) is str and error.reason in _REASONS:
                reason = error.reason
        report.longrepr = "SQL registration component failed: " + reason
        diagnostics = [("dpone.registration.failure", reason)]
        frame = error.__traceback__ if isinstance(error, BaseException) else None
        for _ in range(64):
            if frame is None:
                break
            path = Path(frame.tb_frame.f_code.co_filename).resolve()
            if path in {Path(__file__).resolve(), *(Path(module).resolve() for module in LIVE_MODULES)}:
                diagnostics = diagnostics[:1] + [
                    ("dpone.registration.failure_location", f"{path.name}:{frame.tb_lineno}")
                ]
            frame = frame.tb_next
        if stash is not None:
            stash[_FAILURES] = diagnostics
    elif report.skipped:
        report.longrepr = (
            item.nodeid.split("::", 1)[0],
            0,
            "requires explicit disposable Linux SQL registration runner",
        )
    report.user_properties, report.sections = safe + diagnostics, []


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    sanitize_report(item, call, outcome.get_result())
