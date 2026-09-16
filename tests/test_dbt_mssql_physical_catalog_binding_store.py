"""Immutable binding transaction/fault tests; mocked SQL is not live evidence."""

from dataclasses import replace
from uuid import UUID

import pytest

from dpone.adapters.dbt_mssql_physical_catalog_binding_schema import (
    CatalogBindingStorageError,
    MssqlPhysicalCatalogBindingSchemaProvisioner,
)
from dpone.adapters.dbt_mssql_physical_catalog_binding_store import MssqlPhysicalCatalogBindingStore
from dpone.adapters.dbt_mssql_physical_registration_store import registration_columns
from dpone.contracts.dbt_mssql_physical_catalog_binding import (
    CatalogRegistrationBinding,
    catalog_binding_digest,
    encode_catalog_binding,
)
from dpone.contracts.dbt_mssql_physical_registration import MssqlPhysicalRuntimeRegistration
from dpone.contracts.dbt_mssql_physical_registration_codec import physical_runtime_registration_digest
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_project_documents import NATIVE_POLICY_MEMBER
from tests.support.dbt_mssql_physical_registration import registration_inputs


def values():
    registration = MssqlPhysicalRuntimeRegistration(**registration_inputs())
    registration = replace(registration, limits=replace(registration.limits, max_metadata_bytes=8192))
    binding = CatalogRegistrationBinding(
        registration_id=registration.registration_id,
        registration_sha256=physical_runtime_registration_digest(registration),
        platform_subject=registration.platform_subject,
        trusted_profile=registration.trusted_profile,
        profile_name="example",
        workflow_id="workflow",
        policy_member=OriginalRef(NATIVE_POLICY_MEMBER, registration.platform_subject.platform_policy_sha256),
        project_archive_sha256="sha256:" + "a" * 64,
        model_database_name=registration.model_database.database_name,
        model_schema="models",
        resource_bounds=registration.trusted_profile.reference,
        model_schema_id=7,
        model_schema_owner_id=1,
        catalog_module_sha256="sha256:" + "b" * 64,
    )
    return registration, binding


def stored(binding):
    return (
        binding.registration_id,
        binding.registration_sha256.encode("ascii"),
        encode_catalog_binding(binding),
        catalog_binding_digest(binding).encode("ascii"),
    )


class Database:
    """Only persistence/transaction choreography; does not execute catalog SQL."""

    def __init__(self, registration, *, binding=None, failures=(), existing=True):
        self.registration = tuple(registration_columns(registration).values())
        self.binding = binding
        self.failures = list(failures)
        self.connections = []
        self.existing = existing

    def connect(self):
        connection = Connection(self, self.failures.pop(0) if self.failures else None)
        self.connections.append(connection)
        return connection


class Connection:
    autocommit = True

    def __init__(self, database, failure):
        self.database, self.failure = database, failure
        self.rows, self.statements, self.events = [], [], []
        self.pending = None

    def cursor(self):
        return self

    def execute(self, sql, *parameters):
        self.statements.append((sql, parameters))
        if self.failure and self.failure in sql:
            raise RuntimeError("injected inventory/write failure")
        if sql.startswith("SELECT registration_id,payload,"):
            self.rows = [] if self.database.registration is None else [self.database.registration]
        elif sql.startswith("SELECT registration_id,registration_digest,"):
            self.rows = [] if self.database.binding is None else [self.database.binding]
        elif sql.startswith("INSERT "):
            self.pending = parameters
        elif sql.startswith("SELECT OBJECT_ID"):
            self.rows = [(100 if self.database.existing else None,)]
        elif sql.startswith("SELECT name FROM sys.database_principals"):
            self.rows = [("runtime_" + str(parameters[0]),)]
        return self

    def fetchone(self):
        return self.rows.pop(0) if self.rows else None

    def commit(self):
        self.events.append("commit")
        if self.failure == "commit_before":
            raise RuntimeError("commit did not persist")
        if self.pending is not None:
            self.database.binding = self.pending
        if self.failure == "commit_after":
            raise RuntimeError("commit acknowledgement lost")

    def rollback(self):
        self.events.append("rollback")
        self.pending = None

    def close(self):
        self.events.append("close")


def store(database):
    return MssqlPhysicalCatalogBindingStore(connection_factory=database.connect, local_schema="runtime_local")


def test_insert_then_independent_readback_holds_registration_before_binding():
    registration, binding = values()
    database = Database(registration)
    assert store(database).register(registration, binding) == binding
    assert database.binding == stored(binding)
    assert len(database.connections) == 2
    for connection in database.connections:
        selects = [sql for sql, _ in connection.statements if sql.startswith("SELECT registration_id")]
        assert "physical_runtime_registrations_v1" in selects[0]
        assert "physical_catalog_bindings_v1" in selects[1]
        assert all("HOLDLOCK" in sql for sql in selects)
        assert connection.events == ["commit", "close", "close"]
    assert "UPDLOCK" in database.connections[0].statements[-2][0]
    assert not any(sql.startswith("INSERT") for sql, _ in database.connections[1].statements)


def test_exact_replay_never_mutates():
    registration, binding = values()
    database = Database(registration, binding=stored(binding))
    assert store(database).register(registration, binding) == binding
    assert len(database.connections) == 2
    assert not any(
        sql.startswith(("INSERT", "UPDATE", "DELETE"))
        for connection in database.connections
        for sql, _ in connection.statements
    )


@pytest.mark.parametrize("failure,success", [("commit_after", True), ("commit_before", False), ("INSERT", False)])
def test_uncertain_write_reconciles_once_without_mutation_retry(failure, success):
    registration, binding = values()
    database = Database(registration, failures=[failure])
    if success:
        assert store(database).register(registration, binding) == binding
    else:
        with pytest.raises(CatalogBindingStorageError):
            store(database).register(registration, binding)
    assert len(database.connections) == 2
    assert "rollback" in database.connections[0].events
    assert not any(sql.startswith("INSERT") for sql, _ in database.connections[1].statements)


@pytest.mark.parametrize(
    "index,bad",
    [(0, None), (1, None), (1, b"sha256:" + b"0" * 64), (2, None), (2, b"{}"), (3, None), (3, b"sha256:" + b"0" * 64)],
)
def test_binding_uuid_conflict_or_null_has_no_success(index, bad):
    registration, binding = values()
    row = list(stored(binding))
    row[index] = bad
    database = Database(registration, binding=tuple(row))
    with pytest.raises(CatalogBindingStorageError):
        store(database).register(registration, binding)
    assert len(database.connections) == 2
    assert not any(sql.startswith("INSERT") for connection in database.connections for sql, _ in connection.statements)


@pytest.mark.parametrize("index", [1, 2, 3, 20, 34, 52])
def test_registration_payload_digest_and_all_projections_are_required(index):
    registration, binding = values()
    database = Database(registration, binding=stored(binding))
    row = list(database.registration)
    row[index] = None
    database.registration = tuple(row)
    with pytest.raises(CatalogBindingStorageError):
        store(database).resolve(registration, binding)
    assert not any(
        "FROM [runtime_local].[physical_catalog_bindings_v1]" in sql for sql, _ in database.connections[0].statements
    )


@pytest.mark.parametrize(
    "failure",
    [
        "DPONE_PHYSICAL_REGISTRATION_SCHEMA_MISMATCH",
        "DPONE_PHYSICAL_REGISTRATION_PROTECTION_MISSING",
        "DPONE_CATALOG_BINDING_SCHEMA_MISMATCH",
        "DPONE_CATALOG_BINDING_PROTECTION_MISMATCH",
    ],
)
def test_schema_and_permission_drift_never_repairs(failure):
    registration, binding = values()
    database = Database(registration, binding=stored(binding), failures=[failure])
    with pytest.raises(RuntimeError):
        store(database).resolve(registration, binding)
    assert database.connections[0].events == ["rollback", "close", "close"]
    assert not any(
        sql.startswith(("INSERT", "UPDATE", "ALTER", "DENY")) for sql, _ in database.connections[0].statements
    )


def test_expected_registration_substitution_fails_before_connection():
    registration, binding = values()
    changed = replace(registration, registration_id=str(UUID(int=42)))
    database = Database(registration)
    with pytest.raises(ValueError):
        store(database).register(changed, binding)
    assert database.connections == []


def test_schema_install_adds_only_companion_denies_and_replay_has_no_repairs():
    registration, _ = values()
    for existing in (False, True):
        database = Database(registration, existing=existing)
        MssqlPhysicalCatalogBindingSchemaProvisioner(
            connection_factory=database.connect, local_schema="runtime_local"
        ).apply(registration)
        statements = database.connections[0].statements
        denies = [sql for sql, _ in statements if sql.startswith("DENY")]
        assert len(denies) == (0 if existing else 3)
        assert all("physical_catalog_bindings_v1" in sql for sql in denies)
        assert not any("ALTER TABLE" in sql or "FOREIGN KEY" in sql for sql, _ in statements)
        assert database.connections[0].events == ["commit", "close", "close"]


def test_transaction_shape_is_checked_before_registration_lock():
    registration, binding = values()
    database = Database(registration, binding=stored(binding))
    store(database).resolve(registration, binding)
    assert "@@TRANCOUNT<>1" in database.connections[0].statements[0][0]


def test_absent_binding_and_absent_registration_never_succeed():
    registration, binding = values()
    for missing_registration in (False, True):
        database = Database(registration)
        if missing_registration:
            database.registration = None
        with pytest.raises(CatalogBindingStorageError):
            store(database).resolve(registration, binding)
        assert len(database.connections) == 1


def test_reconciliation_failure_never_retries_or_returns_written_claim():
    registration, binding = values()
    database = Database(registration, failures=["commit_after", "DPONE_CATALOG_BINDING_SCHEMA_MISMATCH"])
    with pytest.raises(CatalogBindingStorageError):
        store(database).register(registration, binding)
    assert database.binding == stored(binding)
    assert len(database.connections) == 2


def test_read_commit_failure_has_no_accepted_result():
    registration, binding = values()
    database = Database(registration, binding=stored(binding), failures=["commit_after"])
    with pytest.raises(RuntimeError, match="acknowledgement"):
        store(database).resolve(registration, binding)
    assert len(database.connections) == 1
    assert database.connections[0].events == ["commit", "rollback", "close", "close"]


def test_cancellation_closes_without_reconciliation(monkeypatch):
    registration, binding = values()
    database = Database(registration)
    original = Connection.execute

    def interrupt(self, sql, *parameters):
        if sql.startswith("INSERT"):
            raise KeyboardInterrupt
        return original(self, sql, *parameters)

    monkeypatch.setattr(Connection, "execute", interrupt)
    with pytest.raises(KeyboardInterrupt):
        store(database).register(registration, binding)
    assert len(database.connections) == 1
    assert database.connections[0].events == ["rollback", "close", "close"]


def test_uuid_driver_representation_is_normalized_only_for_uuid():
    registration, binding = values()
    database = Database(registration, binding=(UUID(binding.registration_id), *stored(binding)[1:]))
    database.registration = (UUID(registration.registration_id), *database.registration[1:])
    assert store(database).resolve(registration, binding) == binding


@pytest.mark.parametrize("table", ["registration", "binding"])
def test_ambiguous_read_is_rejected(monkeypatch, table):
    registration, binding = values()
    database = Database(registration, binding=stored(binding))
    original = Connection.execute

    def duplicate(self, sql, *parameters):
        result = original(self, sql, *parameters)
        marker = (
            "SELECT registration_id,payload,"
            if table == "registration"
            else "SELECT registration_id,registration_digest,"
        )
        if sql.startswith(marker):
            self.rows *= 2
        return result

    monkeypatch.setattr(Connection, "execute", duplicate)
    with pytest.raises(CatalogBindingStorageError):
        store(database).resolve(registration, binding)


def test_schema_replay_permission_drift_never_reapplies_denies():
    registration, _ = values()
    database = Database(registration, existing=True, failures=["DPONE_CATALOG_BINDING_PROTECTION_MISMATCH"])
    with pytest.raises(RuntimeError):
        MssqlPhysicalCatalogBindingSchemaProvisioner(
            connection_factory=database.connect, local_schema="runtime_local"
        ).apply(registration)
    assert not any(sql.startswith("DENY") for sql, _ in database.connections[0].statements)
    assert database.connections[0].events == ["rollback", "close", "close"]


@pytest.mark.parametrize("changed_policy", [False, True])
def test_two_registration_uuids_remain_independently_readable(monkeypatch, changed_policy):
    first_registration, first_binding = values()
    second_registration = replace(first_registration, registration_id=str(UUID(int=43)))
    if changed_policy:
        second_registration = replace(
            second_registration,
            platform_subject=replace(second_registration.platform_subject, platform_policy_sha256="sha256:" + "e" * 64),
            trusted_profile=replace(
                second_registration.trusted_profile,
                reference=OriginalRef("originals/profile-two", "sha256:" + "f" * 64),
            ),
        )
    second_binding = replace(
        first_binding,
        registration_id=second_registration.registration_id,
        registration_sha256=physical_runtime_registration_digest(second_registration),
        platform_subject=second_registration.platform_subject,
        trusted_profile=second_registration.trusted_profile,
        resource_bounds=second_registration.trusted_profile.reference,
        policy_member=OriginalRef(NATIVE_POLICY_MEMBER, second_registration.platform_subject.platform_policy_sha256),
    )
    registrations = {
        value.registration_id: tuple(registration_columns(value).values())
        for value in (first_registration, second_registration)
    }
    bindings = {}
    database = Database(first_registration)
    original_execute, original_commit = Connection.execute, Connection.commit

    def execute(self, sql, *parameters):
        result = original_execute(self, sql, *parameters)
        if sql.startswith("SELECT registration_id,payload,"):
            self.rows = [registrations[parameters[0]]]
        elif sql.startswith("SELECT registration_id,registration_digest,"):
            self.rows = [bindings[parameters[0]]] if parameters[0] in bindings else []
        return result

    def commit(self):
        original_commit(self)
        if self.pending is not None:
            bindings[self.pending[0]] = self.pending

    monkeypatch.setattr(Connection, "execute", execute)
    monkeypatch.setattr(Connection, "commit", commit)
    for registration, binding in ((first_registration, first_binding), (second_registration, second_binding)):
        assert store(database).register(registration, binding) == binding
    assert store(database).resolve(first_registration, first_binding) == first_binding
    assert store(database).resolve(second_registration, second_binding) == second_binding
    assert len(bindings) == 2
