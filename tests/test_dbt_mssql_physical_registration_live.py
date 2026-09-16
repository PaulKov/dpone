"""Opt-in synthetic SQL storage proof; no bridge or model admission claim."""

from __future__ import annotations

import os
import secrets
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from hashlib import sha256
from uuid import UUID, uuid4

import pytest

from dpone.adapters.dbt_mssql_physical_registration_schema import MssqlPhysicalRegistrationSchemaMigration
from dpone.adapters.dbt_mssql_physical_registration_store import (
    MssqlPhysicalRegistrationStore,
    PhysicalRegistrationStorageError,
)
from dpone.contracts.dbt_mssql_physical_registration import MssqlPhysicalRuntimeRegistration
from dpone.contracts.dbt_mssql_physical_registration_values import (
    DatabasePrincipal,
    DatabaseRoleMapping,
    DedicatedObserver,
    PlatformSelection,
    ProgramAuthority,
    RegisteredPrincipals,
)
from dpone.contracts.dbt_workspace_runtime_authority import DbtWorkspaceRuntimeAuthority
from dpone.contracts.mssql_database_authority import MssqlDatabaseAuthorityPin
from dpone.contracts.native_delivery_json import encode_native_delivery_json
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_originals import NativePlatformOriginalSubject
from tests.support.dbt_mssql_physical_registration import registration_inputs
from tests.test_native_originals_mssql_live import CommitFault

pytestmark = [
    pytest.mark.integration_live,
    pytest.mark.skipif(
        os.environ.get("DPONE_RUN_PHYSICAL_REGISTRATION_LIVE") != "1", reason="isolated SQL storage disabled"
    ),
]


def synthetic_registration(pin, principals):
    """The synthetic provisioner explicitly admits its own retained source bytes.

    These documents model external trust inputs only. They are not published
    native-original kinds, a reviewed release, or actual runtime qualification.
    """
    retained = {}

    def retain(name, document):
        payload = encode_native_delivery_json(document)
        retained[name] = payload
        return OriginalRef("synthetic/" + name, "sha256:" + sha256(payload).hexdigest())

    sources = {
        name: retain(name, {"synthetic_platform_input": name, "scope": "storage-test-only"})
        for name in (
            "control",
            "capacity",
            "profile",
            "toolchain",
            "program",
            "package",
            "macro",
            "service",
            "authority",
        )
    }
    digest = sources["authority"].sha256
    authority = DbtWorkspaceRuntimeAuthority.build(
        environment="test",
        release_id=digest,
        deployment_id=digest,
        release_sha256=digest,
        deployment_sha256=digest,
        binding_set_sha256=digest,
        connection_registry_sha256=digest,
        credential_runtime_sha256=digest,
    )
    policy = retain("policy", {name: {"locator": ref.locator, "sha256": ref.sha256} for name, ref in sources.items()})
    subject = NativePlatformOriginalSubject(authority, policy.sha256)
    values = registration_inputs()
    values.update(
        registration_id=str(uuid4()),
        platform_subject=subject,
        control_authority=sources["control"],
        capacity_authority=sources["capacity"],
        trusted_profile=PlatformSelection(sources["profile"], subject),
        trusted_toolchain=PlatformSelection(sources["toolchain"], subject),
        program=ProgramAuthority(sources["program"].sha256, sources["package"].sha256, sources["macro"].sha256),
        service_authority_sha256=sources["service"].sha256,
        control_database=pin,
        model_database=pin,
        principals=principals,
        control_schema="dpone_physical",
        local_schema="dpone_physical",
    )
    return MssqlPhysicalRuntimeRegistration(**values), retained


@pytest.fixture
def storage():
    pyodbc = pytest.importorskip("pyodbc")
    password = os.environ["DPONE_NATIVE_SQL_TEST_PASSWORD"]
    host = os.environ["DPONE_NATIVE_SQL_TEST_HOST"]
    database = "physical_storage_" + uuid4().hex
    credentials = [("physical_" + uuid4().hex, "Aa!9" + secrets.token_hex(24)) for _ in range(3)]

    def connect(user="sa", secret=password, db=database, autocommit=False):
        connection = pyodbc.connect(
            f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={host};DATABASE={db};UID={user};PWD={secret};"
            "Encrypt=no;TrustServerCertificate=yes",
            timeout=5,
            autocommit=autocommit,
        )
        connection.timeout = 10
        return connection

    admin = connect(db="master", autocommit=True)
    created = []
    try:
        admin.execute(f"CREATE DATABASE [{database}]")
        setup = connect(autocommit=True)
        try:
            setup.execute("CREATE SCHEMA dpone_physical AUTHORIZATION dbo")
            mappings = []
            for login, secret in credentials:
                admin.execute(f"CREATE LOGIN [{login}] WITH PASSWORD='{secret}', CHECK_POLICY=OFF")
                created.append(login)
                setup.execute(f"CREATE USER [{login}] FOR LOGIN [{login}]")
                row = setup.execute(
                    "SELECT principal_id,sid FROM sys.database_principals WHERE name=?", login
                ).fetchone()
                principal = DatabasePrincipal(row[0], bytes(row[1]).hex())
                mappings.append(DatabaseRoleMapping(principal, principal))
            pinrow = setup.execute(
                "SELECT DB_NAME(),DB_ID(),CONVERT(char(19),d.create_date,126)+'.'+"
                "RIGHT('0000000'+CONVERT(varchar(7),DATEPART(NANOSECOND,CONVERT(datetime2(7),d.create_date))/100),7),"
                "CONVERT(char(36),r.database_guid) FROM sys.databases d "
                "JOIN sys.database_recovery_status r ON r.database_id=d.database_id WHERE d.database_id=DB_ID()"
            ).fetchone()
            pin = MssqlDatabaseAuthorityPin(pinrow[0], pinrow[1], pinrow[2], UUID(pinrow[3]))
        finally:
            setup.close()
        principals = RegisteredPrincipals(mappings[0], mappings[1], DedicatedObserver(mappings[2]))
        value, retained = synthetic_registration(pin, principals)

        def migrate():
            MssqlPhysicalRegistrationSchemaMigration(
                connection_factory=connect,
                local_schema="dpone_physical",
                runtime_principals=tuple(mapping.model for mapping in mappings),
            ).apply()

        def store(factory=connect):
            return MssqlPhysicalRegistrationStore(connection_factory=factory, local_schema="dpone_physical")

        migrate()
        yield value, store, connect, migrate, credentials, retained
    finally:
        admin.execute(f"ALTER DATABASE [{database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE")
        admin.execute(f"DROP DATABASE [{database}]")
        for login in created:
            admin.execute(f"DROP LOGIN [{login}]")
        admin.close()


def test_live_exact_replay_conflict_and_runtime_denials(storage):
    value, store, connect, migrate, credentials, retained = storage
    assert retained and all(type(payload) is bytes for payload in retained.values())
    migrate()
    assert store().register(value) == value
    assert store().register(value) == value
    with pytest.raises(PhysicalRegistrationStorageError):
        store().register(replace(value, qualification_policy_id="different"))
    for login, secret in credentials:
        for sql in (
            "SELECT * FROM dpone_physical.physical_runtime_registrations_v1",
            "DELETE FROM dpone_physical.physical_runtime_registrations_v1",
            "UPDATE dpone_physical.physical_runtime_registrations_v1 SET payload=0x00",
            "INSERT dpone_physical.physical_runtime_registrations_v1(registration_id) VALUES(NEWID())",
            "ALTER TABLE dpone_physical.physical_runtime_registrations_v1 ADD evil int NULL",
            "CREATE TABLE dpone_physical.evil(id int)",
        ):
            runtime = connect(login, secret)
            try:
                with pytest.raises(pytest.importorskip("pyodbc").Error, match="[Pp]ermission|[Cc]annot find"):
                    runtime.execute(sql)
            finally:
                runtime.rollback()
                runtime.close()
    assert store().resolve(value) == value
    admin = connect()
    try:
        assert admin.execute("SELECT COUNT(*) FROM dpone_physical.physical_runtime_registrations_v1").fetchone()[0] == 1
    finally:
        admin.close()


@pytest.mark.parametrize("committed", [True, False])
def test_live_lost_ack_never_repeats_insert(storage, committed):
    value, store, connect, _, _, _ = storage
    calls = []

    def factory():
        calls.append(1)
        connection = connect()
        return CommitFault(connection, committed) if len(calls) == 1 else connection

    if committed:
        assert store(factory).register(value) == value
    else:
        with pytest.raises(PhysicalRegistrationStorageError):
            store(factory).register(value)
    assert len(calls) == 2


def test_live_competing_same_uuid_has_one_winner(storage):
    value, store, _, _, _, _ = storage
    competing = replace(value, qualification_policy_id="competing")

    def register(candidate):
        try:
            return store().register(candidate)
        except PhysicalRegistrationStorageError:
            return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(register, (value, competing)))
    assert len([result for result in results if result is not None]) == 1


def test_live_replay_rejects_replaced_model_principal(storage):
    value, store, connect, _, credentials, _ = storage
    store().register(value)
    admin = connect(autocommit=True)
    login = credentials[0][0]
    try:
        admin.execute(f"DROP USER [{login}]")
        admin.execute(f"CREATE USER [{login}] WITHOUT LOGIN")
        with pytest.raises(PhysicalRegistrationStorageError):
            store().register(value)
        with pytest.raises(Exception, match="PRINCIPAL_MISMATCH"):
            store().resolve(value)
    finally:
        admin.close()


def test_live_replay_rejects_missing_runtime_protection(storage):
    value, store, connect, _, credentials, _ = storage
    store().register(value)
    admin = connect(autocommit=True)
    try:
        admin.execute(
            f"REVOKE SELECT ON OBJECT::dpone_physical.physical_runtime_registrations_v1 FROM [{credentials[1][0]}]"
        )
        with pytest.raises(PhysicalRegistrationStorageError):
            store().register(value)
    finally:
        admin.close()


def test_live_replay_rejects_existing_row_in_renamed_database(storage):
    value, store, connect, _, _, _ = storage
    store().register(value)
    original = value.model_database.database_name
    renamed = "physical_renamed_" + uuid4().hex
    admin = connect(db="master", autocommit=True)
    try:
        admin.execute(f"ALTER DATABASE [{original}] MODIFY NAME = [{renamed}]")
        try:
            with pytest.raises(PhysicalRegistrationStorageError):
                store(lambda: connect(db=renamed)).register(value)
        finally:
            admin.execute(f"ALTER DATABASE [{renamed}] MODIFY NAME = [{original}]")
    finally:
        admin.close()


@pytest.mark.parametrize(
    "damage", ["nullable", "scale", "trigger", "check", "projection", "payload", "digest", "column_grant"]
)
def test_live_retained_damage_rejects_without_repair(storage, damage):
    value, store, connect, migrate, credentials, _ = storage
    store().register(value)
    admin = connect(autocommit=True)
    table = "dpone_physical.physical_runtime_registrations_v1"
    sql = {
        "nullable": f"ALTER TABLE {table} ALTER COLUMN payload varbinary(max) NULL",
        "scale": f"ALTER TABLE {table} ALTER COLUMN model_database_create_token datetime2(6) NOT NULL",
        "trigger": f"CREATE TRIGGER dpone_physical.evil ON {table} AFTER INSERT AS BEGIN SET NOCOUNT ON; END",
        "check": f"ALTER TABLE {table} NOCHECK CONSTRAINT ALL",
        "projection": f"UPDATE {table} SET max_catalog_rows=max_catalog_rows+1",
        "payload": f"UPDATE {table} SET payload=0x7b7d",
        "digest": f"UPDATE {table} SET registration_digest=0x00",
        "column_grant": f"GRANT SELECT ON OBJECT::{table}(payload) TO [{credentials[0][0]}]",
    }[damage]
    try:
        admin.execute(sql)
        if damage in {"nullable", "scale", "trigger", "check", "column_grant"}:
            with pytest.raises(Exception, match="SCHEMA_MISMATCH"):
                migrate()
        with pytest.raises(PhysicalRegistrationStorageError):
            store().register(value)
    finally:
        admin.close()
