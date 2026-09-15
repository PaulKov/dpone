"""Opt-in isolated SQL Server acceptance; never a production certification."""

from __future__ import annotations

import os
import secrets
import sys
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from uuid import uuid4

import pytest

from dpone.adapters.native_originals_mssql import MssqlNativeOriginalBindings, NativeOriginalBindingError
from dpone.adapters.native_originals_mssql_schema import MssqlNativeOriginalSchemaMigration
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_originals import encode_native_original_binding
from tests.test_native_original_bindings import D, binding

pytestmark = [
    pytest.mark.integration_live,
    pytest.mark.skipif(
        os.environ.get("DPONE_RUN_NATIVE_ORIGINAL_MSSQL_LIVE") != "1",
        reason="isolated SQL original acceptance not enabled",
    ),
]


@pytest.fixture
def ledger():
    pyodbc = pytest.importorskip("pyodbc")
    password = os.environ["DPONE_NATIVE_SQL_TEST_PASSWORD"]
    host = os.environ["DPONE_NATIVE_SQL_TEST_HOST"]
    database = "native_original_" + uuid4().hex
    runtime = "native_runtime_" + uuid4().hex
    runtime_password = "Aa!9" + secrets.token_hex(24)

    def connect(user="sa", secret=password, db=database, autocommit=False):
        connection = pyodbc.connect(
            f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={host};DATABASE={db};"
            f"UID={user};PWD={secret};Encrypt=no;TrustServerCertificate=yes",
            timeout=5,
            autocommit=autocommit,
        )
        connection.timeout = 10
        return connection

    admin = connect(db="master", autocommit=True)
    created_database = False
    created_login = False
    authority = OriginalRef("control/authority", D)

    def migrate(reference=authority):
        MssqlNativeOriginalSchemaMigration(
            connection_factory=connect,
            control_schema="dpone_control",
            control_authority=reference,
            runtime_database_principal=runtime,
        ).apply()

    def runtime_connect():
        return connect(runtime, runtime_password)

    def store(factory=runtime_connect, reference=authority):
        return MssqlNativeOriginalBindings(
            connection_factory=factory,
            control_schema="dpone_control",
            control_authority=reference,
            max_binding_bytes=1048576,
        )

    try:
        admin.execute(f"CREATE DATABASE [{database}]")
        created_database = True
        admin.execute(f"CREATE LOGIN [{runtime}] WITH PASSWORD='{runtime_password}', CHECK_POLICY=OFF")
        created_login = True
        setup = connect(autocommit=True)
        try:
            setup.execute("CREATE SCHEMA dpone_control AUTHORIZATION dbo")
            setup.execute(f"CREATE USER [{runtime}] FOR LOGIN [{runtime}]")
        finally:
            setup.close()
        migrate()
        yield store, runtime_connect, connect, migrate
    finally:
        primary = sys.exception()
        cleanup_errors = []
        statements = []
        if created_database:
            statements.extend(
                [
                    f"ALTER DATABASE [{database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE",
                    f"DROP DATABASE [{database}]",
                ]
            )
        if created_login:
            statements.append(f"DROP LOGIN [{runtime}]")
        for statement in statements:
            try:
                admin.execute(statement)
            except Exception as error:
                cleanup_errors.append(error)
        try:
            admin.close()
        except Exception as error:
            cleanup_errors.append(error)
        if cleanup_errors:
            if primary is not None:
                primary.add_note("Isolated SQL fixture cleanup also failed; wrapper must remove its owned container")
            else:
                raise ExceptionGroup("Isolated SQL fixture cleanup failed", cleanup_errors)


def test_live_install_replay_conflict_permissions_and_readback(ledger):
    store, runtime, admin, migrate = ledger
    migrate()
    value = binding()
    reference = store().bind(value)
    assert store().bind(value) == reference
    resolved = store().resolve(reference, expected_subject=value.subject, expected_kind=value.kind)
    assert encode_native_original_binding(resolved) == encode_native_original_binding(value)
    with pytest.raises(NativeOriginalBindingError):
        store().bind(replace(value, object_ref=replace(value.object_ref, version="other")))
    for sql in [
        "SELECT * FROM dpone_control.native_original_bindings_v1",
        "DELETE FROM dpone_control.native_original_bindings_v1",
        "UPDATE dpone_control.native_original_authorities_v1 SET schema_version=1",
        "CREATE TABLE dpone_control.evil (id int)",
    ]:
        connection = runtime()
        try:
            with pytest.raises(Exception):
                connection.execute(sql)
        finally:
            connection.rollback()
            connection.close()
    with pytest.raises(Exception):
        migrate(OriginalRef("control/authority", "sha256:" + "b" * 64))
    with pytest.raises(Exception):
        store(reference=OriginalRef("control/authority", "sha256:" + "b" * 64)).bind(value)


class CommitFault:
    def __init__(self, connection, committed):
        self.connection, self.committed = connection, committed

    @property
    def autocommit(self):
        return self.connection.autocommit

    @autocommit.setter
    def autocommit(self, value):
        self.connection.autocommit = value

    def cursor(self):
        return self.connection.cursor()

    def commit(self):
        if self.committed:
            self.connection.commit()
        raise OSError("synthetic lost commit acknowledgement")

    def rollback(self):
        self.connection.rollback()

    def close(self):
        self.connection.close()


@pytest.mark.parametrize("committed", [True, False])
def test_live_lost_commit_ack_and_actual_rollback(ledger, committed):
    store, runtime, admin, _ = ledger
    calls = []

    def factory():
        calls.append("connect")
        connection = runtime()
        return CommitFault(connection, committed) if len(calls) == 1 else connection

    if committed:
        assert store(factory).bind(binding()).locator == binding().locator
    else:
        with pytest.raises(NativeOriginalBindingError):
            store(factory).bind(binding())
    assert len(calls) == 2
    connection = admin()
    try:
        count = connection.execute("SELECT COUNT(*) FROM dpone_control.native_original_bindings_v1").fetchone()[0]
        assert count == int(committed)
    finally:
        connection.close()


def test_live_competing_bindings_do_not_overwrite(ledger):
    store, _, _, _ = ledger
    first = binding()
    second = replace(first, object_ref=replace(first.object_ref, version="competing"))

    def publish(value):
        try:
            store().bind(value)
            return value
        except NativeOriginalBindingError:
            return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(publish, [first, second]))
    winners = [value for value in results if value is not None]
    assert len(winners) == 1
    winner = winners[0]
    assert (
        store().resolve(
            OriginalRef(winner.locator, winner.payload_sha256),
            expected_subject=winner.subject,
            expected_kind=winner.kind,
        )
        == winner
    )


@pytest.mark.parametrize("damage", ["primary_key", "nullable", "check", "trigger"])
def test_live_migration_rejects_incompatible_existing_tables(ledger, damage):
    _, _, admin, migrate = ledger
    connection = admin(autocommit=True)
    try:
        if damage == "primary_key":
            name = connection.execute(
                "SELECT name FROM sys.key_constraints WHERE parent_object_id="
                "OBJECT_ID('dpone_control.native_original_bindings_v1') AND type='PK'"
            ).fetchone()[0]
            connection.execute(f"ALTER TABLE dpone_control.native_original_bindings_v1 DROP CONSTRAINT [{name}]")
        elif damage == "nullable":
            connection.execute(
                "ALTER TABLE dpone_control.native_original_bindings_v1 ALTER COLUMN kind varbinary(128) NULL"
            )
        elif damage == "check":
            connection.execute("ALTER TABLE dpone_control.native_original_bindings_v1 NOCHECK CONSTRAINT ALL")
        else:
            connection.execute(
                "CREATE TRIGGER dpone_control.evil ON dpone_control.native_original_bindings_v1 "
                "INSTEAD OF INSERT AS BEGIN SET NOCOUNT ON; END"
            )
        with pytest.raises(Exception, match="DPONE_NATIVE_EXISTING_TABLE_SCHEMA_MISMATCH"):
            migrate()
    finally:
        connection.close()


def test_live_runtime_rejects_changed_principal_registration(ledger):
    store, _, admin, _ = ledger
    connection = admin(autocommit=True)
    try:
        connection.execute("UPDATE dpone_control.native_original_authorities_v1 SET runtime_principal_sid=0x1234")
    finally:
        connection.close()
    with pytest.raises(NativeOriginalBindingError):
        store().bind(binding())


def test_live_migration_does_not_replace_modified_procedure(ledger):
    _, _, admin, migrate = ledger
    connection = admin(autocommit=True)
    try:
        connection.execute("ALTER PROCEDURE dpone_control.native_original_bind_v1 AS SELECT 1")
        definition = connection.execute(
            "SELECT OBJECT_DEFINITION(OBJECT_ID('dpone_control.native_original_bind_v1'))"
        ).fetchone()[0]
        with pytest.raises(RuntimeError, match="differs from exact V1 definition"):
            migrate()
        assert (
            connection.execute(
                "SELECT OBJECT_DEFINITION(OBJECT_ID('dpone_control.native_original_bind_v1'))"
            ).fetchone()[0]
            == definition
        )
    finally:
        connection.close()


def test_live_compatible_reordered_columns_preserve_named_insert_semantics(ledger):
    store, _, admin, migrate = ledger
    connection = admin(autocommit=True)
    try:
        # The database belongs solely to this test and the table is still empty.
        connection.execute("DROP TABLE dpone_control.native_original_bindings_v1")
        connection.execute("""CREATE TABLE dpone_control.native_original_bindings_v1 (
            binding varbinary(max) NOT NULL CHECK (DATALENGTH(binding)>=1 AND DATALENGTH(binding)<=1048576),
            kind varbinary(128) NOT NULL, subject varbinary(max) NOT NULL,
            payload_digest varbinary(71) NOT NULL, authority_digest varbinary(71) NOT NULL,
            authority_locator varbinary(max) NOT NULL, locator varbinary(max) NOT NULL,
            locator_hash binary(32) NOT NULL PRIMARY KEY
        )""")
        migrate()
        assert store().bind(binding()).locator == binding().locator
    finally:
        connection.close()
