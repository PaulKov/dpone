"""Provisioning contract tests; these do not qualify SQL Server permissions."""

from pathlib import Path

import pytest

from dpone.adapters.dbt_mssql_physical_source_schema import (
    MssqlPhysicalSourceSchemaProvisioner,
    module_inventory_sql,
    principal_inventory_sql,
)
from dpone.contracts.dbt_mssql_physical_registration import MssqlPhysicalRuntimeRegistration
from tests.support.dbt_mssql_physical_registration import registration_inputs


class CatalogConnection:
    """Lifecycle fault injection only: SQL semantic qualification remains live."""

    autocommit = True

    def __init__(self, *, existing=False, fail=None):
        self.existing, self.fail = existing, fail
        self.statements, self.events, self.rows = [], [], []

    def cursor(self):
        return self

    def execute(self, sql, *parameters):
        self.statements.append((sql, parameters))
        if self.fail and self.fail in sql:
            raise RuntimeError("catalog mismatch")
        if sql.startswith("SELECT OBJECT_ID"):
            self.rows = [(100 if self.existing else None,)]
        elif "SELECT @name;" in sql:
            self.rows = [("runtime_" + str(parameters[0]),)]
        return self

    def fetchone(self):
        return self.rows.pop(0) if self.rows else None

    def commit(self):
        self.events.append("commit")
        if self.fail == "commit":
            raise RuntimeError("unknown commit")

    def rollback(self):
        self.events.append("rollback")

    def close(self):
        self.events.append("close")


def provisioner(connection):
    return MssqlPhysicalSourceSchemaProvisioner(
        connection_factory=lambda: connection,
        admission_sql=Path("packages/dbt-dpone/control/sqlserver/physical-v1/admission.sql").read_bytes(),
        certificate_name="bridge",
        certificate_public_bytes=b"public-certificate",
        certificate_user="bridge_user",
    )


def test_exact_real_producer_definitions_are_installed_before_registration(monkeypatch):
    from dpone.adapters.dbt_mssql_physical_registration_store import MssqlPhysicalRegistrationStore
    from dpone.adapters.dbt_mssql_physical_source_queries import source_procedures

    connection = CatalogConnection()
    value = MssqlPhysicalRuntimeRegistration(**registration_inputs())
    registered = []

    def register(self, actual):
        assert connection.events == ["commit", "close", "close"]
        assert sum("DPONE_SOURCE_GRANT_INVENTORY_MISMATCH" in sql for sql, _ in connection.statements) == 2
        registered.append(actual)
        return actual

    monkeypatch.setattr(MssqlPhysicalRegistrationStore, "register", register)
    assert provisioner(connection).apply(value) == value
    definitions = source_procedures(
        admission_sql=Path("packages/dbt-dpone/control/sqlserver/physical-v1/admission.sql").read_bytes(),
        model_database="example",
        local_schema="runtime_local",
        control_database="example",
        control_schema="runtime_control",
    )
    created = [sql for sql, _ in connection.statements if sql.startswith("CREATE PROCEDURE")]
    assert set(created) == set(definitions.values())
    assert registered == [value]
    signing = [sql for sql, _ in connection.statements if "ADD " in sql and "SIGNATURE TO" in sql]
    assert len(signing) == 2
    assert "ADD COUNTER SIGNATURE TO OBJECT::[runtime_control].[physical_control_require_source_v1]" in signing[0]
    assert "ADD SIGNATURE TO OBJECT::[runtime_local].[physical_require_source_v1]" in signing[1]


@pytest.mark.parametrize(
    "failure",
    [
        "DPONE_SOURCE_DATABASE_UNSAFE",
        "DPONE_SOURCE_CERTIFICATE_MISMATCH",
        "DPONE_SOURCE_RUNTIME_PRINCIPAL_UNSAFE",
        "DPONE_SOURCE_MODULE_INVENTORY_MISMATCH",
        "DPONE_SOURCE_SIGNATURE_INVENTORY_MISMATCH",
        "DPONE_SOURCE_CERTIFICATE_USER_UNSAFE",
        "DPONE_SOURCE_GRANT_INVENTORY_MISMATCH",
        "commit",
    ],
)
def test_inventory_or_commit_failure_never_registers(monkeypatch, failure):
    from dpone.adapters.dbt_mssql_physical_registration_store import MssqlPhysicalRegistrationStore

    monkeypatch.setattr(MssqlPhysicalRegistrationStore, "register", lambda *_: pytest.fail("must not register"))
    connection = CatalogConnection(existing=True, fail=failure)
    with pytest.raises(RuntimeError):
        provisioner(connection).apply(MssqlPhysicalRuntimeRegistration(**registration_inputs()))
    assert "rollback" in connection.events
    assert connection.events[-2:] == ["close", "close"]
    assert not any(sql.startswith("CREATE PROCEDURE") or "ALTER PROCEDURE" in sql for sql, _ in connection.statements)


def test_inventory_requires_exact_definitions_and_finite_signatures():
    sql = module_inventory_sql("runtime_local", "physical_require_source_v1", "bridge", "SPVC")
    for guard in (
        "execute_as_principal_id IS NULL",
        "CONVERT(varbinary(max),m.definition)",
        "sys.crypt_properties",
        "thumbprint",
        "crypt_type",
        "principal_id",
        "DATALENGTH",
    ):
        assert guard in sql
    assert "ALTER PROCEDURE" not in sql


def test_principal_check_rejects_broad_authority_and_identity_reuse():
    sql = principal_inventory_sql("runtime_local", "physical_require_source_v1", model=True)
    for guard in (
        "sys.server_principals",
        "authentication_type=1",
        "sys.server_role_members",
        "sys.database_role_members",
        "sys.server_permissions",
        "sys.database_permissions",
        "p.sid=@sid",
        "p.principal_id=@principal",
        "IMPERSONATE",
        "SCHEMA_ID",
    ):
        assert guard in sql


def test_principal_check_allows_sql2022_public_encryption_metadata_defaults():
    sql = principal_inventory_sql("runtime_local", "physical_require_source_v1", model=True)
    assert "'VIEW ANY COLUMN ENCRYPTION KEY DEFINITION'" in sql
    assert "'VIEW ANY COLUMN MASTER KEY DEFINITION'" in sql
    assert "'CONTROL'" in sql
    assert "'IMPERSONATE'" in sql


@pytest.mark.parametrize("public", [b"", "public", None])
def test_certificate_identity_must_be_nonempty_public_bytes(public):
    with pytest.raises(ValueError, match="public certificate"):
        MssqlPhysicalSourceSchemaProvisioner(
            connection_factory=lambda: pytest.fail("must not connect"),
            admission_sql=Path("packages/dbt-dpone/control/sqlserver/physical-v1/admission.sql").read_bytes(),
            certificate_name="bridge",
            certificate_public_bytes=public,
            certificate_user="bridge_user",
        )
