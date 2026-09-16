"""Catalog provisioning lifecycle checks, not live permission certification."""

from hashlib import sha256

import pytest

from dpone.adapters import dbt_mssql_physical_catalog_schema as schema
from dpone.contracts.dbt_mssql_physical_registration import MssqlPhysicalRuntimeRegistration
from tests.support.dbt_mssql_physical_registration import registration_inputs


class Connection:
    autocommit = True

    def __init__(self, *, existing=False, fail=None, thumbprint=b"c" * 20):
        self.existing, self.fail, self.thumbprint = existing, fail, thumbprint
        self.statements, self.events, self.rows = [], [], []

    def cursor(self):
        return self

    def execute(self, sql, *parameters):
        self.statements.append((sql, parameters))
        if self.fail and self.fail in sql:
            raise RuntimeError("inventory failure")
        if sql.startswith("SELECT thumbprint"):
            self.rows = [(self.thumbprint,)]
        elif sql.startswith("SELECT OBJECT_ID"):
            self.rows = [(100 if self.existing else None,)]
        elif "SELECT @name;" in sql:
            self.rows = [("runtime_" + str(parameters[0]),)]
        return self

    def fetchone(self):
        return self.rows.pop(0) if self.rows else None

    def commit(self):
        self.events.append("commit")
        if self.fail == "commit":
            raise RuntimeError("uncertain commit")

    def rollback(self):
        self.events.append("rollback")

    def close(self):
        self.events.append("close")


def provisioner(connection):
    return schema.MssqlPhysicalCatalogSchemaProvisioner(
        connection_factory=lambda: connection,
        catalog_sql=b"authenticated package bytes",
        certificate_name="catalog_certificate",
        certificate_public_bytes=b"public bytes",
        certificate_user="catalog_user",
    )


def apply(connection):
    return provisioner(connection).apply(
        MssqlPhysicalRuntimeRegistration(**registration_inputs()),
        model_schema="models",
        model_schema_id=7,
        model_schema_owner_id=1,
    )


@pytest.fixture(autouse=True)
def finite_producer(monkeypatch):
    monkeypatch.setattr(
        schema,
        "catalog_procedure",
        lambda *args, **kwargs: "CREATE PROCEDURE [runtime_local].[physical_catalog_v1] AS SELECT 1;",
    )


def test_new_deployment_separate_certificate_and_exact_grants():
    connection = Connection()
    result = apply(connection)
    assert connection.events == ["commit", "close", "close"]
    assert (
        result.module_sha256
        == sha256("CREATE PROCEDURE [runtime_local].[physical_catalog_v1] AS SELECT 1;".encode("utf-16le")).hexdigest()
    )
    assert result.model_schema == "models" and result.model_schema_id == 7
    assert result.certificate_thumbprint == b"c" * 20
    grants = [sql for sql, _ in connection.statements if sql.startswith("GRANT")]
    assert len(grants) == 5
    assert "GRANT VIEW DEFINITION TO [catalog_user];" in grants
    assert "GRANT SELECT ON OBJECT::sys.sql_expression_dependencies TO [catalog_user];" in grants
    assert "GRANT SELECT ON SCHEMA::[models] TO [catalog_user];" in grants
    assert sum("GRANT EXECUTE ON OBJECT::[runtime_local].[physical_catalog_v1]" in sql for sql in grants) == 2
    assert not any("COUNTER SIGNATURE" in sql or "ALTER PROCEDURE" in sql for sql, _ in connection.statements)


def test_existing_deployment_verifies_without_repairs():
    connection = Connection(existing=True)
    apply(connection)
    assert not any(
        sql.startswith(("GRANT", "CREATE PROCEDURE")) or "ADD SIGNATURE" in sql for sql, _ in connection.statements
    )
    assert connection.events == ["commit", "close", "close"]


@pytest.mark.parametrize(
    "failure",
    [
        "DPONE_CATALOG_DATABASE_UNSAFE",
        "DPONE_CATALOG_SCHEMA_MISMATCH",
        "DPONE_CATALOG_CERTIFICATE_MISMATCH",
        "DPONE_SOURCE_RUNTIME_PRINCIPAL_UNSAFE",
        "DPONE_CATALOG_VISIBILITY_UNSAFE",
        "DPONE_SOURCE_MODULE_INVENTORY_MISMATCH",
        "DPONE_CATALOG_SIGNATURE_MISMATCH",
        "DPONE_CATALOG_CERTIFICATE_USER_UNSAFE",
        "DPONE_CATALOG_GRANT_MISMATCH",
        "commit",
    ],
)
def test_inventory_and_uncertain_commit_failure_emit_no_report(failure):
    connection = Connection(existing=True, fail=failure)
    with pytest.raises(RuntimeError):
        apply(connection)
    assert "rollback" in connection.events
    assert connection.events[-2:] == ["close", "close"]
    assert sum(event == "commit" for event in connection.events) <= 1


@pytest.mark.parametrize("name", ["dbo", "sys", "INFORMATION_SCHEMA", "runtime_local", "runtime_control", "bad]schema"])
def test_invalid_or_reserved_schema_is_rejected_before_connect(name):
    connection = Connection()
    with pytest.raises(ValueError):
        provisioner(connection).apply(
            MssqlPhysicalRuntimeRegistration(**registration_inputs()),
            model_schema=name,
            model_schema_id=7,
            model_schema_owner_id=1,
        )
    assert connection.statements == []


@pytest.mark.parametrize("schema_id,owner", [(True, 1), (0, 1), (2**31, 1), (7, True), (7, 2)])
def test_invalid_schema_pin_rejected_before_connect(schema_id, owner):
    connection = Connection()
    with pytest.raises(ValueError):
        provisioner(connection).apply(
            MssqlPhysicalRuntimeRegistration(**registration_inputs()),
            model_schema="models",
            model_schema_id=schema_id,
            model_schema_owner_id=owner,
        )
    assert connection.statements == []


@pytest.mark.parametrize("thumbprint", [None, "c" * 20, b"c" * 19, b"c" * 21])
def test_bad_thumbprint_rejects_before_module_creation(thumbprint):
    connection = Connection(thumbprint=thumbprint)
    with pytest.raises(RuntimeError):
        apply(connection)
    assert not any(sql.startswith("CREATE PROCEDURE") for sql, _ in connection.statements)
    assert connection.events == ["rollback", "close", "close"]


def test_inventory_sql_covers_inherited_denies_and_schema_identity():
    connection = Connection()
    apply(connection)
    sql = "\n".join(statement for statement, _ in connection.statements)
    for guard in (
        "DATALENGTH",
        "state='D'",
        "grantee_principal_id IN",
        "sys.database_role_members",
        "sys.server_principals",
        "class<>1",
        "crypt_type<>'SPVC'",
        "EXCEPT",
        "principal_id=1",
        "is_trustworthy_on=0",
        "is_db_chaining_on=0",
    ):
        assert guard in sql


def test_actual_package_renderer_is_installed_with_observed_certificate(monkeypatch):
    from pathlib import Path

    from dpone.adapters.dbt_mssql_physical_catalog_queries import catalog_procedure

    monkeypatch.setattr(schema, "catalog_procedure", catalog_procedure)
    package = Path("packages/dbt-dpone/control/sqlserver/physical-v1/catalog.sql").read_bytes()
    connection = Connection()
    installer = schema.MssqlPhysicalCatalogSchemaProvisioner(
        connection_factory=lambda: connection,
        catalog_sql=package,
        certificate_name="catalog_certificate",
        certificate_public_bytes=b"public bytes",
        certificate_user="catalog_user",
    )
    registration = MssqlPhysicalRuntimeRegistration(**registration_inputs())
    observed = installer.apply(registration, model_schema="models", model_schema_id=7, model_schema_owner_id=1)
    expected = catalog_procedure(
        registration,
        catalog_sql=package,
        model_schema="models",
        model_schema_id=7,
        model_schema_owner_id=1,
        catalog_certificate_thumbprint=b"c" * 20,
    )
    created = [sql for sql, _ in connection.statements if sql.startswith("CREATE PROCEDURE")]
    assert created == [expected]
    assert observed.module_sha256 == sha256(expected.encode("utf-16le")).hexdigest()


@pytest.mark.parametrize(
    "argument,value",
    [
        ("catalog_sql", b""),
        ("catalog_sql", "sql"),
        ("certificate_public_bytes", b""),
        ("certificate_public_bytes", "public"),
    ],
)
def test_missing_authenticated_bytes_rejected_without_connect(argument, value):
    options = dict(
        connection_factory=lambda: pytest.fail("must not connect"),
        catalog_sql=b"package",
        certificate_name="catalog_certificate",
        certificate_public_bytes=b"public",
        certificate_user="catalog_user",
    )
    options[argument] = value
    with pytest.raises(ValueError):
        schema.MssqlPhysicalCatalogSchemaProvisioner(**options)


def test_connection_factory_failure_preserved_and_not_retried():
    attempts = []

    def fail():
        attempts.append(1)
        raise OSError("connection unavailable")

    installer = schema.MssqlPhysicalCatalogSchemaProvisioner(
        connection_factory=fail,
        catalog_sql=b"package",
        certificate_name="catalog_certificate",
        certificate_public_bytes=b"public",
        certificate_user="catalog_user",
    )
    with pytest.raises(OSError, match="connection unavailable"):
        installer.apply(
            MssqlPhysicalRuntimeRegistration(**registration_inputs()),
            model_schema="models",
            model_schema_id=7,
            model_schema_owner_id=1,
        )
    assert attempts == [1]


@pytest.mark.parametrize("rows", [[], [(b"c" * 20,), (b"c" * 20,)], [(b"c" * 20, 2)]])
def test_absent_ambiguous_or_extra_column_identity_rows_reject(rows):
    class BadRows(Connection):
        def execute(self, sql, *parameters):
            super().execute(sql, *parameters)
            if sql.startswith("SELECT thumbprint"):
                self.rows = list(rows)
            return self

    connection = BadRows()
    with pytest.raises(RuntimeError, match="exactly one scalar row"):
        apply(connection)
    assert connection.events == ["rollback", "close", "close"]


def test_signature_inventory_for_new_module_rejects_certificate_reuse():
    connection = Connection()
    apply(connection)
    first = next(sql for sql, _ in connection.statements if "DPONE_CATALOG_SIGNATURE_MISMATCH" in sql)
    assert "OBJECT_ID(N'[runtime_local].[physical_catalog_v1]') IS NULL" in first
