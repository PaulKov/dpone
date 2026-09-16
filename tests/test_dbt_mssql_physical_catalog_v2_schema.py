"""V2 provisioner selects its own program and exact certificate inventory."""

from dpone.adapters import dbt_mssql_physical_catalog_v2_schema as schema
from dpone.contracts.dbt_mssql_physical_registration import MssqlPhysicalRuntimeRegistration
from tests.support.dbt_mssql_physical_registration import registration_inputs
from tests.test_dbt_mssql_physical_catalog_schema import Connection


def test_v2_install_selects_only_its_new_entry(monkeypatch):
    monkeypatch.setattr(
        schema,
        "catalog_procedure_v2",
        lambda **kwargs: "CREATE PROCEDURE [runtime_local].[physical_catalog_v2] AS SELECT 1;",
    )
    connection = Connection()
    installer = schema.MssqlPhysicalCatalogV2SchemaProvisioner(
        connection_factory=lambda: connection,
        catalog_sql=b"v2 authenticated package",
        certificate_name="catalog_v2_certificate",
        certificate_public_bytes=b"public",
        certificate_user="catalog_v2_user",
    )
    installer.apply(
        MssqlPhysicalRuntimeRegistration(**registration_inputs()),
        model_schema="models",
        model_schema_id=7,
        model_schema_owner_id=1,
    )
    sql = "\n".join(statement for statement, _ in connection.statements)
    assert "physical_catalog_v2" in sql
    assert "physical_catalog_v1" not in sql
    assert "catalog_v2_certificate" in sql
    assert connection.events[0] == "commit"


def test_v2_replay_does_not_resign_or_expand_grants(monkeypatch):
    monkeypatch.setattr(
        schema,
        "catalog_procedure_v2",
        lambda **kwargs: "CREATE PROCEDURE [runtime_local].[physical_catalog_v2] AS SELECT 1;",
    )
    connection = Connection(existing=True)
    installer = schema.MssqlPhysicalCatalogV2SchemaProvisioner(
        connection_factory=lambda: connection,
        catalog_sql=b"v2 authenticated package",
        certificate_name="catalog_v2_certificate",
        certificate_public_bytes=b"public",
        certificate_user="catalog_v2_user",
    )
    installer.apply(
        MssqlPhysicalRuntimeRegistration(**registration_inputs()),
        model_schema="models",
        model_schema_id=7,
        model_schema_owner_id=1,
    )
    statements = [statement for statement, _ in connection.statements]
    assert not any(statement.startswith(("CREATE", "ADD SIGNATURE", "GRANT")) for statement in statements)
