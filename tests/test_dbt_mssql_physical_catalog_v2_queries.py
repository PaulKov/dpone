"""Reusable SQL package invariants, without claiming live SQL certification."""

from inspect import signature
from pathlib import Path

import pytest

from dpone.adapters.dbt_mssql_physical_catalog_v2_queries import ENTRY, catalog_procedure_v2
from tests.support.dbt_mssql_physical_registration import registration_inputs

TEMPLATE = Path("packages/dbt-dpone/control/sqlserver/physical-v1/catalog-v2.sql")


def render(**changes):
    values = dict(
        catalog_sql=TEMPLATE.read_bytes(),
        local_schema="runtime_local",
        model_database=registration_inputs()["model_database"],
        model_schema="models",
        model_schema_id=9,
        model_schema_owner_id=1,
        catalog_certificate_thumbprint=b"x" * 20,
    )
    values.update(changes)
    return catalog_procedure_v2(**values)


def test_module_has_no_registration_or_policy_expansion_arguments():
    assert ENTRY == "physical_catalog_v2"
    assert set(signature(catalog_procedure_v2).parameters) == {
        "catalog_sql",
        "local_schema",
        "model_database",
        "model_schema",
        "model_schema_id",
        "model_schema_owner_id",
        "catalog_certificate_thumbprint",
    }
    sql = render()
    assert registration_inputs()["registration_id"] not in sql
    assert "@row_limit int=" not in sql
    assert "physical_catalog_bindings_v1" in sql


def test_source_capture_precedes_protected_binding_and_limits_read():
    sql = render()
    assert sql.index("INSERT @source EXEC") < sql.index("FROM [runtime_local].[physical_runtime_registrations_v1]")
    assert sql.index("FROM [runtime_local].[physical_runtime_registrations_v1]") < sql.index(
        "FROM [runtime_local].[physical_catalog_bindings_v1]"
    )
    assert sql.index("physical_catalog_bindings_v1") < sql.index("WITH (TABLOCK,HOLDLOCK)")
    assert "COMMIT" not in sql and "ROLLBACK" not in sql and "FOREIGN KEY" not in sql


@pytest.mark.parametrize("schema", ["dbo", "sys", "INFORMATION_SCHEMA", "runtime_local"])
def test_forbidden_namespace_rejected(schema):
    with pytest.raises(ValueError):
        render(model_schema=schema)


def test_all_six_limits_come_from_source_authenticated_registration():
    sql = render()
    for column in (
        "max_metadata_bytes",
        "max_generation_bytes",
        "max_catalog_rows",
        "max_definition_utf16_bytes",
        "max_dependency_rows",
        "max_columns",
    ):
        assert f"=r.{column}" in sql
    for variable in (
        "metadata_limit",
        "generation_limit",
        "row_limit",
        "definition_limit",
        "dependency_limit",
        "registered_columns",
    ):
        assert f"@{variable} IS NULL" in sql
    assert "source.registration_digest=r.registration_digest" in sql
    assert "DATALENGTH(r.registration_digest)=71" in sql
    assert "HASHBYTES('SHA2_256',@registration_payload)" in sql
    assert "@dependency_limit NOT BETWEEN 1 AND @row_limit" in sql
    assert "@registered_columns<>256" in sql
    assert "@column_limit=CASE WHEN @row_limit<@registered_columns" in sql


def test_binding_size_and_digest_are_checked_before_utf8_decode():
    sql = render()
    decode = sql.index("DECLARE @binding_utf8 TABLE")
    assert sql.index("DATALENGTH(b.payload) BETWEEN 1 AND @metadata_limit") < decode
    assert sql.index("DATALENGTH(b.payload)<=1048576") < decode
    assert sql.index("HASHBYTES('SHA2_256',@binding_payload)") < decode
    assert "b.registration_digest=@registration_digest" in sql
    assert "DATALENGTH(b.registration_digest)=DATALENGTH(@registration_digest)" in sql
    assert sql.count("WITH (HOLDLOCK)") == 2
    assert "DPONE_CATALOG_BINDING_UTF8_INVALID" in sql
    assert "DATALENGTH(@binding_payload)" in sql
    assert "@binding_payload IS NULL" in sql and "@binding_digest IS NULL" in sql


@pytest.mark.parametrize(
    "path",
    [
        "$",
        "$.platform_subject",
        "$.platform_subject.authority",
        "$.trusted_profile",
        "$.trusted_profile.reference",
        "$.trusted_profile.subject",
        "$.trusted_profile.subject.authority",
        "$.policy_member",
        "$.resource_bounds",
    ],
)
def test_every_nested_object_is_closed_and_reencoded(path):
    sql = render()
    document = "@binding" if path == "$" else f"JSON_QUERY(@binding,'{path}')"
    assert f"{document} IS NULL OR ISNULL(ISJSON({document},OBJECT),0)<>1" in sql
    assert f"FROM OPENJSON({document}))<>" in sql
    assert f"DATALENGTH({document})<>" in sql
    assert "CONVERT(varbinary(max),j.[key])=CONVERT(varbinary(max),e.name)" in sql
    assert "DATALENGTH(j.[key])=DATALENGTH(e.name) AND j.type=e.kind" in sql
    assert "DPONE_CATALOG_BINDING_NONCANONICAL" in sql


def test_binding_provenance_and_runtime_facts_have_exact_null_closed_links():
    sql = render()
    for projection in ("platform_subject", "profile_subject", "profile_locator", "profile_digest"):
        assert f"@{projection} IS NULL" in sql
        assert f"DATALENGTH(@{projection})" in sql
    for name in (
        "registration_sha256",
        "policy_member.sha256",
        "platform_subject.platform_policy_sha256",
        "resource_bounds",
        "trusted_profile.reference",
        "model_schema_id",
        "model_schema_owner_id",
        "project_archive_sha256",
        "profile_name",
        "workflow_id",
    ):
        assert f"$.{name}" in sql
    assert "HASHBYTES('SHA2_256',CONVERT(varbinary(max),OBJECT_DEFINITION(@@PROCID)))" in sql
    assert "LIKE N'%[^0-9a-f]%'" in sql
    assert "TRY_CONVERT(int," in sql
    assert "N'dpone.native-original-subject.v1'" in sql and "N'PLATFORM'" in sql
    # Retained profile subject is independent, never forced to current PLATFORM subject.
    assert "@profile_subject=@platform_subject" not in sql


def test_new_module_retains_lock_visibility_and_finite_query_semantics():
    sql = render()
    assert "HAS_PERMS_BY_NAME(DB_NAME(),'DATABASE','VIEW SECURITY DEFINITION')" in sql
    assert "SELECT principal_id FROM sys.user_token" in sql
    assert "SELECT principal_id FROM sys.login_token" in sql
    assert "IF @kind='COUNT'\nBEGIN\n SET @count_sql" in sql
    assert "SELECT TOP (1) @probe=1 FROM '" in sql
    assert sql.count("WITH (TABLOCK,HOLDLOCK)") == 2
    assert "t.create_date)=@object_create_time" in sql
    assert "t.modify_date)=@object_modify_time" in sql
    for kind in ("COLUMN", "INDEX", "INDEX_COLUMN", "PARTITION", "DEPENDENCY", "FORBIDDEN_PROPERTY"):
        assert f"IF @kind IN ('HEADER','{kind}')" in sql
        assert f"ELSE SELECT 1,'{kind}',@object_id,0,0," in sql
    assert "WHERE 1=0" not in sql
    assert "UPDATE " not in sql and "DELETE " not in sql


def test_module_stability_across_registration_and_policy_variation():
    from dataclasses import replace

    from dpone.contracts.dbt_mssql_physical_registration import MssqlPhysicalRuntimeRegistration
    from dpone.contracts.native_identity import OriginalRef

    first = MssqlPhysicalRuntimeRegistration(**registration_inputs())
    second = replace(
        first,
        registration_id="20000000-0000-0000-0000-000000000002",
        trusted_profile=replace(first.trusted_profile, reference=OriginalRef("originals/other", "sha256:" + "e" * 64)),
        limits=replace(first.limits, max_catalog_rows=100, max_metadata_bytes=65536),
    )
    assert render(model_database=first.model_database) == render(model_database=second.model_database)
    assert first.registration_id not in render() and second.registration_id not in render()
    with pytest.raises(TypeError):
        render(registration=second)


@pytest.mark.parametrize(
    "changes",
    [
        dict(model_schema_id=True),
        dict(model_schema_id=0),
        dict(model_schema_owner_id=2),
        dict(model_schema_owner_id=True),
        dict(catalog_certificate_thumbprint=b"x"),
        dict(catalog_sql=b""),
        dict(catalog_sql=b"CREATE PROCEDURE wrong AS SELECT 1"),
    ],
)
def test_invalid_deployment_inputs_fail_closed(changes):
    with pytest.raises(ValueError):
        render(**changes)


def test_database_and_schema_names_are_binary_pinned_and_safely_quoted():
    from dataclasses import replace

    pin = replace(registration_inputs()["model_database"], database_name="name']quoted")
    sql = render(model_database=pin, model_schema="model']quoted")
    assert "N'name'']quoted'" in sql and "N'model'']quoted'" in sql
    assert "CONVERT(varbinary(max),r.model_database_name)" in sql
    assert "DATALENGTH(r.model_database_name)" in sql
    assert str(pin.database_guid) in sql
    assert pin.create_token in sql


def test_registered_control_namespace_cannot_alias_model_namespace():
    sql = render()
    assert "r.control_schema<>@model_schema" in sql
    assert "ISNULL(SCHEMA_ID(r.control_schema),-1)<>9" in sql


def test_member_locator_is_exact_fixed_policy_member():
    from dpone.contracts.native_project_documents import NATIVE_POLICY_MEMBER

    sql = render()
    assert f"N'{NATIVE_POLICY_MEMBER}' IS NULL" in sql
    assert f"DATALENGTH(N'{NATIVE_POLICY_MEMBER}')" in sql


def test_discovery_can_verify_catalog_module_without_changing_existing_catalog_bytes():
    from hashlib import sha256

    from dpone.adapters.dbt_mssql_physical_catalog_v2_queries import _links

    database = registration_inputs()["model_database"]
    original = _links(database, "models", 9)
    discovery = _links(database, "models", 9, catalog_module_schema="runtime_local")
    fixed_catalog = "OBJECT_ID(N'[runtime_local].[physical_catalog_v2]')"
    assert discovery == original.replace("@@PROCID", fixed_catalog)
    assert "@@PROCID" not in discovery
    assert sha256(render().encode("utf-16-le")).hexdigest() == (
        "e081afad7b53f578fc0be5af1a9a66a09f98641214cb5d4bf131b21be89f53eb"
    )


@pytest.mark.parametrize("schema", ["x];DROP TABLE x;--", "db.schema", "", 1])
def test_catalog_binding_module_selection_accepts_only_fixed_local_schema(schema):
    from dpone.adapters.dbt_mssql_physical_catalog_v2_queries import _links

    with pytest.raises(ValueError):
        _links(registration_inputs()["model_database"], "models", 9, catalog_module_schema=schema)
