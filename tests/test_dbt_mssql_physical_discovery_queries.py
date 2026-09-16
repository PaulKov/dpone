"""Executable producer structure, without claiming SQL Server certification."""

from pathlib import Path

from dpone.adapters.dbt_mssql_physical_discovery_queries import ENTRY, HELPER, discovery_procedures
from tests.support.dbt_mssql_physical_registration import registration_inputs


def procedures():
    values = registration_inputs()
    return discovery_procedures(
        discovery_sql=Path("packages/dbt-dpone/control/sqlserver/physical-v1/discovery.sql").read_bytes(),
        model_database=values["model_database"],
        local_schema="runtime_local",
        control_database=values["control_database"].database_name,
        control_schema="native_control",
        model_schema="analytics",
        model_schema_id=8,
        model_schema_owner_id=1,
        discovery_certificate_thumbprint=b"x" * 20,
    )


def test_fixed_modules_and_real_owner_before_namespace():
    modules = procedures()
    assert set(modules) == {ENTRY, HELPER}
    model, control = modules[ENTRY], modules[HELPER]
    assert model.index("EXEC [") < model.index("FROM sys.objects")
    assert "native_generations_v1" not in model + control
    assert "WITH (UPDLOCK,HOLDLOCK)" in control
    assert "COLLATE CATALOG_DEFAULT" in model
    assert "OBJECT_ID(N'[runtime_local].[physical_catalog_v2]')" in model
    assert "SELECT CONVERT(smallint,1)" in model
    assert "OUTPUT" in control
    assert "{{" not in model + control


def test_full_projections_binding_and_visibility_are_retained():
    model = procedures()[ENTRY]
    assert "r.max_generation_bytes" in model
    assert "r.observer_permission_contract_sha256" in model
    assert "DPONE_CATALOG_BINDING_NONCANONICAL" in model
    assert "sys.user_token" in model
    assert "sys.login_token" in model
    assert "sys.filegroups" in model
    assert "is_read_only=0" in model
    assert "TRY_CONVERT(uniqueidentifier" in model


def test_registration_projection_policy_is_reused_without_omission():
    from dpone.adapters.dbt_mssql_physical_source_queries import _projections
    from dpone.adapters.native_generation_mssql_owner import physical_owner

    modules = procedures()
    assert _projections() in modules[ENTRY]
    assert physical_owner("native_control") in modules[HELPER]
    assert "'CPVC'" in modules[HELPER]
    assert "'SPVC'" in modules[ENTRY]
    assert "DPONE_DISCOVERY_SUBJECT_ARRAY_NONCANONICAL" in modules[HELPER]
    assert modules[ENTRY].index("DPONE_DISCOVERY_METADATA_DENY_UNSUPPORTED") < modules[ENTRY].index("FROM sys.objects")


def test_unknown_or_missing_template_substitution_rejects():
    import pytest

    source = Path("packages/dbt-dpone/control/sqlserver/physical-v1/discovery.sql").read_bytes()
    values = registration_inputs()
    for payload in (source + b"{{UNKNOWN}}", source.replace(b"{{MODEL_CALLER}}", b"")):
        with pytest.raises(ValueError):
            discovery_procedures(
                discovery_sql=payload,
                model_database=values["model_database"],
                local_schema="runtime_local",
                control_database=values["control_database"].database_name,
                control_schema="native_control",
                model_schema="models",
                model_schema_id=8,
                model_schema_owner_id=1,
                discovery_certificate_thumbprint=b"x" * 20,
            )


def test_native_lexical_token_guard_precedes_object_loop():
    from dpone.contracts.native_delivery_json import MAX_NATIVE_JSON_TOKENS

    modules = procedures()
    for sql in modules.values():
        assert f">{MAX_NATIVE_JSON_TOKENS}" in sql
        assert "CONVERT(bigint,113)" in sql
        assert "COUNT_BIG(*) FROM OPENJSON(@json,'$.workspace_attempt.write_subjects')" in sql
        assert "COUNT_BIG(*) FROM OPENJSON(@json,'$.objects')" in sql
    assert modules[ENTRY].index("DPONE_DISCOVERY_TOKEN_BOUND") < modules[ENTRY].index("WHILE @i<@count")
