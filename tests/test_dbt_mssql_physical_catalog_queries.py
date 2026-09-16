"""Static producer invariants; these do not certify SQL execution or visibility."""

from pathlib import Path

import pytest

from dpone.adapters.dbt_mssql_physical_catalog_queries import ENTRY, catalog_procedure
from dpone.contracts.dbt_mssql_physical_registration import MssqlPhysicalRuntimeRegistration
from tests.support.dbt_mssql_physical_registration import registration_inputs


def render(**changes):
    arguments = dict(
        catalog_sql=Path("packages/dbt-dpone/control/sqlserver/physical-v1/catalog.sql").read_bytes(),
        model_schema="models",
        model_schema_id=9,
        model_schema_owner_id=1,
        catalog_certificate_thumbprint=b"x" * 20,
    )
    arguments.update(changes)
    return catalog_procedure(MssqlPhysicalRuntimeRegistration(**registration_inputs()), **arguments)


def test_source_guard_precedes_lock_and_all_materialization():
    sql = render()
    assert ENTRY == "physical_catalog_v1"
    assert sql.index("INSERT @source EXEC") < sql.index("WITH (TABLOCK,HOLDLOCK)") < sql.index("INTO #catalog_COLUMN")
    assert "COMMIT" not in sql and "ROLLBACK" not in sql and "BEGIN TRAN" not in sql
    assert "EXECUTE AS" not in sql
    assert "sys.security_predicates" in sql[: sql.index("WITH (TABLOCK,HOLDLOCK)")]


@pytest.mark.parametrize("schema", ["dbo", "sys", "INFORMATION_SCHEMA", "native_ctl"])
def test_unsafe_namespace_rejected(schema):
    registration = MssqlPhysicalRuntimeRegistration(**registration_inputs())
    if schema == "native_ctl":
        schema = registration.local_schema
    with pytest.raises(ValueError):
        render(model_schema=schema)


@pytest.mark.parametrize(
    "changes",
    [
        dict(model_schema_id=True),
        dict(model_schema_id=0),
        dict(model_schema_owner_id=2),
        dict(catalog_certificate_thumbprint=b"x"),
    ],
)
def test_invalid_deployment_pins_rejected(changes):
    with pytest.raises(ValueError):
        render(**changes)


def test_digest_and_limits_cannot_be_replaced_by_runtime_inputs():
    from dpone.contracts.dbt_mssql_physical_registration_codec import physical_runtime_registration_digest

    registration = MssqlPhysicalRuntimeRegistration(**registration_inputs())
    sql = render()
    parameters = sql.split("AS\nBEGIN", 1)[0]
    assert "@row_limit" not in parameters and "@model_schema" not in parameters
    assert physical_runtime_registration_digest(registration) in sql
    assert f"@row_limit int={registration.limits.max_catalog_rows}" in sql
    assert f"@definition_limit int={registration.limits.max_definition_utf16_bytes}" in sql
    assert "DATALENGTH(registration_digest)=71" in sql


@pytest.mark.parametrize(
    "kind,limit",
    [
        ("COLUMN", "@column_limit"),
        ("INDEX", "@row_limit"),
        ("INDEX_COLUMN", "@row_limit"),
        ("PARTITION", "@row_limit"),
        ("DEPENDENCY", "@dependency_limit"),
        ("FORBIDDEN_PROPERTY", "@row_limit"),
    ],
)
def test_every_collection_is_overflow_bounded_before_sort_and_has_empty_marker(kind, limit):
    sql = render()
    prefix = f"SELECT TOP (CONVERT(bigint,{limit})+1) * INTO #catalog_{kind}"
    assert prefix in sql  # bigint makes the SQL-int maximum's sentinel representable.
    assert f"IF (SELECT COUNT_BIG(*) FROM #catalog_{kind})>{limit}" in sql
    assert sql.index(prefix) < sql.index(f"IF @kind='{kind}'")
    assert f"ELSE SELECT 1,'{kind}',@object_id,0,0," in sql
    assert "CONVERT(int,COUNT_BIG(*) OVER ())" in sql
    assert "DROP " not in sql and "DELETE " not in sql and "UPDATE " not in sql


def test_real_count_and_definition_budget_precede_catalog_materialization():
    sql = render()
    assert "COUNT_BIG(*) FROM '" in sql
    assert "QUOTENAME(@model_schema)" in sql and "QUOTENAME(@object_name)" in sql
    assert "p.rows" not in sql and "SUM(rows)" not in sql
    assert sql.index("DATALENGTH(filter_definition)>@definition_limit") < sql.index("INTO #catalog_INDEX")
    assert "'@count bigint OUTPUT',@count=@row_count_exact OUTPUT" in sql


def test_full_dependency_query_preserves_inbound_outbound_unresolved_and_nulls():
    sql = render()
    assert "'OUTBOUND' AS [direction]" in sql and "'INBOUND' AS [direction]" in sql
    assert "d.referenced_id IS NULL" in sql
    assert "d.referenced_schema_name IS NULL OR d.referenced_schema_name=@model_schema" in sql
    assert "d.referenced_entity_name=@object_name" in sql
    assert "DISTINCT" not in sql
    for field in (
        "referencing_id",
        "referencing_minor_id",
        "referenced_id",
        "referenced_minor_id",
        "referenced_server_name",
        "referenced_database_name",
        "referenced_schema_name",
        "referenced_entity_name",
        "is_schema_bound_reference",
        "is_caller_dependent",
        "is_ambiguous",
    ):
        assert f"d.[{field}] AS [{field}]" in sql
    assert "[referenced_entity_name] COLLATE Latin1_General_100_BIN2" in sql


def test_unsupported_catalog_rows_are_never_filtered_away():
    sql = render()
    assert "FROM sys.indexes i LEFT JOIN sys.data_spaces" in sql
    assert "i.index_id>0" not in sql and "i.type=" not in sql
    assert "FROM sys.columns c JOIN sys.types t ON t.user_type_id=c.user_type_id" in sql
    assert "LEFT JOIN sys.data_spaces s ON s.data_space_id=i.data_space_id WHERE p.object_id=@object_id" in sql
    for field in ("is_hidden", "is_masked", "is_computed", "is_identity", "is_sparse", "is_column_set"):
        assert f"c.[{field}] AS [{field}]" in sql


def test_forbidden_registry_has_no_disabled_or_permission_state_filter():
    from dpone.adapters.dbt_mssql_physical_catalog_properties import FORBIDDEN_PROPERTY_CODES, forbidden_property_query

    sql = forbidden_property_query()
    assert len(FORBIDDEN_PROPERTY_CODES) == 35
    assert sql.count("UNION ALL") == len(FORBIDDEN_PROPERTY_CODES) - 1
    assert "is_disabled" not in sql and "state=" not in sql and "is_not_trusted" not in sql
    for code in (
        "ROW_SECURITY_PREDICATE",
        "FOREIGN_KEY_INBOUND",
        "FOREIGN_KEY_OUTBOUND",
        "TABLE_LEDGER",
        "COLUMN_LEDGER",
        "COLUMN_GRAPH",
        "COLUMN_ENCRYPTION_METADATA",
        "INDEX_RESUMABLE_OPERATION",
        "PARTITION_XML_COMPRESSION",
        "DEPENDENCY_CLASS_UNSUPPORTED",
    ):
        assert code in FORBIDDEN_PROPERTY_CODES
    assert "class IN (1,7)" in sql  # Include index extended properties.
    assert "t.default_object_id<>0" in sql and "t.rule_object_id<>0" in sql
    assert "xml_compression<>0" in sql


def test_schema_literal_is_escaped_and_pin_comparison_is_binary():
    sql = render(model_schema="model']; PRINT 'unsafe")
    assert "N'model'']; PRINT ''unsafe'" in sql
    assert "CONVERT(varbinary(max),name)=CONVERT(varbinary(max),@model_schema)" in sql
    assert "DATALENGTH(name)=DATALENGTH(@model_schema)" in sql
    assert "principal_id=1" in sql


@pytest.mark.parametrize("payload", [b"", b"CREATE PROCEDURE p AS SELECT 1", b"{{UNKNOWN}}"])
def test_incomplete_or_unknown_package_substitutions_fail_closed(payload):
    with pytest.raises(ValueError):
        render(catalog_sql=payload)


def test_exact_signature_no_extra_resultset_or_implicit_transaction_settlement():
    sql = render()
    assert "(SELECT COUNT(*) FROM sys.crypt_properties WHERE class=1 AND major_id=@@PROCID)<>1" in sql
    assert "crypt_type='SPVC' AND thumbprint=0x" + (b"x" * 20).hex() in sql
    assert "SET NOCOUNT ON" in sql
    assert "SELECT * FROM @source" not in sql
    assert sql.count("ELSE SELECT 1,") == 6
    assert "SET XACT_ABORT" not in sql


def test_unresolved_inbound_dependency_classes_also_fail_closed():
    from dpone.adapters.dbt_mssql_physical_catalog_properties import forbidden_property_query

    branch = forbidden_property_query().split("'DEPENDENCY_CLASS_UNSUPPORTED'", 1)[1]
    assert "referenced_id IS NULL" in branch
    assert "referenced_entity_name=@object_name" in branch
    assert "referenced_schema_name IS NULL OR referenced_schema_name=@model_schema" in branch


def test_effective_visibility_checked_before_any_empty_security_result():
    sql = render()
    rls_position = sql.index("IF EXISTS (SELECT 1 FROM sys.security_predicates")
    for permission in (
        "HAS_PERMS_BY_NAME(DB_NAME(),'DATABASE','VIEW DEFINITION')",
        "HAS_PERMS_BY_NAME(DB_NAME(),'DATABASE','VIEW SECURITY DEFINITION')",
        "HAS_PERMS_BY_NAME(N'sys.sql_expression_dependencies','OBJECT','SELECT')",
        "HAS_PERMS_BY_NAME(QUOTENAME(@model_schema)+N'.'+QUOTENAME(@object_name),'OBJECT','SELECT')",
    ):
        assert sql.index(permission) < rls_position
        assert f"ISNULL({permission},0)<>1" in sql
    assert sql.count("@@TRANCOUNT<>1 OR XACT_STATE()<>1") >= 2


@pytest.mark.parametrize("mode", ["SHARE_METADATA", "SHARE_BUILD"])
def test_shared_observer_registration_is_outside_initial_catalog_cell(mode):
    from dataclasses import replace

    from dpone.contracts.dbt_mssql_physical_registration_values import SharedObserver

    registration = MssqlPhysicalRuntimeRegistration(**registration_inputs())
    registration = replace(
        registration, principals=replace(registration.principals, observer=SharedObserver(mode, "sha256:" + "a" * 64))
    )
    with pytest.raises(ValueError, match="dedicated observer"):
        catalog_procedure(
            registration,
            catalog_sql=Path("packages/dbt-dpone/control/sqlserver/physical-v1/catalog.sql").read_bytes(),
            model_schema="models",
            model_schema_id=9,
            model_schema_owner_id=1,
            catalog_certificate_thumbprint=b"x" * 20,
        )


def test_count_scan_occurs_only_for_count_kind_and_probe_locks_other_kinds():
    sql = render()
    count = sql.index("IF @kind='COUNT'\nBEGIN\n SET @count_sql")
    scan = sql.index("COUNT_BIG(*) FROM '")
    probe = sql.index("SELECT TOP (1) @probe=1 FROM '")
    assert count < scan < probe
    assert "@probe int OUTPUT',@probe=@lock_probe OUTPUT" in sql
    assert "WHERE 1=0" not in sql and "WHERE 0=1" not in sql
    assert "t.create_date)=@object_create_time" in sql
    assert "t.modify_date)=@object_modify_time" in sql


def test_only_header_and_selected_collection_materialize_rows():
    sql = render()
    first_materialization = sql.index("INTO #catalog_COLUMN")
    assert sql.index("SELECT 1,'TABLE'") < first_materialization
    assert sql.index("SELECT 1,'COUNT'") < first_materialization
    for kind in ("COLUMN", "INDEX", "INDEX_COLUMN", "PARTITION", "DEPENDENCY", "FORBIDDEN_PROPERTY"):
        assert f"IF @kind IN ('HEADER','{kind}')\nBEGIN\nSELECT TOP" in sql


def test_permission_denies_on_foreign_dependency_objects_fail_before_catalog():
    sql = render()
    deny = sql.index("p.grantee_principal_id IN (SELECT principal_id FROM sys.user_token)")
    assert deny < sql.index("IF EXISTS (SELECT 1 FROM sys.security_predicates")
    assert "p.state='D' AND p.class IN (0,1,3)" in sql
    assert "p.permission_name IN ('VIEW DEFINITION','VIEW SECURITY DEFINITION','CONTROL')" in sql
    assert "p.grantee_principal_id IN (SELECT principal_id FROM sys.login_token)" in sql
    assert "p.state='D' AND p.class=100" in sql
    assert "'VIEW ANY DEFINITION','VIEW ANY SECURITY DEFINITION','CONTROL SERVER'" in sql
    # No target-only major_id/schema filter: an incoming view can live anywhere.
    db_deny = sql.split('FROM sys.database_permissions p', 1)[1].split('THROW', 1)[0]
    assert "major_id" not in db_deny
