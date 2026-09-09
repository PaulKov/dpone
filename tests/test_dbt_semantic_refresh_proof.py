from __future__ import annotations

from dataclasses import replace

import pytest

from dpone.adapters.dbt_semantic_refresh_sql_proof import (
    prove_target_independent_compiled_sql,
)
from dpone.contracts.dbt_semantic_refresh_dependency_proof import (
    SqlServerCatalogSnapshot,
    SqlServerDependencyEdge,
    SqlServerDependencyLimits,
    SqlServerObjectMetadata,
    prove_sqlserver_read_dependencies,
)
from dpone.contracts.dbt_semantic_refresh_source_proof import (
    prove_raw_jinja_closure,
    resolve_manifest_macro_source_closure,
)


def _object(
    object_id: int,
    name: str,
    *,
    object_type: str,
    definition: str | None = None,
    edges: tuple[SqlServerDependencyEdge, ...] = (),
    schema_bound: bool = False,
    encrypted: bool = False,
    computed_columns: bool = False,
) -> SqlServerObjectMetadata:
    return SqlServerObjectMetadata(
        object_id=object_id,
        database="DWH",
        schema="raw",
        name=name,
        object_type=object_type,
        definition=definition,
        dependency_edges=edges,
        schema_bound=schema_bound,
        encrypted=encrypted,
        has_computed_columns=computed_columns,
    )


def _edge(source: int, target: int, **overrides: object) -> SqlServerDependencyEdge:
    values: dict[str, object] = {
        "referencing_object_id": source,
        "referenced_object_id": target,
        "referenced_server": None,
        "referenced_database": "DWH",
        "caller_dependent": False,
    }
    values.update(overrides)
    return SqlServerDependencyEdge(**values)  # type: ignore[arg-type]


def _snapshot(*objects: SqlServerObjectMetadata, **overrides: object) -> SqlServerCatalogSnapshot:
    values: dict[str, object] = {
        "database": "DWH",
        "metadata_visible": True,
        "ddl_exclusivity_proven": True,
        "objects": objects,
    }
    values.update(overrides)
    return SqlServerCatalogSnapshot(**values)  # type: ignore[arg-type]


LIMITS = SqlServerDependencyLimits(
    max_depth=4,
    max_nodes=8,
    max_edges=8,
    max_definition_bytes=4096,
)


@pytest.mark.parametrize(
    "name",
    ["orders__dbt_tmp", "dpone_sr_before_attempt", "dpone_sr_after_attempt"],
)
def test_read_dependency_proof_rejects_reserved_scratch_names(name: str) -> None:
    report = prove_sqlserver_read_dependencies(
        _snapshot(_object(1, name, object_type="USER_TABLE")),
        root_object_ids=(1,),
        target_object_id=99,
        limits=LIMITS,
    )

    assert report.status == "NONCONFORMANT"
    assert [issue.code for issue in report.issues] == ["DPONE_DBT_V2_RESERVED_RELATION_NAMESPACE"]


def test_raw_model_and_exact_macro_closure_admit_only_platform_scheduler_vars() -> None:
    report = prove_raw_jinja_closure(
        model_raw_sql=(
            "select * from {{ source('raw', 'events') }} where occurred_at >= {{ var('dpone_data_interval_start') }}"
        ),
        macro_sources={
            "macro.analytics.event_key": "{% macro event_key() %}event_id{% endmacro %}",
        },
        required_macro_ids=("macro.analytics.event_key",),
        allowed_vars=("dpone_data_interval_start", "dpone_data_interval_end"),
        maximum_source_bytes=4096,
    )

    assert report.status == "PROVEN"
    assert tuple(source_id for source_id, _digest in report.source_digests) == (
        "macro.analytics.event_key",
        "model",
    )
    assert report.proof_sha256.startswith("sha256:")


def test_raw_model_admits_only_exact_static_macro_namespace_member() -> None:
    report = prove_raw_jinja_closure(
        model_raw_sql="select {{ analytics.event_key() }} as event_id",
        macro_sources={
            "macro.analytics.event_key": "{% macro event_key() %}event_id{% endmacro %}",
        },
        required_macro_ids=("macro.analytics.event_key",),
        allowed_vars=(),
        maximum_source_bytes=4096,
    )

    assert report.status == "PROVEN"


def test_raw_model_requires_package_qualification_for_ambiguous_macro_name() -> None:
    macro_sources = {
        "macro.analytics.event_key": "{% macro event_key() %}event_id{% endmacro %}",
        "macro.shared.event_key": "{% macro event_key() %}shared_id{% endmacro %}",
    }

    ambiguous = prove_raw_jinja_closure(
        model_raw_sql="select {{ event_key() }} as event_id",
        macro_sources=macro_sources,
        required_macro_ids=tuple(macro_sources),
        allowed_vars=(),
        maximum_source_bytes=4096,
    )
    qualified = prove_raw_jinja_closure(
        model_raw_sql="select {{ analytics.event_key() }} as event_id",
        macro_sources=macro_sources,
        required_macro_ids=tuple(macro_sources),
        allowed_vars=(),
        maximum_source_bytes=4096,
    )

    assert ambiguous.status == "NONCONFORMANT"
    assert any(issue.code == "DPONE_DBT_JINJA_GLOBAL" for issue in ambiguous.issues)
    assert qualified.status == "PROVEN"


@pytest.mark.parametrize(
    "jinja",
    [
        "select * from {{ this }}",
        "{% if is_incremental() %}select 1{% endif %}",
        "{% set incremental = is_incremental %}{% if incremental() %}select 1{% endif %}",
        "{% do run_query('delete from x') %}",
        "{% set rq = run_query %}{% do rq('delete from x') %}",
        "{{ caller(run_query) }}",
        "{% call statement('mutate') %}delete from x{% endcall %}",
        "{% set execute_statement = statement %}{% do execute_statement('mutate') %}",
        "{{ caller(statement) }}",
        "select * from {{ adapter.get_relation(database='DWH', schema='raw', identifier='events') }}",
        "{% set a = adapter %}{% do a.add_query('delete from dbo.x') %}select 1",
        "{% set a = adapter %}{% do a.drop_relation(relation) %}select 1",
        "{% set relation_loader = load_relation %}{{ relation_loader(ref('events')) }}",
        "{% if target.name == 'prod' %}select 1{% endif %}",
        "{% set runtime_target = target %}{{ runtime_target.name }}",
        "{% set rq = context['run_' ~ 'query'] %}{% do rq('delete from dbo.x') %}",
        "{% set a = context['adap' ~ 'ter'] %}{% do a.add_query('delete from dbo.x') %}",
        "{% set rq = builtins['run_' ~ 'query'] %}{% do rq('delete from dbo.x') %}",
        "{% set rq = dbt['run_' ~ 'query'] %}{% do rq('delete from dbo.x') %}",
        "select '{{ cycler.__init__.__globals__.os.getcwd() }}'",
        "select '{{ ref('events') | attr('database') }}'",
        "{% if ref('events') is defined %}select 1{% endif %}",
        "{% set macro = unknown_package['mutate'] %}{% do macro() %}",
        "{% include 'unclassified.sql' %}",
        "{% if true %}{% include 'nested-unclassified.sql' %}{% endif %}",
        "select '{{ env_var('CALLER_VALUE') }}'",
        "select {{ var(dynamic_name) }}",
        "{% set runtime_var = var %}{{ runtime_var('author_owned') }}",
        "select {{ var('author_owned') }}",
    ],
)
def test_raw_jinja_runtime_target_introspection_and_dynamic_branches_fail_closed(
    jinja: str,
) -> None:
    report = prove_raw_jinja_closure(
        model_raw_sql=jinja,
        macro_sources={},
        required_macro_ids=(),
        allowed_vars=("dpone_data_interval_start", "dpone_data_interval_end"),
        maximum_source_bytes=4096,
    )

    assert report.status == "NONCONFORMANT"
    assert all(
        issue.code.startswith("DPONE_DBT_V2_") or issue.code == "DPONE_DBT_JINJA_GLOBAL" for issue in report.issues
    )


def test_raw_model_rejects_dynamic_lookup_inside_an_allowed_macro_namespace() -> None:
    report = prove_raw_jinja_closure(
        model_raw_sql="{% set macro = analytics['mut' ~ 'ate'] %}{% do macro() %}select 1",
        macro_sources={
            "macro.analytics.event_key": "{% macro event_key() %}event_id{% endmacro %}",
        },
        required_macro_ids=("macro.analytics.event_key",),
        allowed_vars=(),
        maximum_source_bytes=4096,
    )

    assert report.status == "NONCONFORMANT"
    assert any(issue.code == "DPONE_DBT_JINJA_GLOBAL" and issue.field == "jinja_global" for issue in report.issues)


def test_raw_model_rejects_alias_of_an_exact_supplied_macro() -> None:
    report = prove_raw_jinja_closure(
        model_raw_sql="{% set key = event_key %}select {{ key() }} as event_id",
        macro_sources={
            "macro.analytics.event_key": "{% macro event_key() %}event_id{% endmacro %}",
        },
        required_macro_ids=("macro.analytics.event_key",),
        allowed_vars=(),
        maximum_source_bytes=4096,
    )

    assert report.status == "NONCONFORMANT"
    assert any(issue.code == "DPONE_DBT_JINJA_GLOBAL" and issue.field == "jinja_global" for issue in report.issues)


def test_missing_macro_source_is_unverified_and_never_an_empty_closure() -> None:
    report = prove_raw_jinja_closure(
        model_raw_sql="select 1 as event_id",
        macro_sources={},
        required_macro_ids=("macro.analytics.missing",),
        allowed_vars=(),
        maximum_source_bytes=4096,
    )

    assert report.status == "UNVERIFIED"
    assert report.issues[0].code == "DPONE_DBT_V2_MACRO_CLOSURE_UNVERIFIED"


def test_manifest_macro_closure_rejects_cycles_and_node_budget_overflow() -> None:
    with pytest.raises(ValueError, match="cycle"):
        resolve_manifest_macro_source_closure(
            root_macro_ids=("macro.analytics.first",),
            macros={
                "macro.analytics.first": {
                    "macro_sql": "{% macro first() %}{{ second() }}{% endmacro %}",
                    "depends_on": {"macros": ["macro.analytics.second"]},
                },
                "macro.analytics.second": {
                    "macro_sql": "{% macro second() %}{{ first() }}{% endmacro %}",
                    "depends_on": {"macros": ["macro.analytics.first"]},
                },
            },
        )

    macros = {
        f"macro.analytics.node_{index}": {
            "macro_sql": f"{{% macro node_{index}() %}}1{{% endmacro %}}",
            "depends_on": {"macros": ([f"macro.analytics.node_{index + 1}"] if index < 4096 else [])},
        }
        for index in range(4097)
    }
    with pytest.raises(ValueError, match="node budget"):
        resolve_manifest_macro_source_closure(
            root_macro_ids=("macro.analytics.node_0",),
            macros=macros,
        )


def test_compiled_sql_is_target_independent_read_only_and_target_free() -> None:
    report = prove_target_independent_compiled_sql(
        {
            "dev": "select event_id, occurred_at from [DWH].[raw].[events]",
            "prod": " SELECT event_id, occurred_at FROM [DWH].[raw].[events] ",
        },
        forbidden_relations=(("DWH", "mart", "event_fact"),),
    )

    assert report.status == "PROVEN"
    assert report.compiled_sql_sha256 is not None
    assert report.read_relations == (("dwh", "raw", "events"),)
    assert report.issues == ()


def test_compiled_cte_alias_is_not_reported_as_a_catalog_relation() -> None:
    sql = "with scoped as (select event_id from DWH.raw.events) select event_id from scoped"

    report = prove_target_independent_compiled_sql(
        {"dev": sql, "prod": sql},
        forbidden_relations=(("DWH", "mart", "event_fact"),),
    )

    assert report.status == "PROVEN"
    assert report.read_relations == (("dwh", "raw", "events"),)


@pytest.mark.parametrize(
    "function_sql",
    (
        "dbo.calculate_payload(event_id)",
        "calculate_payload(event_id)",
    ),
)
def test_compiled_scalar_or_unresolved_function_call_never_becomes_proven(
    function_sql: str,
) -> None:
    sql = f"select {function_sql} from DWH.raw.events"

    report = prove_target_independent_compiled_sql(
        {"dev": sql, "prod": sql},
        forbidden_relations=(("DWH", "mart", "event_fact"),),
    )

    assert report.status == "NONCONFORMANT"
    assert report.issues[0].code == "DPONE_DBT_V2_FUNCTION_UNSUPPORTED"


@pytest.mark.parametrize(
    "compiled",
    [
        {
            "dev": "select event_id from raw.events",
            "prod": "select event_id, payload from raw.events",
        },
        {
            "dev": "delete from mart.event_fact",
            "prod": "delete from mart.event_fact",
        },
        {
            "dev": "select * from DWH.mart.event_fact",
            "prod": "select * from DWH.mart.event_fact",
        },
    ],
)
def test_compiled_sql_drift_mutation_or_target_read_is_nonconformant(
    compiled: dict[str, str],
) -> None:
    report = prove_target_independent_compiled_sql(
        compiled,
        forbidden_relations=(("DWH", "mart", "event_fact"),),
    )

    assert report.status == "NONCONFORMANT"
    assert report.issues


def test_compiled_sql_parse_failure_is_unverified() -> None:
    report = prove_target_independent_compiled_sql(
        {"dev": "select (", "prod": "select ("},
        forbidden_relations=(("DWH", "mart", "event_fact"),),
    )

    assert report.status == "UNVERIFIED"
    assert report.compiled_sql_sha256 is None


def test_schema_bound_view_and_inline_tvf_resolve_to_ordered_base_table_proof() -> None:
    table = _object(1, "events", object_type="USER_TABLE")
    view = _object(
        2,
        "events_view",
        object_type="VIEW",
        definition="CREATE VIEW raw.events_view WITH SCHEMABINDING AS SELECT event_id FROM raw.events",
        edges=(_edge(2, 1),),
        schema_bound=True,
    )
    inline_tvf = _object(
        3,
        "events_for_day",
        object_type="SQL_INLINE_TABLE_VALUED_FUNCTION",
        definition=(
            "CREATE FUNCTION raw.events_for_day() RETURNS TABLE WITH SCHEMABINDING "
            "AS RETURN SELECT event_id FROM raw.events_view"
        ),
        edges=(_edge(3, 2),),
        schema_bound=True,
    )

    report = prove_sqlserver_read_dependencies(
        _snapshot(inline_tvf, table, view),
        root_object_ids=(3,),
        target_object_id=99,
        limits=LIMITS,
    )

    assert report.status == "PROVEN"
    assert report.resolved_base_object_ids == (1,)
    assert report.ordered_edges == ((2, 1), (3, 2))
    assert report.module_definition_digests == (
        (2, report.module_definition_digests[0][1]),
        (3, report.module_definition_digests[1][1]),
    )
    assert all(digest.startswith("sha256:") for _, digest in report.module_definition_digests)
    assert report.catalog_sha256.startswith("sha256:")
    assert report.proof_sha256.startswith("sha256:")


@pytest.mark.parametrize(
    "root",
    [
        _object(4, "synonym", object_type="SYNONYM"),
        _object(4, "external", object_type="EXTERNAL_TABLE"),
        _object(4, "scalar", object_type="SQL_SCALAR_FUNCTION"),
        _object(4, "procedure", object_type="SQL_STORED_PROCEDURE"),
        _object(4, "computed", object_type="USER_TABLE", computed_columns=True),
        _object(
            4,
            "encrypted_view",
            object_type="VIEW",
            definition="select 1",
            schema_bound=True,
            encrypted=True,
        ),
        _object(
            4,
            "openquery_view",
            object_type="VIEW",
            definition="create view raw.v with schemabinding as select * from openquery(remote, 'select 1')",
            schema_bound=True,
        ),
    ],
)
def test_unsupported_sqlserver_objects_fail_closed(root: SqlServerObjectMetadata) -> None:
    report = prove_sqlserver_read_dependencies(
        _snapshot(root),
        root_object_ids=(4,),
        target_object_id=99,
        limits=LIMITS,
    )

    assert report.status == "NONCONFORMANT"
    assert report.issues


def test_cross_database_caller_dependent_cycle_and_target_reads_fail_closed() -> None:
    first = _object(
        1,
        "first",
        object_type="VIEW",
        definition="create view raw.first with schemabinding as select 1 as value",
        edges=(_edge(1, 2),),
        schema_bound=True,
    )
    second = _object(
        2,
        "second",
        object_type="VIEW",
        definition="create view raw.second with schemabinding as select 1 as value",
        edges=(
            _edge(2, 1),
            _edge(2, 3, referenced_database="Other"),
            _edge(2, 4, caller_dependent=True),
        ),
        schema_bound=True,
    )
    target = _object(3, "event_fact", object_type="USER_TABLE")

    report = prove_sqlserver_read_dependencies(
        _snapshot(first, second, target),
        root_object_ids=(1, 3),
        target_object_id=3,
        limits=LIMITS,
    )

    assert report.status == "NONCONFORMANT"
    assert {issue.code for issue in report.issues} >= {
        "DPONE_DBT_V2_DEPENDENCY_CYCLE",
        "DPONE_DBT_V2_CROSS_DATABASE_READ",
        "DPONE_DBT_V2_CALLER_DEPENDENCY_UNSUPPORTED",
        "DPONE_DBT_V2_TARGET_READ_UNSUPPORTED",
    }


def test_invisible_or_incomplete_metadata_is_unverified_not_an_empty_proof() -> None:
    report = prove_sqlserver_read_dependencies(
        _snapshot(metadata_visible=False),
        root_object_ids=(123,),
        target_object_id=99,
        limits=LIMITS,
    )

    assert report.status == "UNVERIFIED"
    assert report.resolved_base_object_ids == ()
    assert report.issues[0].code == "DPONE_DBT_V2_CATALOG_UNVERIFIED"


def test_missing_limits_are_not_defaulted_and_budgets_fail_closed() -> None:
    with pytest.raises(ValueError, match="positive integer"):
        SqlServerDependencyLimits(0, 1, 1, 1)

    table = _object(1, "events", object_type="USER_TABLE")
    second_table = _object(2, "accounts", object_type="USER_TABLE")
    report = prove_sqlserver_read_dependencies(
        _snapshot(table, second_table),
        root_object_ids=(1, 2),
        target_object_id=99,
        limits=replace(LIMITS, max_nodes=1),
    )
    assert report.status == "NONCONFORMANT"
    assert report.issues[0].code == "DPONE_DBT_V2_DEPENDENCY_BUDGET_EXCEEDED"
