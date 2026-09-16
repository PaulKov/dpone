"""Deterministic DDL representation tests; no SQL Server execution claims."""

from dataclasses import replace

import pytest

from dpone.contracts.dbt_mssql_physical import PhysicalFilegroup, PhysicalRelation
from dpone.contracts.dbt_mssql_physical_rendering import render_candidate_create, render_candidate_layout
from dpone.contracts.dbt_mssql_physical_validation import PhysicalPlanError
from dpone.contracts.dbt_mssql_physical_wire import decode_physical_plan_set
from dpone.contracts.mssql_type_contract import MssqlCatalogColumn
from tests.support.dbt_mssql_physical import canonical, plan_set_document


def plan(layout="rowstore_none", *, managed=False):
    return decode_physical_plan_set(canonical(plan_set_document(layout=layout, managed=managed))).models[0]


@pytest.mark.parametrize("layout", ["rowstore_none", "rowstore_row", "rowstore_page", "columnstore"])
def test_every_layout_starts_with_explicit_heap_none_on_selected_filegroup(layout):
    value = plan(layout)
    assert render_candidate_create(value) == (
        f"CREATE TABLE [example].[models].[{value.candidate_name}] (\n"
        "    [id] int NOT NULL\n) ON [PRIMARY]\nWITH (DATA_COMPRESSION = NONE);"
    )


@pytest.mark.parametrize("layout,compression", [("rowstore_row", "ROW"), ("rowstore_page", "PAGE")])
def test_rowstore_layout_is_one_offline_rebuild(layout, compression):
    value = plan(layout)
    assert render_candidate_layout(value) == (
        f"ALTER TABLE [example].[models].[{value.candidate_name}] REBUILD\n"
        f"WITH (DATA_COMPRESSION = {compression}, ONLINE = OFF, MAXDOP = 1);"
    )


def test_none_layout_emits_no_statement():
    assert render_candidate_layout(plan()) is None


def test_columnstore_is_one_named_ordinary_offline_index():
    value = plan("columnstore")
    assert render_candidate_layout(value) == (
        f"CREATE CLUSTERED COLUMNSTORE INDEX [{value.columnstore_index_name}]\n"
        f"ON [example].[models].[{value.candidate_name}]\n"
        "WITH (DATA_COMPRESSION = COLUMNSTORE, ONLINE = OFF, MAXDOP = 1)\nON [PRIMARY];"
    )


@pytest.mark.parametrize("layout", ["rowstore_none", "rowstore_row", "rowstore_page", "columnstore"])
def test_actual_sql_server_collation_is_an_unquoted_literal_name(layout):
    value = plan(layout)
    value = replace(
        value,
        spec=replace(
            value.spec,
            columns=(MssqlCatalogColumn("label", "nvarchar(40)", True, "SQL_Latin1_General_CP1_CI_AS"),),
        ),
    )
    assert render_candidate_create(value) == (
        f"CREATE TABLE [example].[models].[{value.candidate_name}] (\n"
        "    [label] nvarchar(40) COLLATE SQL_Latin1_General_CP1_CI_AS NULL\n"
        ") ON [PRIMARY]\nWITH (DATA_COMPRESSION = NONE);"
    )


def test_unicode_quoted_identifiers_and_order_are_preserved():
    value = plan("columnstore")
    spec = replace(
        value.spec,
        relation=PhysicalRelation("База]data", " схема ", "never_address_target"),
        filegroup=PhysicalFilegroup(987, "FG] Я"),
        columns=(
            MssqlCatalogColumn("Цена] 🧩.x", "nvarchar(12)", True, "Latin1_General_100_CI_AS"),
            MssqlCatalogColumn("decimal", "decimal(12,3)", False),
            MssqlCatalogColumn(" [clock] ", "datetime2(7)", True),
        ),
    )
    value = replace(value, spec=spec)
    sql = render_candidate_create(value)
    assert sql == (
        f"CREATE TABLE [База]]data].[ схема ].[{value.candidate_name}] (\n"
        "    [Цена]] 🧩.x] nvarchar(12) COLLATE Latin1_General_100_CI_AS NULL,\n"
        "    [decimal] decimal(12,3) NOT NULL,\n"
        "    [ [clock]] ] datetime2(7) NULL\n) ON [FG]] Я]\nWITH (DATA_COMPRESSION = NONE);"
    )
    assert "ON [FG]] Я];" in render_candidate_layout(value)
    assert "[987]" not in sql
    assert "never_address_target" not in sql


@pytest.mark.parametrize(
    "dtype",
    [
        "bigint",
        "bit",
        "date",
        "datetime",
        "float",
        "int",
        "money",
        "real",
        "smalldatetime",
        "smallint",
        "smallmoney",
        "tinyint",
        "uniqueidentifier",
        "float(1)",
        "float(24)",
        "float(25)",
        "float(53)",
        "decimal(1,0)",
        "decimal(38,38)",
        "numeric(38,0)",
        "datetime2(0)",
        "datetime2(7)",
        "datetimeoffset(0)",
        "datetimeoffset(7)",
        "time(0)",
        "time(7)",
        "binary(1)",
        "binary(8000)",
        "varbinary(8000)",
        "char(8000)",
        "varchar(8000)",
        "nchar(4000)",
        "nvarchar(4000)",
    ],
)
def test_canonical_type_spelling_and_dimensions_remain_exact(dtype):
    value = plan()
    collation = (
        "Latin1_General_100_BIN2" if dtype.split("(", 1)[0] in {"char", "varchar", "nchar", "nvarchar"} else None
    )
    value = replace(value, spec=replace(value.spec, columns=(MssqlCatalogColumn("v", dtype, False, collation),)))
    assert f"[v] {dtype}" in render_candidate_create(value)


@pytest.mark.parametrize(
    "dtype",
    [
        "int); DROP TABLE x;--",
        "integer",
        "xml",
        "hierarchyid",
        "timestamp",
        "rowversion",
        "decimal(39,1)",
        "float(54)",
        "varchar(8001)",
        "nchar(max)",
        "datetime2(8)",
        "INT",
        "datetime2",
        "decimal(10, 2)",
    ],
)
@pytest.mark.parametrize("render", [render_candidate_create, render_candidate_layout])
def test_untrusted_or_noncanonical_type_never_renders(dtype, render):
    value = plan()
    object.__setattr__(value.spec.columns[0], "dtype", dtype)
    with pytest.raises(PhysicalPlanError):
        render(value)
    assert value.spec.columns[0].dtype == dtype


@pytest.mark.parametrize("dtype", ["varchar(max)", "nvarchar(max)", "varbinary(max)"])
@pytest.mark.parametrize("render", [render_candidate_create, render_candidate_layout])
def test_max_lob_not_supported_by_managed_physical_policy(dtype, render):
    value = plan()
    collation = None if dtype.startswith("varbinary") else "Latin1_General_100_BIN2"
    value = replace(value, spec=replace(value.spec, columns=(MssqlCatalogColumn("v", dtype, True, collation),)))
    with pytest.raises(PhysicalPlanError, match="MAX"):
        render(value)


@pytest.mark.parametrize(
    "field,bad",
    [
        ("candidate_name", "target"),
        ("helper_name", "wrong_helper"),
        ("columnstore_index_name", "arbitrary_cci"),
        ("model_plan_sha256", "sha256:" + "0" * 64),
        ("generation_id", "10000000-0000-0000-0000-000000000099"),
    ],
)
@pytest.mark.parametrize("render", [render_candidate_create, render_candidate_layout])
def test_tampered_plan_is_rejected_without_repair(field, bad, render):
    value = plan("columnstore")
    object.__setattr__(value, field, bad)
    with pytest.raises(PhysicalPlanError):
        render(value)
    assert getattr(value, field) == bad


@pytest.mark.parametrize("render", [render_candidate_create, render_candidate_layout])
def test_exact_record_type_required(render):
    for value in (None, "SELECT 1", {}, plan().to_dict()):
        with pytest.raises(PhysicalPlanError):
            render(value)


@pytest.mark.parametrize("name", ["default", "DeFaUlT", " default "])
def test_default_filegroup_alias_is_not_an_explicit_storage_selection(name):
    value = plan()
    value = replace(value, spec=replace(value.spec, filegroup=PhysicalFilegroup(17, name)))
    with pytest.raises(PhysicalPlanError, match="filegroup"):
        render_candidate_create(value)


@pytest.mark.parametrize(
    "name",
    [
        "Latin1_General_100_BIN2",
        "Latin1_General_100_CI_AS_SC_UTF8",
        "Traditional_Spanish_ci_ai",
        "Japanese_Bushu_Kakusu_140_CI_AS_KS_WS_VSS",
    ],
)
def test_collation_token_preserves_exact_catalog_spelling(name):
    value = plan()
    value = replace(value, spec=replace(value.spec, columns=(MssqlCatalogColumn("v", "varchar(12)", True, name),)))
    assert f" COLLATE {name} NULL" in render_candidate_create(value)


@pytest.mark.parametrize(
    "name",
    [
        " Latin1_General_CI_AS",
        "Latin1_General_CI_AS ",
        "Latin1_General_CI_AS\n",
        "Latin1_General\tCI_AS",
        "Latin1_General CI_AS",
        "Latin1_General_CI_AS;DROP TABLE x;--",
        "Latin1_General_CI_AS--",
        "Latin1_General_CI_AS/*comment*/",
        "[Latin1_General_CI_AS]",
        '"Latin1_General_CI_AS"',
        "'Latin1_General_CI_AS'",
        "Latin1_General_CI_AS] NULL);--",
        "Latin1.General_CI_AS",
        "@collation",
        "1Latin1_General_CI_AS",
        "_Latin1_General_CI_AS",
        "Látin1_General_CI_AS",
        "Latin1_General_CI_AS\x00",
        "A" * 129,
    ],
)
@pytest.mark.parametrize("render", [render_candidate_create, render_candidate_layout])
def test_unsafe_collation_tokens_are_rejected_without_rewriting(name, render):
    value = plan()
    value = replace(
        value, spec=replace(value.spec, columns=(MssqlCatalogColumn("v", "varchar(12)", True, "Latin1_General_CI_AS"),))
    )
    object.__setattr__(value.spec.columns[0], "collation", name)
    with pytest.raises(PhysicalPlanError):
        render(value)
    assert value.spec.columns[0].collation == name


@pytest.mark.parametrize("name", ["database_default", "DATABASE_DEFAULT", " database_default "])
def test_database_default_collation_alias_is_rejected(name):
    value = plan()
    value = replace(value, spec=replace(value.spec, columns=(MssqlCatalogColumn("v", "varchar(12)", True, name),)))
    with pytest.raises(PhysicalPlanError, match="collation"):
        render_candidate_create(value)


@pytest.mark.parametrize("layout", ["rowstore_none", "rowstore_row", "rowstore_page", "columnstore"])
def test_no_target_helper_cleanup_filtering_or_transaction_synthesis(layout):
    value = plan(layout, managed=True)
    statements = (render_candidate_create(value), render_candidate_layout(value))
    combined = "\n".join(statement for statement in statements if statement is not None)
    for forbidden in (
        "SELECT",
        "DISTINCT",
        "WHERE",
        "PRIMARY KEY",
        "UNIQUE",
        "INSERT",
        "DROP",
        "RENAME",
        "COMMIT",
        "ROLLBACK",
        "RESUMABLE",
        "ONLINE = ON",
        "COLUMNSTORE_ARCHIVE",
        "ORDER",
        "NONCLUSTERED",
        "PARTITION",
        value.helper_name,
        value.backup_name,
        "[orders]",
    ):
        assert forbidden not in combined
    assert render_candidate_create(value) == render_candidate_create(value)
    assert sum(statement.count(";") for statement in statements if statement) == (1 if layout == "rowstore_none" else 2)


def test_generation_changes_only_derived_object_identifiers_in_sql():
    first = plan("columnstore")
    second = replace(first, generation_id="10000000-0000-0000-0000-000000000002")
    assert first.candidate_name != second.candidate_name
    assert first.columnstore_index_name != second.columnstore_index_name
    assert render_candidate_create(first).replace(
        first.candidate_name, second.candidate_name
    ) == render_candidate_create(second)
    assert render_candidate_layout(first).replace(first.candidate_name, second.candidate_name).replace(
        first.columnstore_index_name, second.columnstore_index_name
    ) == render_candidate_layout(second)


def test_documented_preview_is_runnable_without_io(capsys):
    from pathlib import Path

    document = Path("docs/dbt-mssql-physical-rendering.md").read_text()
    example = document.split("```python\n", 1)[1].split("```", 1)[0]
    namespace = {}
    exec(compile(example, "documented-rendering-example", "exec"), namespace)
    assert namespace["create_sql"].startswith("CREATE TABLE [example].[models].[dpone_c_")
    assert "DATA_COMPRESSION = PAGE, ONLINE = OFF, MAXDOP = 1" in namespace["layout_sql"]
    assert capsys.readouterr().out == namespace["create_sql"] + "\n" + namespace["layout_sql"] + "\n"


@pytest.mark.parametrize(
    "name,message",
    [
        ("database_default", "rendering requires a named collation, not database_default"),
        (" database_default ", "rendering requires a named collation, not database_default"),
        ("A;--", "rendering requires a collation name containing only ASCII letters, digits and underscores"),
    ],
)
def test_shared_validator_preserves_renderer_error_contract(name, message):
    value = plan()
    value = replace(value, spec=replace(value.spec, columns=(MssqlCatalogColumn("v", "varchar(12)", True, name),)))
    with pytest.raises(PhysicalPlanError) as error:
        render_candidate_create(value)
    assert str(error.value) == message
