"""Exact catalog equality is narrower than plan or execution admission."""

from dataclasses import replace
from uuid import UUID

import pytest

from dpone.contracts.dbt_mssql_physical import (
    AbsentPredecessor,
    PhysicalFilegroup,
    PhysicalModelPlan,
    PhysicalModelSpec,
    PhysicalRelation,
)
from dpone.contracts.dbt_mssql_physical_catalog_comparison import CatalogComparisonError, require_catalog_match
from dpone.contracts.dbt_mssql_physical_catalog_rows import (
    ColumnRow,
    CountRow,
    DependencyRow,
    ForbiddenPropertyRow,
    HeaderRow,
    IndexColumnRow,
    IndexRow,
    PartitionRow,
    TableRow,
)
from dpone.contracts.mssql_type_contract import MssqlCatalogColumn
from dpone.contracts.native_identity import OriginalRef

STAMP = "2026-09-16T01:02:03.1234567"


def example(layout="rowstore_none"):
    plan = PhysicalModelPlan(
        str(UUID(int=1)),
        PhysicalModelSpec(
            "model.example.fact",
            "sha256:" + "a" * 64,
            PhysicalRelation("models", "data", "fact"),
            (MssqlCatalogColumn("id", "int", False),),
            layout,
            PhysicalFilegroup(1, "PRIMARY"),
            OriginalRef("bounds", "sha256:" + "b" * 64),
        ),
        AbsentPredecessor(),
    )
    cci = layout == "columnstore"
    idx = int(cci)
    compression = {
        "rowstore_none": (0, "NONE"),
        "rowstore_row": (1, "ROW"),
        "rowstore_page": (2, "PAGE"),
        "columnstore": (3, "COLUMNSTORE"),
    }[layout]

    def prefix(kind):
        return (1, kind, 42, 1, 1)

    rows = {
        "HEADER": (
            HeaderRow(*prefix("HEADER"), 7, UUID(int=2), STAMP, STAMP, "data", "fact", "U ", 1, 1, int(cci), 1, 0, 0),
        ),
        "TABLE": (
            TableRow(
                *prefix("TABLE"), 5, "data", "fact", "U ", STAMP, STAMP, False, 0, 0, False, False, False, 0, 0, None
            ),
        ),
        "COLUMN": (
            ColumnRow(
                *prefix("COLUMN"),
                1,
                "id",
                56,
                56,
                "sys",
                "int",
                4,
                10,
                0,
                None,
                False,
                False,
                False,
                False,
                False,
                False,
                False,
                0,
                None,
                False,
                0,
                0,
            ),
        ),
        "INDEX": (
            IndexRow(
                *prefix("INDEX"),
                idx,
                plan.columnstore_index_name,
                5 if cci else 0,
                "CLUSTERED COLUMNSTORE" if cci else "HEAP",
                False,
                False,
                False,
                False,
                False,
                False,
                None,
                1,
                "PRIMARY",
                "FG",
            ),
        ),
        "INDEX_COLUMN": (IndexColumnRow(*prefix("INDEX_COLUMN"), 1, 1, 1, 0, 0, False, True, 0),) if cci else (),
        "PARTITION": (PartitionRow(*prefix("PARTITION"), idx, 1, 123, 456, *compression, 1, "PRIMARY", "FG"),),
        "DEPENDENCY": (),
        "FORBIDDEN_PROPERTY": (),
        "COUNT": (CountRow(*prefix("COUNT"), 12),),
    }
    return plan, rows


def check(plan, rows):
    require_catalog_match(rows, plan=plan, expected_object_name="fact", expected_object_create_time=STAMP)


@pytest.mark.parametrize("layout", ["rowstore_none", "rowstore_row", "rowstore_page", "columnstore"])
def test_four_exact_layouts(layout):
    check(*example(layout))


@pytest.mark.parametrize(
    "kind,field,value",
    [
        ("HEADER", "column_count", 2),
        ("HEADER", "object_id", 43),
        ("HEADER", "object_create_time", STAMP[:-1] + "8"),
        ("TABLE", "object_modify_time", STAMP[:-1] + "8"),
        ("TABLE", "schema_name", "DATA"),
        ("TABLE", "is_memory_optimized", True),
        ("TABLE", "is_filetable", True),
        ("TABLE", "lob_data_space_id", 1),
        ("COLUMN", "name", "ID"),
        ("COLUMN", "column_id", 2),
        ("COLUMN", "user_type_id", 999),
        ("COLUMN", "type_schema", "custom"),
        ("COLUMN", "max_length", 8),
        ("COLUMN", "precision", 11),
        ("COLUMN", "scale", 1),
        ("COLUMN", "is_nullable", True),
        ("COLUMN", "is_identity", True),
        ("COLUMN", "is_ansi_padded", True),
        ("COLUMN", "is_nullable", 0),
        ("INDEX", "is_disabled", True),
        ("INDEX", "data_space_type", "PS"),
        ("INDEX", "data_space_name", "primary"),
        ("PARTITION", "partition_number", 2),
        ("PARTITION", "data_compression", 2),
        ("PARTITION", "data_compression_desc", "PAGE"),
        ("PARTITION", "partition_id", 0),
        ("COUNT", "row_count_exact", -1),
        ("COLUMN", "row_count", 2),
    ],
)
def test_mismatches_fail_closed(kind, field, value):
    plan, rows = example()
    rows[kind] = (replace(rows[kind][0], **{field: value}),)
    with pytest.raises(CatalogComparisonError):
        check(plan, rows)


@pytest.mark.parametrize("field,value", [("name", "other"), ("type", 6), ("is_unique", True)])
def test_cci_requires_exact_derived_index(field, value):
    plan, rows = example("columnstore")
    rows["INDEX"] = (replace(rows["INDEX"][0], **{field: value}),)
    with pytest.raises(CatalogComparisonError):
        check(plan, rows)


@pytest.mark.parametrize(
    "field,value",
    [("column_id", 2), ("key_ordinal", 1), ("is_included_column", False), ("column_store_order_ordinal", 1)],
)
def test_cci_membership_is_exact(field, value):
    plan, rows = example("columnstore")
    rows["INDEX_COLUMN"] = (replace(rows["INDEX_COLUMN"][0], **{field: value}),)
    with pytest.raises(CatalogComparisonError):
        check(plan, rows)


@pytest.mark.parametrize("code", ["CHECK_CONSTRAINT", "COLUMN_FILESTREAM", "UNKNOWN_FUTURE_PROPERTY"])
def test_forbidden_occurrence_rejected(code):
    plan, rows = example()
    rows["HEADER"] = (replace(rows["HEADER"][0], forbidden_property_count=1),)
    rows["FORBIDDEN_PROPERTY"] = (ForbiddenPropertyRow(1, "FORBIDDEN_PROPERTY", 42, 1, 1, code, None, None),)
    with pytest.raises(CatalogComparisonError):
        check(plan, rows)


def test_dependencies_have_no_implicit_admission():
    plan, rows = example()
    rows["HEADER"] = (replace(rows["HEADER"][0], dependency_count=1),)
    rows["DEPENDENCY"] = (
        DependencyRow(1, "DEPENDENCY", 42, 1, 1, "INBOUND", 55, 0, 42, 0, None, None, None, None, False, False, False),
    )
    with pytest.raises(CatalogComparisonError):
        check(plan, rows)


def test_missing_and_unknown_resultsets_rejected():
    for key in ["COLUMN", "UNKNOWN"]:
        plan, rows = example()
        if key == "COLUMN":
            del rows[key]
        else:
            rows[key] = ()
        with pytest.raises(CatalogComparisonError):
            check(plan, rows)


@pytest.mark.parametrize(
    "dtype,type_name,type_id,length,precision,scale,collation,padding",
    [
        ("nvarchar(20)", "nvarchar", 231, 40, 0, 0, "Latin1_General_100_BIN2", True),
        ("varchar(11)", "varchar", 167, 11, 0, 0, "Latin1_General_100_BIN2", True),
        ("varbinary(7)", "varbinary", 165, 7, 0, 0, None, True),
        ("decimal(9,2)", "decimal", 106, 5, 9, 2, None, False),
        ("numeric(19,7)", "numeric", 108, 9, 19, 7, None, False),
        ("decimal(28,2)", "decimal", 106, 13, 28, 2, None, False),
        ("decimal(38,2)", "decimal", 106, 17, 38, 2, None, False),
        ("float(24)", "real", 59, 4, 24, 0, None, False),
        ("float(25)", "float", 62, 8, 53, 0, None, False),
        ("datetime2(7)", "datetime2", 42, 8, 27, 7, None, False),
        ("time(0)", "time", 41, 5, 8, 0, None, False),
        ("datetimeoffset(3)", "datetimeoffset", 43, 10, 30, 3, None, False),
    ],
)
def test_physical_dimensions(dtype, type_name, type_id, length, precision, scale, collation, padding):
    plan, rows = example()
    plan = replace(plan, spec=replace(plan.spec, columns=(MssqlCatalogColumn("id", dtype, False, collation),)))
    rows["COLUMN"] = (
        replace(
            rows["COLUMN"][0],
            type_name=type_name,
            system_type_id=type_id,
            user_type_id=type_id,
            max_length=length,
            precision=precision,
            scale=scale,
            collation_name=collation,
            is_ansi_padded=padding,
        ),
    )
    check(plan, rows)
    rows["COLUMN"] = (replace(rows["COLUMN"][0], max_length=length + 1),)
    with pytest.raises(CatalogComparisonError):
        check(plan, rows)


def test_cci_allows_zero_hobt_identity():
    plan, rows = example("columnstore")
    rows["PARTITION"] = (replace(rows["PARTITION"][0], hobt_id=0),)
    check(plan, rows)


def test_max_lob_plan_does_not_expand_supported_cell():
    plan, rows = example()
    plan = replace(plan, spec=replace(plan.spec, columns=(MssqlCatalogColumn("id", "varbinary(max)", False),)))
    rows["COLUMN"] = (
        replace(
            rows["COLUMN"][0],
            type_name="varbinary",
            system_type_id=165,
            user_type_id=165,
            max_length=-1,
            precision=0,
            is_ansi_padded=True,
        ),
    )
    with pytest.raises(CatalogComparisonError):
        check(plan, rows)


def test_valid_wire_cannot_hide_column_order_difference():
    plan, rows = example()
    plan = replace(
        plan,
        spec=replace(
            plan.spec, columns=(MssqlCatalogColumn("id", "int", False), MssqlCatalogColumn("other", "int", False))
        ),
    )
    rows["HEADER"] = (replace(rows["HEADER"][0], column_count=2),)
    first = replace(rows["COLUMN"][0], row_count=2)
    second = replace(first, row_ordinal=2, column_id=2, name="other")
    rows["COLUMN"] = (first, second)
    check(plan, rows)
    rows["COLUMN"] = (replace(first, name="other"), replace(second, name="id"))
    with pytest.raises(CatalogComparisonError):
        check(plan, rows)


def test_tampered_plan_digest_rejected_without_repairing_input():
    plan, rows = example()
    object.__setattr__(plan, "model_plan_sha256", "sha256:" + "f" * 64)
    with pytest.raises(CatalogComparisonError):
        check(plan, rows)
    assert plan.model_plan_sha256 == "sha256:" + "f" * 64


@pytest.mark.parametrize("filestream", [None, 0, 1, -1, False])
def test_native_filestream_absence_is_explicit(filestream):
    plan, rows = example()
    rows["TABLE"] = (replace(rows["TABLE"][0], filestream_data_space_id=filestream),)
    if filestream is None:
        check(plan, rows)
    else:
        with pytest.raises(CatalogComparisonError):
            check(plan, rows)
