"""Validated DDL shapes preserve complete catalog metadata and evidence."""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace

import pytest

from dpone.readiness.schema_evolution import ColumnDef
from dpone.runtime.sinks import mssql_target_catalog_model as model
from dpone.runtime.sinks import mssql_target_catalog_types as types
from dpone.runtime.sinks.mssql_target_catalog_fingerprint import exact_schema_catalog_transition


@pytest.mark.parametrize(
    ("dtype", "expected", "rendered"),
    [
        ("bigint", ("bigint", 8, 19, 0), "bigint"),
        ("bit", ("bit", 1, 1, 0), "bit"),
        ("date", ("date", 3, 10, 0), "date"),
        ("datetime", ("datetime", 8, 23, 3), "datetime"),
        ("int", ("int", 4, 10, 0), "int"),
        ("money", ("money", 8, 19, 4), "money"),
        ("real", ("real", 4, 24, 0), "real"),
        ("smalldatetime", ("smalldatetime", 4, 16, 0), "smalldatetime"),
        ("smallint", ("smallint", 2, 5, 0), "smallint"),
        ("smallmoney", ("smallmoney", 4, 10, 4), "smallmoney"),
        ("tinyint", ("tinyint", 1, 3, 0), "tinyint"),
        ("uniqueidentifier", ("uniqueidentifier", 16, 0, 0), "uniqueidentifier"),
        ("float", ("float", 8, 53, 0), "float(53)"),
        ("float(1)", ("float", 4, 1, 0), "float(1)"),
        ("float(24)", ("float", 4, 24, 0), "float(24)"),
        ("float(25)", ("float", 8, 25, 0), "float(25)"),
        ("float(53)", ("float", 8, 53, 0), "float(53)"),
        ("decimal(1,0)", ("decimal", 5, 1, 0), "decimal(1,0)"),
        ("decimal(9,9)", ("decimal", 5, 9, 9), "decimal(9,9)"),
        ("decimal(10,2)", ("decimal", 9, 10, 2), "decimal(10,2)"),
        ("decimal(19,0)", ("decimal", 9, 19, 0), "decimal(19,0)"),
        ("decimal(20,5)", ("decimal", 13, 20, 5), "decimal(20,5)"),
        ("decimal(28,28)", ("decimal", 13, 28, 28), "decimal(28,28)"),
        ("decimal(29,0)", ("decimal", 17, 29, 0), "decimal(29,0)"),
        (" NuMeRiC ( 38 , 7 ) \n", ("numeric", 17, 38, 7), "numeric(38,7)"),
        ("char(1)", ("char", 1, 0, 0), "char(1)"),
        ("varchar(8000)", ("varchar", 8000, 0, 0), "varchar(8000)"),
        ("varchar(max)", ("varchar", -1, 0, 0), "varchar(max)"),
        ("nchar(4000)", ("nchar", 8000, 0, 0), "nchar(4000)"),
        (" NVARCHAR ( 002 ) ", ("nvarchar", 4, 0, 0), "nvarchar(2)"),
        ("nvarchar(max)", ("nvarchar", -1, 0, 0), "nvarchar(max)"),
        ("binary(8000)", ("binary", 8000, 0, 0), "binary(8000)"),
        ("varbinary(1)", ("varbinary", 1, 0, 0), "varbinary(1)"),
        ("varbinary(max)", ("varbinary", -1, 0, 0), "varbinary(max)"),
        ("time", ("time", 5, 16, 7), "time(7)"),
        ("datetime2", ("datetime2", 8, 27, 7), "datetime2(7)"),
        ("datetimeoffset", ("datetimeoffset", 10, 34, 7), "datetimeoffset(7)"),
    ],
)
def test_validated_shape_and_canonical_rendering(
    dtype: str, expected: tuple[str, int, int, int], rendered: str
) -> None:
    assert "validated_mssql_catalog_type_shape" in types.__all__
    assert types.validated_mssql_catalog_type_shape(dtype) == expected
    assert types.canonical_mssql_catalog_type(dtype) == rendered


@pytest.mark.parametrize(
    ("dtype", "expected"),
    [
        ("time(0)", ("time", 3, 8, 0)),
        ("time(1)", ("time", 3, 10, 1)),
        ("time(2)", ("time", 3, 11, 2)),
        ("time(3)", ("time", 4, 12, 3)),
        ("time(4)", ("time", 4, 13, 4)),
        ("time(5)", ("time", 5, 14, 5)),
        ("time(6)", ("time", 5, 15, 6)),
        ("time(7)", ("time", 5, 16, 7)),
        ("datetime2(0)", ("datetime2", 6, 19, 0)),
        ("datetime2(1)", ("datetime2", 6, 21, 1)),
        ("datetime2(2)", ("datetime2", 6, 22, 2)),
        ("datetime2(3)", ("datetime2", 7, 23, 3)),
        ("datetime2(4)", ("datetime2", 7, 24, 4)),
        ("datetime2(5)", ("datetime2", 8, 25, 5)),
        ("datetime2(6)", ("datetime2", 8, 26, 6)),
        ("datetime2(7)", ("datetime2", 8, 27, 7)),
        ("datetimeoffset(0)", ("datetimeoffset", 8, 26, 0)),
        ("datetimeoffset(1)", ("datetimeoffset", 8, 28, 1)),
        ("datetimeoffset(2)", ("datetimeoffset", 8, 29, 2)),
        ("datetimeoffset(3)", ("datetimeoffset", 9, 30, 3)),
        ("datetimeoffset(4)", ("datetimeoffset", 9, 31, 4)),
        ("datetimeoffset(5)", ("datetimeoffset", 10, 32, 5)),
        ("datetimeoffset(6)", ("datetimeoffset", 10, 33, 6)),
        ("datetimeoffset(7)", ("datetimeoffset", 10, 34, 7)),
    ],
)
def test_every_temporal_scale(dtype: str, expected: tuple[str, int, int, int]) -> None:
    assert types.validated_mssql_catalog_type_shape(dtype) == expected
    assert types.canonical_mssql_catalog_type(dtype) == dtype


@pytest.mark.parametrize("pipeline", ["shape", "canonical", "fresh", "alter"])
@pytest.mark.parametrize(
    ("dtype", "message"),
    [
        ("char", "unsupported MSSQL physical type: char"),
        (" XML ", "unsupported MSSQL physical type:  XML "),
        ("", "unsupported MSSQL physical type: "),
        ("nvarchar(1);DROP", "unsupported MSSQL physical type: nvarchar(1);DROP"),
        ("decimal(0,0)", "MSSQL decimal precision or scale is invalid"),
        ("numeric(39,0)", "MSSQL decimal precision or scale is invalid"),
        ("decimal(9,10)", "MSSQL decimal precision or scale is invalid"),
        ("float(0)", "MSSQL float precision must be between 1 and 53"),
        ("float(54)", "MSSQL float precision must be between 1 and 53"),
        ("time(8)", "MSSQL temporal scale must be between 0 and 7"),
        ("datetime2(8)", "MSSQL temporal scale must be between 0 and 7"),
        ("datetimeoffset(8)", "MSSQL temporal scale must be between 0 and 7"),
        ("char(max)", "MSSQL char does not support MAX"),
        ("nchar(max)", "MSSQL nchar does not support MAX"),
        ("binary(max)", "MSSQL binary does not support MAX"),
        ("nvarchar(0)", "MSSQL nvarchar length must be between 1 and 4000"),
        ("nchar(4001)", "MSSQL nchar length must be between 1 and 4000"),
        ("varchar(8001)", "MSSQL varchar length must be between 1 and 8000"),
        ("varbinary(8001)", "MSSQL varbinary length must be between 1 and 8000"),
    ],
)
def test_generated_ddl_rejects_invalid_types_before_construction(pipeline: str, dtype: str, message: str) -> None:
    with pytest.raises(ValueError) as caught:
        if pipeline == "shape":
            types.validated_mssql_catalog_type_shape(dtype)
        elif pipeline == "canonical":
            types.canonical_mssql_catalog_type(dtype)
        elif pipeline == "fresh":
            model.catalog_column_from_definition(ColumnDef("v", dtype), ordinal=1, database_collation="DB")
        else:
            model.altered_catalog_column(_catalog_column(), ColumnDef("v", dtype))
    assert type(caught.value) is ValueError
    assert str(caught.value) == message


def test_raw_parser_keeps_its_distinct_unvalidated_contract() -> None:
    assert types.mssql_catalog_type_shape("char") == ("char", 0, 0, 0)
    assert types.mssql_catalog_type_shape("nvarchar(0)") == ("nvarchar", 0, 0, 0)
    for dtype, message in [
        (" INT ", "mssql_transaction.target_catalog_type_invalid"),
        ("xml", "mssql_transaction.target_catalog_type_unsupported"),
    ]:
        with pytest.raises(ValueError) as caught:
            types.mssql_catalog_type_shape(dtype)
        assert type(caught.value) is ValueError
        assert str(caught.value) == message


def _catalog_column() -> model.MssqlCatalogColumnState:
    """Use distinct metadata sentinels to detect lossy ALTER reconstruction."""
    return model.MssqlCatalogColumnState(
        1,
        "café",
        "sys",
        "int",
        "dbo",
        "alias",
        4,
        10,
        0,
        False,
        "CURRENT",
        True,
        True,
        True,
        True,
        2,
        "((7))",
        "([x]+1)",
        "7",
        "3",
    )


@pytest.mark.parametrize("nullable", [False, True])
@pytest.mark.parametrize("collation", [None, "EXPLICIT"])
@pytest.mark.parametrize(("dtype", "base", "length"), [(" NVARCHAR(2) ", "nvarchar", 4), ("binary(2)", "binary", 2)])
def test_fresh_and_alter_preserve_full_metadata(
    nullable: bool, collation: str | None, dtype: str, base: str, length: int
) -> None:
    desired = ColumnDef("new_name", dtype, nullable=nullable, collation=collation)
    fresh = model.catalog_column_from_definition(desired, ordinal=3, database_collation="DB")
    fresh_collation = collation or ("DB" if base == "nvarchar" else None)
    expected: dict[str, object] = {
        "ordinal": 3,
        "name": "new_name",
        "system_type_schema": "sys",
        "system_type_name": base,
        "user_type_schema": "sys",
        "user_type_name": base,
        "max_length": length,
        "precision": 0,
        "scale": 0,
        "nullable": nullable,
        "collation": fresh_collation,
        "identity": False,
        "computed": False,
        "sparse": False,
        "rowguidcol": False,
        "generated_always_type": 0,
        "default_definition": None,
        "computed_definition": None,
        "identity_seed": None,
        "identity_increment": None,
    }
    assert fresh.to_dict() == expected
    assert fresh.to_column_def() == ColumnDef("new_name", f"{base}(2)", nullable, fresh_collation)
    current = _catalog_column()
    altered = model.altered_catalog_column(current, desired)
    expected.update(
        ordinal=1,
        name="café",
        collation=collation if collation is not None else "CURRENT",
        identity=True,
        computed=True,
        sparse=True,
        rowguidcol=True,
        generated_always_type=2,
        default_definition="((7))",
        computed_definition="([x]+1)",
        identity_seed="7",
        identity_increment="3",
    )
    assert altered.to_dict() == expected
    assert current == _catalog_column()


def _snapshot() -> model.MssqlSchemaCatalogSnapshot:
    return model.MssqlSchemaCatalogSnapshot(
        True,
        "DB",
        (_catalog_column(),),
        checks=(model.MssqlCheckConstraintState("ck", 1, "([café]>0)", False, True),),
        indexes=(
            model.MssqlIndexState(
                "ix",
                "NONCLUSTERED",
                True,
                False,
                True,
                False,
                False,
                True,
                "([café]>0)",
                ("café",),
                ("x",),
                (True,),
                80,
                "DATA",
                "ROWS_FILEGROUP",
                ("PAGE", "ROW"),
            ),
        ),
        foreign_keys=(
            model.MssqlForeignKeyState(
                "fk",
                "outbound",
                "dbo",
                "t",
                ("café",),
                "ref",
                "r",
                ("id",),
                "CASCADE",
                "NO_ACTION",
                False,
                True,
                True,
            ),
        ),
        triggers=(model.MssqlTriggerState("tr", False, True, True, False, ("INSERT", "UPDATE"), "dbo", "SELECT 1"),),
        permissions=(model.MssqlPermissionState("reader", "DATABASE_ROLE", "SELECT", "GRANT", "café"),),
        behavior=model.MssqlTableBehaviorState.ordinary_disk_table(),
        default_filegroup="PRIMARY",
        available_row_filegroups=("PRIMARY", "DATA"),
    )


def _encoded(value: dict[str, object]) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def test_complete_original_catalog_evidence_and_class_identity() -> None:
    snapshot = _snapshot()
    # Frozen from the approved base before production edits; these bind every field.
    assert (
        hashlib.sha256(_encoded(snapshot.to_dict())).hexdigest()
        == "3426caa6301d9e74566a8665068c5ba5ee1d12dfb8ef9f3d979a9166b23637ed"
    )
    assert (
        hashlib.sha256(_encoded(snapshot.to_schema_dict())).hexdigest()
        == "7bec2195be3392cc718f28be063a6e3dd67282ad195c2b890e8473fbbf9692b4"
    )
    expectation = exact_schema_catalog_transition(snapshot, snapshot)
    assert expectation.representation == "exact_sys_catalog_v5"
    assert expectation.kind == "schema_columns"
    assert expectation.before_sha256.hex() == "7bec2195be3392cc718f28be063a6e3dd67282ad195c2b890e8473fbbf9692b4"
    assert expectation.after_sha256 == expectation.before_sha256
    rebuilt = replace(snapshot, indexes=(replace(snapshot.indexes[0], partition_compression=("NONE",)),))
    assert rebuilt.to_dict() != snapshot.to_dict()
    assert rebuilt.to_schema_dict() == snapshot.to_schema_dict()
    assert exact_schema_catalog_transition(snapshot, rebuilt) == expectation
    after = replace(
        snapshot,
        columns=(model.altered_catalog_column(snapshot.columns[0], ColumnDef("new_name", " FLOAT ", True)),),
    )
    assert (
        hashlib.sha256(_encoded(after.to_dict())).hexdigest()
        == "0e2b3bc05d1abb5639455a8a945def9e3d178124124cea770d2ad84dd8a6b818"
    )
    assert (
        hashlib.sha256(_encoded(after.to_schema_dict())).hexdigest()
        == "1e1429f47a20953df3221b794316c7c4c78e0645ebc912c07eda758d05cc1927"
    )
    transition = exact_schema_catalog_transition(snapshot, after)
    assert transition.before_sha256 == expectation.before_sha256
    assert transition.after_sha256.hex() == "1e1429f47a20953df3221b794316c7c4c78e0645ebc912c07eda758d05cc1927"
    for name in (
        "MssqlCatalogColumnState",
        "MssqlCheckConstraintState",
        "MssqlIndexState",
        "MssqlForeignKeyState",
        "MssqlTriggerState",
        "MssqlPermissionState",
        "MssqlTableBehaviorState",
        "MssqlSchemaCatalogSnapshot",
    ):
        cls = getattr(model, name)
        assert cls.__module__ == "dpone.runtime.sinks.mssql_target_catalog_model"
        assert cls.__qualname__ == name
        assert name in model.__all__
