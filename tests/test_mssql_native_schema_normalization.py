"""Ordered physical schema equality preserves native admission and read ordering."""

from contextlib import nullcontext
from types import SimpleNamespace

import pytest

from dpone.runtime.sinks import mssql_target_catalog_types as types
from dpone.runtime.sinks.mssql_native_import import MssqlNativeChunkImporter


@pytest.mark.parametrize(
    "declared,normalized",
    [
        (" INT ", "int"),
        ("decimal ( 038, 009 )", "decimal(38,9)"),
        ("NVARCHAR ( 004000 )", "nvarchar(4000)"),
        ("nvarchar(MAX)", "nvarchar(max)"),
        ("time", "time(7)"),
        ("datetime2", "datetime2(7)"),
        ("datetimeoffset(0)", "datetimeoffset(0)"),
        ("float", "float"),
        ("float(53)", "float(53)"),
    ],
)
def test_ordered_normalization_preserves_names_nullability_and_defaults(declared, normalized):
    schema = (("Second", declared, True), ("first", "int", False))
    expected = (("Second", normalized, True), ("first", "int", False))
    assert types.normalize_mssql_physical_schema(iter(schema)) == expected
    assert types.mssql_physical_schema_matches(iter(schema), expected)


@pytest.mark.parametrize(
    "actual",
    [
        (("b", "int", False), ("a", "float", True)),
        (("A", "float", True), ("b", "int", False)),
        (("a", "float", False), ("b", "int", False)),
        (("a", "float(53)", True), ("b", "int", False)),
        (("a", "float", True),),
        (("a", "float", True), ("b", "int", False), ("b", "int", False)),
    ],
)
def test_schema_comparison_retains_ordinal_name_nullability_type_and_cardinality(actual):
    assert not types.mssql_physical_schema_matches(actual, (("a", "float", True), ("b", "int", False)))


@pytest.mark.parametrize(
    "dtype,message",
    [
        ("decimal(39,0)", "MSSQL decimal precision or scale is invalid"),
        ("decimal(3,4)", "MSSQL decimal precision or scale is invalid"),
        ("nvarchar(4001)", "MSSQL nvarchar length must be between 1 and 4000"),
        ("nvarchar(0)", "MSSQL nvarchar length must be between 1 and 4000"),
        ("float(54)", "MSSQL float precision must be between 1 and 53"),
        ("time(8)", "MSSQL temporal scale must be between 0 and 7"),
        ("int; DROP TABLE t", "unsupported MSSQL physical type: int; DROP TABLE t"),
    ],
)
def test_schema_errors_are_unchanged_even_after_an_earlier_mismatch(dtype, message):
    with pytest.raises(ValueError) as caught:
        types.mssql_physical_schema_matches((("changed", "int", False), ("b", dtype, True)), ())
    assert str(caught.value) == message


def _importer(columns, connector):
    return MssqlNativeChunkImporter(
        connector,
        database="db",
        schema="stage",
        columns=columns,
        encode_row=lambda row: b"",
        assert_lease=lambda lease: None,
        mutation_scope=lambda *args: nullcontext(),
        options_factory=lambda **kwargs: SimpleNamespace(**kwargs),
    )


@pytest.mark.parametrize(
    "dtype,message",
    [
        ("int NULLABLE", "unsupported MSSQL physical type: int NULLABLE"),
        ("decimal(39,0)", "MSSQL decimal precision or scale is invalid"),
        ("varchar(2)", "mssql_native.utf8_collation_authority_required"),
        ("char(2)", "mssql_native.utf8_collation_authority_required"),
    ],
)
def test_constructor_preserves_rejections(dtype, message):
    with pytest.raises(ValueError) as caught:
        _importer((SimpleNamespace(name="n", source_type=dtype, nullable=False),), object())
    assert str(caught.value) == message


@pytest.mark.parametrize(
    "actual,message",
    [
        ([("n", "float(53)", True)], "mssql_native.stage_schema_changed"),
        ([("renamed", "float", True)], "mssql_native.stage_schema_changed"),
        ([("n", "float", False)], "mssql_native.stage_schema_changed"),
        ([], "mssql_native.stage_schema_changed"),
        ([("n", "decimal(39,0)", True)], "MSSQL decimal precision or scale is invalid"),
    ],
)
def test_observed_schema_rejection_precedes_count_and_row_reads(actual, message):
    effects = []

    class Connector:
        def fetch_schema_columns(self, *args, **kwargs):
            effects.append("schema")
            return [SimpleNamespace(name=name, dtype=dtype, nullable=nullable) for name, dtype, nullable in actual]

        def get_records(self, *args, **kwargs):
            pytest.fail("count read before schema admission")

        def get_records_iterator(self, *args, **kwargs):
            pytest.fail("row read before schema admission")

    value = _importer((SimpleNamespace(name="n", source_type=" FLOAT  nullable", nullable=True),), Connector())
    with pytest.raises(ValueError) as caught:
        value._verify_contents("owned", 0, "unused")
    assert str(caught.value) == message
    assert effects == ["schema"]
