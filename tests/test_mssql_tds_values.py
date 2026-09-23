"""Exact, SDK-independent TDS scalar admission and conversion."""

from datetime import UTC, datetime

import pytest

from dpone.runtime.mssql_tds_decoder import convert_tds_row, validate_tds_layouts
from dpone.runtime.native_wire_mssql import _MssqlDateTime2, build_mssql_bcp_native_contract


def contract(dtype: str):
    return build_mssql_bcp_native_contract(schema=[("value", dtype)], query="synthetic")


@pytest.mark.parametrize("mode", ["rows", "arrow"])
@pytest.mark.parametrize("dtype", ["int", "float", "datetime2(7)", "nvarchar(10)", "varbinary(max)"])
def test_unsupported_layouts_fail_before_io(mode, dtype):
    with pytest.raises(ValueError, match="tds_unsupported_layout"):
        validate_tds_layouts(contract(dtype), input_mode=mode)


@pytest.mark.parametrize(
    "dtype,value",
    [
        ("bigint", True),
        ("bigint", 2**63),
        ("float(53)", 1),
        ("float(53)", float("nan")),
        ("nvarchar(max)", b"text"),
        ("nvarchar(max)", "\ud800"),
        ("datetime2(6)", datetime.now(UTC)),
        ("datetime2(6)", _MssqlDateTime2(datetime(2026, 1, 1), submicrosecond_100ns=1)),
    ],
)
def test_invalid_values_are_not_coerced(dtype, value):
    with pytest.raises(ValueError, match="tds_invalid_value"):
        convert_tds_row({"value": value}, contract(dtype), input_mode="rows")


@pytest.mark.parametrize(
    "dtype,value",
    [
        ("bigint", -(2**63)),
        ("bigint", 2**63 - 1),
        ("float(53)", -0.0),
        ("float(53)", 5e-324),
        ("nvarchar(max)", "\0NULL 😀 é"),
        ("datetime2(6)", datetime.max),
    ],
)
def test_rows_preserve_values(dtype, value):
    assert convert_tds_row({"value": value}, contract(dtype), input_mode="rows") == (value,)


@pytest.mark.parametrize("mode", ["rows", "arrow"])
def test_nullability_and_row_identity(mode):
    assert convert_tds_row({"value": None}, contract("bigint nullable"), input_mode=mode) == (None,)
    for row in ({"value": None}, {"other": 1}, {"value": 1, "extra": 2}):
        with pytest.raises(ValueError):
            convert_tds_row(row, contract("bigint"), input_mode=mode)


@pytest.mark.parametrize(
    "value,expected",
    [
        (datetime(1970, 1, 1), 0),
        (datetime(1969, 12, 31, 23, 59, 59, 999999), -1),
        (datetime.min, -62135596800000000),
        (datetime.max, 253402300799999999),
    ],
)
def test_arrow_epoch_uses_exact_integer_arithmetic(value, expected):
    assert convert_tds_row({"value": value}, contract("datetime2(6)"), input_mode="arrow") == (expected,)
