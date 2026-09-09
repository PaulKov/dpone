"""Exact SQL Server physical-domain compatibility authority."""

from __future__ import annotations

import re

from dpone.contracts.mssql_type_contract import (
    normalize_mssql_physical_type,
    normalized_mssql_base_type,
)

_INTEGER_RANK = {"tinyint": 1, "smallint": 2, "int": 3, "bigint": 4}
_INTEGER_DIGITS = {"tinyint": 3, "smallint": 5, "int": 10, "bigint": 19}


class MssqlLosslessProjectionError(ValueError):
    """A physical conversion cannot preserve the complete source domain."""

    def __init__(self, column: str, source_type: str, target_type: str) -> None:
        self.column = column
        self.source_type = source_type
        self.target_type = target_type
        super().__init__(f"mssql.lossless_projection:{column}:{source_type}->{target_type}")


def mssql_physical_types_equal(left: str, right: str) -> bool:
    """Compare exact SQL Server storage semantics, including scale and family."""

    try:
        return _comparison_shape(left) == _comparison_shape(right)
    except ValueError:
        return False


def mssql_is_safe_widening(desired_type: str, existing_type: str) -> bool:
    """Return whether ALTER from ``existing`` to ``desired`` preserves its domain."""

    if mssql_physical_types_equal(desired_type, existing_type):
        return False
    try:
        validate_mssql_lossless_projection(
            existing_type,
            desired_type,
            column="<schema-evolution>",
            allow_value_guarded=False,
        )
    except (MssqlLosslessProjectionError, ValueError):
        return False
    return True


def validate_mssql_lossless_projection(
    source_type: str,
    target_type: str,
    *,
    column: str,
    allow_value_guarded: bool = False,
) -> None:
    """Prove that every source value has an exact target representation."""

    source = normalize_mssql_physical_type(source_type)
    target = normalize_mssql_physical_type(target_type)
    if _comparison_shape(source) == _comparison_shape(target):
        return
    source_base = normalized_mssql_base_type(source)
    target_base = normalized_mssql_base_type(target)
    if source_base in _INTEGER_RANK and target_base in _INTEGER_RANK:
        if _INTEGER_RANK[target_base] >= _INTEGER_RANK[source_base]:
            return
        _raise_lossy(column, source, target)
    if source_base in _INTEGER_RANK and (decimal := _decimal_shape(target)) is not None:
        precision, scale = decimal
        if precision - scale >= _INTEGER_DIGITS[source_base]:
            return
        _raise_lossy(column, source, target)
    source_decimal = _decimal_shape(source)
    target_decimal = _decimal_shape(target)
    if source_decimal is not None and target_decimal is not None:
        source_precision, source_scale = source_decimal
        target_precision, target_scale = target_decimal
        if target_scale >= source_scale and target_precision - target_scale >= source_precision - source_scale:
            return
        _raise_lossy(column, source, target)
    source_float = _float_precision(source)
    target_float = _float_precision(target)
    if source_float is not None and target_float is not None:
        if target_float >= source_float:
            return
        _raise_lossy(column, source, target)
    source_temporal = _temporal_shape(source)
    target_temporal = _temporal_shape(target)
    if source_temporal is not None and target_temporal is not None:
        source_family, source_scale = source_temporal
        target_family, target_scale = target_temporal
        if source_family == target_family and target_scale >= source_scale:
            return
        _raise_lossy(column, source, target)
    if (
        allow_value_guarded
        and source_base in {"char", "nchar", "varchar", "nvarchar"}
        and target_base
        in {
            "char",
            "nchar",
            "nvarchar",
            "varchar",
        }
    ):
        return
    if allow_value_guarded and source_base in {"binary", "varbinary"} and target_base in {"binary", "varbinary"}:
        return
    bounded_families = {"char", "nchar", "varchar", "nvarchar", "binary", "varbinary"}
    if source_base == target_base and source_base in bounded_families:
        source_length = _bounded_length(source)
        target_length = _bounded_length(target)
        if target_length is None or (source_length is not None and target_length >= source_length):
            return
    _raise_lossy(column, source, target)


def _comparison_shape(dtype: str) -> tuple[object, ...]:
    normalized = normalize_mssql_physical_type(dtype)
    decimal = _decimal_shape(normalized)
    if decimal is not None:
        return ("decimal", *decimal)
    floating = _float_precision(normalized)
    if floating is not None:
        return ("float", floating)
    temporal = _temporal_shape(normalized)
    if temporal is not None:
        return ("temporal", *temporal)
    return (normalized,)


def _decimal_shape(dtype: str) -> tuple[int, int] | None:
    match = re.fullmatch(r"(?:decimal|numeric)\((\d+),(\d+)\)", dtype)
    return (int(match.group(1)), int(match.group(2))) if match is not None else None


def _float_precision(dtype: str) -> int | None:
    if dtype == "real":
        return 24
    if dtype == "float":
        return 53
    match = re.fullmatch(r"float\((\d+)\)", dtype)
    if match is None:
        return None
    return 24 if int(match.group(1)) <= 24 else 53


def _temporal_shape(dtype: str) -> tuple[str, int] | None:
    if dtype in {"date", "datetime", "smalldatetime"}:
        return (dtype, 0)
    match = re.fullmatch(r"(datetime2|datetimeoffset|time)\((\d+)\)", dtype)
    return (match.group(1), int(match.group(2))) if match is not None else None


def _bounded_length(dtype: str) -> int | None:
    match = re.fullmatch(r"(?:nvarchar|varchar|nchar|char|varbinary|binary)\((max|\d+)\)", dtype)
    if match is None or match.group(1) == "max":
        return None
    return int(match.group(1))


def _raise_lossy(column: str, source_type: str, target_type: str) -> None:
    raise MssqlLosslessProjectionError(column, source_type, target_type)


__all__ = [
    "MssqlLosslessProjectionError",
    "mssql_is_safe_widening",
    "mssql_physical_types_equal",
    "validate_mssql_lossless_projection",
]
