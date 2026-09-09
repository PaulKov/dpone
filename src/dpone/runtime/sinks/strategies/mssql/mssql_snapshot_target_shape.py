"""Exact business, lineage, and soft-delete target-shape validation."""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from dpone.contracts.soft_delete import SoftDeleteMode, SoftDeletePolicy


class MssqlTargetShapeError(ValueError):
    """Pure target-shape violation translated by the reconciliation service."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def normalize_type(value: Any) -> str:
    normalized = re.sub(r"\s+nullable$", "", str(value).strip().lower())
    return re.sub(r"\s+", "", normalized)


def validate_business_shape(
    expected_schema: Sequence[tuple[str, str, bool]],
    metadata: dict[str, dict[str, Any]],
) -> None:
    expected_names = {column.lower() for column, _dtype, _nullable in expected_schema}
    if set(metadata) != expected_names:
        raise MssqlTargetShapeError("mssql_snapshot_reconciliation.target_business_shape_invalid")
    for column, target_type, nullable in expected_schema:
        actual = metadata[column.lower()]
        if not metadata_matches_type(actual, target_type):
            raise MssqlTargetShapeError("mssql_snapshot_reconciliation.target_business_shape_invalid")
        if (
            bool(actual.get("is_nullable")) != nullable
            or bool(actual.get("is_computed"))
            or bool(actual.get("is_identity"))
            or bool(actual.get("default_object_id"))
        ):
            raise MssqlTargetShapeError("mssql_snapshot_reconciliation.target_business_shape_invalid")


def metadata_matches_type(metadata: dict[str, Any], expected: str) -> bool:
    normalized = normalize_type(expected)
    match = re.fullmatch(r"([a-z0-9]+)(?:\((max|\d+)(?:,(\d+))?\))?", normalized)
    if match is None or str(metadata.get("type_name", "")).lower() != match.group(1):
        return False
    base, first, second = match.groups()
    shape = _catalog_shape(base, first, second)
    if shape is None:
        return False
    return all(
        int(metadata.get(field, -2)) == value
        for field, value in (
            ("max_length", shape.max_length),
            ("precision", shape.precision),
            ("scale", shape.scale),
        )
    )


@dataclass(frozen=True)
class _CatalogShape:
    """Exact ``sys.columns`` shape for one built-in SQL Server type."""

    max_length: int
    precision: int
    scale: int


_FIXED_CATALOG_SHAPES = {
    "bigint": _CatalogShape(8, 19, 0),
    "bit": _CatalogShape(1, 1, 0),
    "date": _CatalogShape(3, 10, 0),
    "datetime": _CatalogShape(8, 23, 3),
    "int": _CatalogShape(4, 10, 0),
    "money": _CatalogShape(8, 19, 4),
    "real": _CatalogShape(4, 24, 0),
    "smalldatetime": _CatalogShape(4, 16, 0),
    "smallint": _CatalogShape(2, 5, 0),
    "smallmoney": _CatalogShape(4, 10, 4),
    "tinyint": _CatalogShape(1, 3, 0),
    "uniqueidentifier": _CatalogShape(16, 0, 0),
}


def _catalog_shape(base: str, first: str | None, second: str | None) -> _CatalogShape | None:
    """Resolve the canonical shape exposed by ``sys.columns``.

    SQL Server stores character lengths in bytes, canonicalizes ``float(n)``
    to either 24 or 53 bits, and exposes display precision plus fractional
    scale for the variable-precision temporal families.  Checking the complete
    shape prevents a lossy or incorrectly provisioned external target from
    passing preflight merely because its base type and scale happen to match.
    """

    if base in {"nvarchar", "nchar", "varchar", "char", "varbinary", "binary"}:
        return _character_shape(base, first, second)
    if base in {"decimal", "numeric"}:
        return _decimal_shape(first, second)
    if base in {"datetime2", "datetimeoffset", "time"}:
        return _temporal_shape(base, first, second)
    if base == "float":
        return _float_shape(first, second)
    if first is not None or second is not None:
        return None
    return _FIXED_CATALOG_SHAPES.get(base)


def _character_shape(base: str, first: str | None, second: str | None) -> _CatalogShape | None:
    if second is not None:
        return None
    length = first or "1"
    if length == "max":
        if base not in {"nvarchar", "varchar", "varbinary"}:
            return None
        return _CatalogShape(-1, 0, 0)
    declared_length = int(length)
    maximum = 4000 if base in {"nvarchar", "nchar"} else 8000
    if not 1 <= declared_length <= maximum:
        return None
    max_length = declared_length * 2 if base in {"nvarchar", "nchar"} else declared_length
    return _CatalogShape(max_length, 0, 0)


def _decimal_shape(first: str | None, second: str | None) -> _CatalogShape | None:
    precision = int(first or 18)
    scale = int(second or 0)
    if not 1 <= precision <= 38 or not 0 <= scale <= precision:
        return None
    max_length = 5 if precision <= 9 else 9 if precision <= 19 else 13 if precision <= 28 else 17
    return _CatalogShape(max_length, precision, scale)


def _temporal_shape(base: str, first: str | None, second: str | None) -> _CatalogShape | None:
    if first == "max" or second is not None:
        return None
    scale = int(first or 7)
    if not 0 <= scale <= 7:
        return None
    variable_bytes = 0 if scale <= 2 else 1 if scale <= 4 else 2
    base_length = {"datetime2": 6, "datetimeoffset": 8, "time": 3}[base]
    base_precision = {"datetime2": 19, "datetimeoffset": 26, "time": 8}[base]
    precision = base_precision if scale == 0 else base_precision + scale + 1
    return _CatalogShape(base_length + variable_bytes, precision, scale)


def _float_shape(first: str | None, second: str | None) -> _CatalogShape | None:
    if first == "max" or second is not None:
        return None
    declared_precision = int(first or 53)
    if not 1 <= declared_precision <= 53:
        return None
    precision = 24 if declared_precision <= 24 else 53
    return _CatalogShape(4 if precision == 24 else 8, precision, 0)


def validate_soft_delete_shape(
    policy: SoftDeletePolicy,
    metadata: dict[str, dict[str, Any]],
) -> None:
    timestamp = metadata.get("__dpone__deleted_at")
    flag = metadata.get("__dpone__is_deleted")
    if policy.timestamp_is_source_of_truth and not _is_timestamp_column(timestamp):
        raise MssqlTargetShapeError("mssql_snapshot_reconciliation.deleted_at_shape_invalid")
    if policy.mode == SoftDeleteMode.TIMESTAMP_ONLY:
        if flag is not None:
            raise MssqlTargetShapeError("mssql_snapshot_reconciliation.unexpected_delete_flag")
        return
    if policy.mode == SoftDeleteMode.TIMESTAMP_AND_FLAG:
        definition = _compact_sql((flag or {}).get("computed_definition")).replace("(", "").replace(")", "")
        canonical = "convertbit,casewhen__dpone__deleted_atisnullthen0else1end"
        if (
            flag is None
            or str(flag.get("type_name", "")).lower() != "bit"
            or not bool(flag.get("is_computed"))
            or not bool(flag.get("is_persisted"))
            or definition != canonical
        ):
            raise MssqlTargetShapeError("mssql_snapshot_reconciliation.delete_flag_computed_shape_invalid")
        return
    if timestamp is not None:
        raise MssqlTargetShapeError("mssql_snapshot_reconciliation.unexpected_delete_timestamp")
    default = _compact_sql((flag or {}).get("default_definition")).replace("(", "").replace(")", "")
    if (
        flag is None
        or str(flag.get("type_name", "")).lower() != "bit"
        or bool(flag.get("is_nullable"))
        or bool(flag.get("is_computed"))
        or default != "0"
    ):
        raise MssqlTargetShapeError("mssql_snapshot_reconciliation.delete_flag_shape_invalid")


def validate_required_technical_shape(metadata: dict[str, dict[str, Any]]) -> None:
    fixed_char_lengths = {
        "__dpone__run_id": 26,
        "__dpone__load_id": 26,
        "__dpone__row_hash": 64,
    }
    valid = all(
        _is_nonnull_fixed_char(metadata.get(column), length) for column, length in fixed_char_lengths.items()
    ) and all(_is_nonnull_datetime2(metadata.get(column)) for column in ("__dpone__extracted_at", "__dpone__loaded_at"))
    if not valid:
        raise MssqlTargetShapeError("mssql_snapshot_reconciliation.target_technical_shape_invalid")


def _is_nonnull_fixed_char(metadata: dict[str, Any] | None, length: int) -> bool:
    return bool(
        metadata
        and str(metadata.get("type_name", "")).lower() == "char"
        and int(metadata.get("max_length", -1)) == length
        and not bool(metadata.get("is_nullable"))
        and not bool(metadata.get("is_computed"))
    )


def _is_nonnull_datetime2(metadata: dict[str, Any] | None) -> bool:
    return bool(
        metadata
        and str(metadata.get("type_name", "")).lower() == "datetime2"
        and int(metadata.get("scale", -1)) == 7
        and not bool(metadata.get("is_nullable"))
        and not bool(metadata.get("is_computed"))
    )


def _is_timestamp_column(metadata: dict[str, Any] | None) -> bool:
    return bool(
        metadata
        and str(metadata.get("type_name", "")).lower() == "datetime2"
        and int(metadata.get("scale", -1)) == 7
        and bool(metadata.get("is_nullable"))
        and not bool(metadata.get("is_computed"))
    )


def _compact_sql(value: Any) -> str:
    return re.sub(r"[\[\]\s]", "", str(value or "").lower())


__all__ = [
    "MssqlTargetShapeError",
    "metadata_matches_type",
    "normalize_type",
    "validate_business_shape",
    "validate_required_technical_shape",
    "validate_soft_delete_shape",
]
