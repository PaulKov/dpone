"""Column-level SQL Server catalog contracts for external state tables."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any


@dataclass(frozen=True, slots=True)
class MssqlColumnShape:
    """Semantic and safety-critical SQL Server catalog shape for one column."""

    name: str
    type_name: str
    max_length: int | None
    precision: int | None
    scale: int | None
    nullable: bool
    identity: bool | None = None
    is_computed: bool | None = None
    is_sparse: bool | None = None
    is_rowguidcol: bool | None = None
    generated_always_type: int | None = None
    is_hidden: bool | None = None
    is_masked: bool | None = None
    is_encrypted: bool | None = None
    is_ansi_padded: bool | None = None
    is_filestream: bool | None = None
    is_column_set: bool | None = None
    uses_database_default_collation: bool | None = None
    is_user_defined: bool | None = None
    is_assembly_type: bool | None = None
    has_bound_rule: bool | None = None
    has_bound_default: bool | None = None


@dataclass(frozen=True, slots=True)
class MssqlExternalTableContract:
    """Required columns and unique key shapes for one state object."""

    columns: frozenset[str]
    unique_indexes: tuple[tuple[str, ...], ...]
    shapes: tuple[MssqlColumnShape, ...] = ()
    exact_columns: bool = False


def exact_external_table_contract(
    *,
    columns: frozenset[str],
    unique_indexes: tuple[tuple[str, ...], ...],
    shapes: tuple[MssqlColumnShape, ...],
    identity_columns: frozenset[str] = frozenset(),
) -> MssqlExternalTableContract:
    """Build a fully shaped exact contract with safe catalog metadata.

    Additive compatibility contracts continue to instantiate
    :class:`MssqlExternalTableContract` directly and retain optional metadata.
    Exact contracts require complete shape coverage, make IDENTITY presence and
    absence explicit, and reject writable/authority-altering column features.
    """

    shape_names = tuple(shape.name for shape in shapes)
    if len(shape_names) != len(set(shape_names)):
        raise ValueError("mssql_explicit_identity_duplicate_shape")
    shape_columns = frozenset(shape_names)
    if shape_columns != columns:
        missing = ",".join(sorted(columns - shape_columns))
        unexpected = ",".join(sorted(shape_columns - columns))
        raise ValueError(f"mssql_explicit_identity_shape_coverage:missing={missing}:unexpected={unexpected}")
    unknown_identity_columns = identity_columns - columns
    if unknown_identity_columns:
        raise ValueError(f"mssql_explicit_identity_unknown_columns:{','.join(sorted(unknown_identity_columns))}")
    for shape in shapes:
        expected_identity = shape.name in identity_columns
        if shape.identity is not None and shape.identity != expected_identity:
            raise ValueError(f"mssql_explicit_identity_conflict:{shape.name}")
    return MssqlExternalTableContract(
        columns=columns,
        unique_indexes=unique_indexes,
        shapes=tuple(_exact_column_shape(shape, identity_columns=identity_columns) for shape in shapes),
        exact_columns=True,
    )


def matches_mssql_column_shape(row: Any, shape: MssqlColumnShape) -> bool:
    """Return whether one catalog row satisfies its declared column shape."""

    if str(row.get("type_name", "")).lower() != shape.type_name:
        return False
    if shape.max_length is not None and int(row.get("max_length", -2)) != shape.max_length:
        return False
    if shape.precision is not None and int(row.get("precision", -1)) != shape.precision:
        return False
    if shape.scale is not None and int(row.get("scale", -1)) != shape.scale:
        return False
    if shape.identity is not None and not _matches_bool(row, "is_identity", shape.identity):
        return False
    for field in (
        "is_computed",
        "is_sparse",
        "is_rowguidcol",
        "is_hidden",
        "is_masked",
        "is_encrypted",
        "is_ansi_padded",
        "is_filestream",
        "is_column_set",
        "uses_database_default_collation",
        "is_user_defined",
        "is_assembly_type",
        "has_bound_rule",
        "has_bound_default",
    ):
        if not _matches_bool(row, field, getattr(shape, field)):
            return False
    if shape.generated_always_type is not None and (
        "generated_always_type" not in row or int(row.get("generated_always_type") or 0) != shape.generated_always_type
    ):
        return False
    return _matches_bool(row, "is_nullable", shape.nullable)


def _exact_column_shape(
    shape: MssqlColumnShape,
    *,
    identity_columns: frozenset[str],
) -> MssqlColumnShape:
    return replace(
        shape,
        identity=shape.name in identity_columns,
        is_computed=False,
        is_sparse=False,
        is_rowguidcol=False,
        generated_always_type=0,
        is_hidden=False,
        is_masked=False,
        is_encrypted=False,
        is_ansi_padded=shape.type_name in {"binary", "char", "nchar", "nvarchar", "varbinary", "varchar"},
        is_filestream=False,
        is_column_set=False,
        uses_database_default_collation=True,
        is_user_defined=False,
        is_assembly_type=False,
        has_bound_rule=False,
        has_bound_default=False,
    )


def _matches_bool(row: Any, field: str, expected: bool | None) -> bool:
    return expected is None or (field in row and bool(row.get(field)) == expected)


__all__ = [
    "MssqlColumnShape",
    "MssqlExternalTableContract",
    "exact_external_table_contract",
    "matches_mssql_column_shape",
]
