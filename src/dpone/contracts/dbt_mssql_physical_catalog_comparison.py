"""Exact SQL2022 physical observations; no plan-membership or execution authority.

This closed comparison consumes all nine decoded catalog rowsets. Its caller
owns authenticated visibility, resource limits, transaction locking, database
identity and repeated-header stability. Equality here never proves those facts,
COUNT_BIG provenance, absence, row reconciliation, or enrollment permission.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import astuple, replace
from typing import TypeVar, cast

from dpone.contracts.dbt_mssql_physical import PhysicalModelPlan
from dpone.contracts.dbt_mssql_physical_catalog_rows import (
    CatalogRow,
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
from dpone.contracts.dbt_mssql_physical_catalog_wire import CatalogWireError, decode_catalog_result

_TYPES = {
    "HEADER": HeaderRow,
    "TABLE": TableRow,
    "COLUMN": ColumnRow,
    "INDEX": IndexRow,
    "INDEX_COLUMN": IndexColumnRow,
    "PARTITION": PartitionRow,
    "DEPENDENCY": DependencyRow,
    "FORBIDDEN_PROPERTY": ForbiddenPropertyRow,
    "COUNT": CountRow,
}
# Native sys.types identity, maximum byte length, precision, scale. These are
# physical catalog dimensions, not permissive logical widening conversions.
_FIXED = {
    "bigint": (127, 8, 19, 0),
    "bit": (104, 1, 1, 0),
    "int": (56, 4, 10, 0),
    "smallint": (52, 2, 5, 0),
    "tinyint": (48, 1, 3, 0),
    "date": (40, 3, 10, 0),
    "datetime": (61, 8, 23, 3),
    "smalldatetime": (58, 4, 16, 0),
    "money": (60, 8, 19, 4),
    "smallmoney": (122, 4, 10, 4),
    "real": (59, 4, 24, 0),
    "float": (62, 8, 53, 0),
    "uniqueidentifier": (36, 16, 0, 0),
}
_SIZED = {"char": 175, "varchar": 167, "nchar": 239, "nvarchar": 231, "binary": 173, "varbinary": 165}
_T = TypeVar("_T", bound=CatalogRow)


class CatalogComparisonError(ValueError):
    """An incomplete, unsupported or unequal catalog; messages omit source data."""


def _require(condition: bool, field: str) -> None:
    if not condition:
        raise CatalogComparisonError(f"catalog mismatch: {field}")


def _one(rows: Mapping[str, tuple[CatalogRow, ...]], kind: str, cls: type[_T]) -> _T:
    _require(len(rows[kind]) == 1, kind)
    return cast(_T, rows[kind][0])


def _validate_wire(results: Mapping[str, tuple[CatalogRow, ...]]) -> HeaderRow:
    _require(set(results) == set(_TYPES), "result kinds")
    _require(type(results["HEADER"]) is tuple and len(results["HEADER"]) == 1, "HEADER")
    header = results["HEADER"][0]
    _require(type(header) is HeaderRow, "HEADER type")
    for kind, cls in _TYPES.items():
        rows = results[kind]
        _require(type(rows) is tuple and all(type(row) is cls for row in rows), kind)
        if not rows:
            _require(kind not in {"HEADER", "TABLE", "COUNT"}, kind)
            continue
        try:
            # Representation validation only: authenticated acquisition budgets
            # were already enforced upstream. SQL integer maxima are not a new
            # allocation allowance and this function performs no I/O.
            decode_catalog_result(
                tuple(astuple(row) for row in rows),
                expected_kind=kind,
                expected_object_id=header.object_id,
                max_rows=2147483647,
                max_definition_bytes=2147483647,
            )
        except CatalogWireError as exc:
            raise CatalogComparisonError(f"catalog wire invalid: {kind}") from exc
    return cast(HeaderRow, header)


def _dimensions(dtype: str) -> tuple[str, int, int, int, int]:
    base, _, args = dtype.partition("(")
    if base in _FIXED and not args:
        return (base, *_FIXED[base])
    values = args.removesuffix(")").split(",")
    _require("max" not in values, "MAX type unsupported")
    if base in _SIZED:
        length = int(values[0]) * (2 if base in {"nchar", "nvarchar"} else 1)
        return base, _SIZED[base], length, 0, 0
    if base in {"decimal", "numeric"}:
        precision, scale = map(int, values)
        length = next(size for upper, size in ((9, 5), (19, 9), (28, 13), (38, 17)) if precision <= upper)
        return base, 106 if base == "decimal" else 108, length, precision, scale
    if base == "float":
        return ("real", 59, 4, 24, 0) if int(values[0]) <= 24 else ("float", 62, 8, 53, 0)
    if base in {"time", "datetime2", "datetimeoffset"}:
        scale = int(values[0])
        type_id, length, precision = {"time": (41, 5, 8), "datetime2": (42, 8, 19), "datetimeoffset": (43, 10, 26)}[
            base
        ]
        return base, type_id, length, precision + (scale + 1 if scale else 0), scale
    raise CatalogComparisonError("catalog mismatch: unsupported physical type")


def _columns(rows: tuple[CatalogRow, ...], plan: PhysicalModelPlan) -> None:
    _require(len(rows) == len(plan.spec.columns), "column count")
    _require(len({column.name for column in plan.spec.columns}) == len(rows), "duplicate columns")
    for ordinal, (raw, expected) in enumerate(zip(rows, plan.spec.columns, strict=True), 1):
        row = cast(ColumnRow, raw)
        name, type_id, length, precision, scale = _dimensions(expected.dtype)
        _require(
            (
                row.column_id,
                row.name,
                row.type_schema,
                row.type_name,
                row.system_type_id,
                row.user_type_id,
                row.max_length,
                row.precision,
                row.scale,
                row.is_nullable,
                row.collation_name,
            )
            == (
                ordinal,
                expected.name,
                "sys",
                name,
                type_id,
                type_id,
                length,
                precision,
                scale,
                expected.nullable,
                expected.collation,
            ),
            "column identity/type",
        )
        _require(row.is_ansi_padded == (name in _SIZED), "ANSI padding")
        _require(
            not any(
                (
                    row.is_identity,
                    row.is_computed,
                    row.is_sparse,
                    row.is_column_set,
                    row.is_hidden,
                    row.generated_always_type,
                    row.is_masked,
                    row.default_object_id,
                    row.rule_object_id,
                )
            )
            and row.encryption_type is None,
            "column properties",
        )


def _layout(results: Mapping[str, tuple[CatalogRow, ...]], plan: PhysicalModelPlan) -> None:
    cci = plan.spec.layout == "columnstore"
    index = _one(results, "INDEX", IndexRow)
    _require(
        (index.index_id, index.name, index.type, index.type_desc)
        == ((1, plan.columnstore_index_name, 5, "CLUSTERED COLUMNSTORE") if cci else (0, None, 0, "HEAP")),
        "index layout",
    )
    _require(
        not any(
            (
                index.is_unique,
                index.is_primary_key,
                index.is_unique_constraint,
                index.is_disabled,
                index.is_hypothetical,
                index.has_filter,
            )
        )
        and index.filter_definition is None,
        "index properties",
    )
    partition = _one(results, "PARTITION", PartitionRow)
    compression = {
        "rowstore_none": (0, "NONE"),
        "rowstore_row": (1, "ROW"),
        "rowstore_page": (2, "PAGE"),
        "columnstore": (3, "COLUMNSTORE"),
    }[plan.spec.layout]
    _require(
        (partition.index_id, partition.partition_number, partition.data_compression, partition.data_compression_desc)
        == (index.index_id, 1, *compression),
        "partition layout",
    )
    _require(partition.partition_id > 0 and partition.hobt_id >= 0, "partition identity")
    for row in (index, partition):
        _require(
            (row.data_space_id, row.data_space_name, row.data_space_type)
            == (plan.spec.filegroup.data_space_id, plan.spec.filegroup.name, "FG"),
            "filegroup",
        )
    members = results["INDEX_COLUMN"]
    _require(len(members) == (len(plan.spec.columns) if cci else 0), "index membership count")
    for ordinal, raw in enumerate(members, 1):
        member = cast(IndexColumnRow, raw)
        _require(
            (
                member.index_id,
                member.index_column_id,
                member.column_id,
                member.key_ordinal,
                member.partition_ordinal,
                member.is_descending_key,
                member.is_included_column,
                member.column_store_order_ordinal,
            )
            == (1, ordinal, ordinal, 0, 0, False, True, 0),
            "CCI membership",
        )


def require_catalog_match(
    results: Mapping[str, tuple[CatalogRow, ...]],
    *,
    plan: PhysicalModelPlan,
    expected_object_name: str,
    expected_object_create_time: str,
) -> None:
    """Require an exact existing-table match or raise ``CatalogComparisonError``.

    Names, column order, collation and native type dimensions compare exactly.
    Only a single heap (NONE/ROW/PAGE) or ordinary CCI on one filegroup and one
    partition is supported. Dependencies require future authenticated closure
    admission; this first cell rejects every dependency and forbidden occurrence.
    Directly constructed catalog DTOs are revalidated against the wire decoder.
    Empty tuples mean decoded complete empty collections, whose SQL provenance
    is the acquisition caller's responsibility. A valid COUNT is retained by the
    caller but cannot be reconciled without an authoritative expected row count.
    Expected object name and creation token are explicit because predecessor,
    candidate and helper identities cannot be inferred from a model plan alone.
    """
    try:
        _require(type(plan) is PhysicalModelPlan, "plan type")
        rebuilt = replace(plan, spec=replace(plan.spec))
        _require(rebuilt == plan, "plan integrity")
        header = _validate_wire(results)
        _require(
            header.object_type == "U "
            and header.schema_name == plan.spec.relation.schema
            and header.object_name == expected_object_name
            and header.object_create_time == expected_object_create_time,
            "object identity",
        )
        table = _one(results, "TABLE", TableRow)
        for field in ("schema_name", "object_name", "object_type", "object_create_time", "object_modify_time"):
            _require(getattr(table, field) == getattr(header, field), "TABLE/HEADER identity")
        _require(
            table.schema_id > 0
            and not any(
                (
                    table.is_memory_optimized,
                    table.durability,
                    table.temporal_type,
                    table.is_filetable,
                    table.is_node,
                    table.is_edge,
                    table.ledger_type,
                    table.lob_data_space_id,
                )
            ),
            "table properties",
        )
        # SQL2022/Linux reports NULL for the absence of FILESTREAM placement.
        # Preserve that fact; a synthetic zero is not a qualified absence proof.
        _require(table.filestream_data_space_id is None, "FILESTREAM placement")
        for kind in ("COLUMN", "INDEX", "INDEX_COLUMN", "PARTITION", "DEPENDENCY", "FORBIDDEN_PROPERTY"):
            _require(getattr(header, kind.lower() + "_count") == len(results[kind]), "header collection count")
        _require(not results["DEPENDENCY"], "unadmitted dependency")
        _require(not results["FORBIDDEN_PROPERTY"], "forbidden property")
        _columns(results["COLUMN"], plan)
        _layout(results, plan)
    except (TypeError, ValueError, AttributeError, KeyError) as exc:
        if isinstance(exc, CatalogComparisonError):
            raise
        raise CatalogComparisonError("catalog comparison input invalid") from exc
