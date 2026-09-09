"""Neutral MSSQL physical projection for source identity and sink staging.

The module deliberately lives outside both runtime source and sink slices.
It lets extraction fingerprint the exact declared target contract without a
source-to-sink dependency, while the sink reuses the same resolution logic.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any

from dpone.contracts.incremental_snapshot import MSSQL_TEXT_KEY_COLLATION, is_mssql_text_key_type
from dpone.contracts.mssql_physical_design import (
    MssqlIndexKeyContractError,
    mssql_index_key_column_bytes,
    require_mssql_index_key_width,
)
from dpone.contracts.mssql_type_contract import normalize_mssql_physical_type
from dpone.runtime.incremental_snapshot import SnapshotReconciliationError
from dpone.runtime.support.mssql_lossless_projection import (
    require_lossless_target_projection,
    resolved_source_type,
)
from dpone.runtime.support.mssql_types import MSSQLTypeMapper

MSSQL_CLUSTERED_KEY_MAX_BYTES = 900
_CHARACTER_TYPE = re.compile(r"^(n?(?:var)?char)\((max|\d+)\)$", re.IGNORECASE)
_DECIMAL_TYPE = re.compile(r"^(?:decimal|numeric)\((\d+)(?:,(\d+))?\)$", re.IGNORECASE)
_FIXED_WIDTH_BYTES = {
    "bigint": 8,
    "bit": 1,
    "date": 3,
    "datetime": 8,
    "datetime2": 8,
    "datetimeoffset": 10,
    "float": 8,
    "int": 4,
    "money": 8,
    "real": 4,
    "smalldatetime": 4,
    "smallint": 2,
    "smallmoney": 4,
    "time": 5,
    "tinyint": 1,
    "uniqueidentifier": 16,
}


def business_schema(schema: Sequence[tuple[str, str]]) -> tuple[tuple[str, str], ...]:
    """Return published source columns, excluding dpone-managed values."""

    return tuple((str(name), str(dtype)) for name, dtype in schema if not str(name).lower().startswith("__dpone__"))


def business_columns(schema: Sequence[tuple[str, str]]) -> list[str]:
    """Return only published business column names."""

    return [name for name, _dtype in business_schema(schema)]


def explicit_mssql_column_types(options: Any) -> dict[str, str]:
    """Resolve per-column physical MSSQL overrides from the public design."""

    if not isinstance(options, Mapping):
        return {}
    physical = options.get("physical_design")
    columns = physical.get("columns") if isinstance(physical, Mapping) else None
    if not isinstance(columns, Mapping):
        return {}
    output: dict[str, str] = {}
    for column, raw in columns.items():
        target_type = raw.get("target_type") if isinstance(raw, Mapping) else None
        value = target_type.get("mssql") if isinstance(target_type, Mapping) else None
        if value:
            output[str(column)] = str(value)
    return output


def resolved_business_nullability(
    load_config: Any,
    schema: Sequence[tuple[str, str]],
    unique_key: Sequence[str],
) -> dict[str, bool]:
    """Resolve required target nullability from the typed schema contract."""

    options = getattr(load_config, "options", {}) or {}
    contract = options.get("schema_contract") if isinstance(options, Mapping) else None
    columns = contract.get("columns") if isinstance(contract, Mapping) else None
    values = columns if isinstance(columns, Mapping) else {}
    keys = {str(key).lower() for key in unique_key}
    output: dict[str, bool] = {}
    for column, _dtype in business_schema(schema):
        raw = values.get(column)
        nullable = raw.get("nullable") if isinstance(raw, Mapping) else None
        if not isinstance(nullable, bool):
            raise SnapshotReconciliationError("mssql_snapshot_reconciliation.business_nullability_contract_required")
        if column.lower() in keys and nullable:
            raise SnapshotReconciliationError("mssql_snapshot_reconciliation.unique_key_must_be_not_null")
        output[column] = nullable
    return output


def resolved_target_type(load_config: Any, column: str, source_type: str) -> str:
    """Return the one physical type used by target and normalized staging."""

    try:
        source_target = normalize_mssql_physical_type(resolved_source_type(source_type))
    except ValueError as exc:
        raise SnapshotReconciliationError(
            f"mssql_snapshot_reconciliation.source_physical_type_invalid:{column}"
        ) from exc
    override = explicit_mssql_column_types(getattr(load_config, "options", None)).get(column)
    if override:
        try:
            resolved = normalize_mssql_physical_type(override)
        except ValueError as exc:
            raise SnapshotReconciliationError(
                f"mssql_snapshot_reconciliation.target_physical_type_invalid:{column}"
            ) from exc
        require_lossless_target_projection(source_target, resolved, column=column)
        return resolved
    return source_target


def resolved_mssql_target_column_type(load_config: Any, column: str, source_type: str) -> str:
    """Return the catalog-facing MSSQL type for one authored source column.

    Explicit physical types pass through the native-staging losslessness gate.
    Columns without an override retain the established source-to-MSSQL mapper;
    this is the same rule used by fresh-target preplanning and schema evolution.
    """

    explicit = explicit_mssql_column_types(getattr(load_config, "options", None))
    if column in explicit:
        return resolved_target_type(load_config, column, source_type)
    return MSSQLTypeMapper.to_mssql(source_type)


def require_indexable_key_types(
    schema: Sequence[tuple[str, str]],
    load_config: Any,
    unique_key: Sequence[str],
) -> None:
    """Reject unbounded/oversized native keys before any source export."""

    by_name = {str(name): str(dtype) for name, dtype in schema}
    for key in unique_key:
        dtype = by_name.get(str(key))
        if dtype is None:
            raise SnapshotReconciliationError("mssql_snapshot_reconciliation.unique_key_missing_from_schema")
        if "(max)" in resolved_target_type(load_config, str(key), dtype).lower():
            raise SnapshotReconciliationError("mssql_snapshot_reconciliation.unique_key_indexable_type_required")
    require_clustered_key_width(
        {str(key): resolved_target_type(load_config, str(key), by_name[str(key)]) for key in unique_key}
    )


def resolved_text_key_columns(
    schema: Sequence[tuple[str, str]],
    load_config: Any,
    unique_key: Sequence[str],
) -> tuple[str, ...]:
    """Return ordered keys whose native SQL Server identity is collated text."""

    by_name = {str(name): str(dtype) for name, dtype in schema}
    return tuple(
        key
        for key in (str(value) for value in unique_key)
        if key in by_name and is_text_key_type(resolved_target_type(load_config, key, by_name[key]))
    )


def is_text_key_type(dtype: str) -> bool:
    """Return whether a resolved MSSQL type participates in collation."""

    return is_mssql_text_key_type(dtype)


def key_column_definition(dtype: str, *, is_text_key: bool) -> str:
    """Attach the canonical binary collation to a text-key definition."""

    if not is_text_key:
        return dtype
    if not is_text_key_type(dtype):
        raise SnapshotReconciliationError("mssql_snapshot_reconciliation.text_key_type_invalid")
    return f"{dtype} COLLATE {MSSQL_TEXT_KEY_COLLATION}"


def require_clustered_key_width(resolved_key_types: Mapping[str, str]) -> int:
    """Prove the complete native key fits SQL Server's 900-byte gate."""

    try:
        total = require_mssql_index_key_width(
            resolved_key_types,
            tuple(resolved_key_types),
            kind="clustered",
        )
    except MssqlIndexKeyContractError as exc:
        if exc.code == "mssql.index_key.unbounded_type":
            raise SnapshotReconciliationError(
                "mssql_snapshot_reconciliation.unique_key_indexable_type_required"
            ) from exc
        if exc.code.startswith("mssql.index_key.width_exceeded"):
            raise SnapshotReconciliationError(
                "mssql_snapshot_reconciliation.clustered_unique_key_exceeds_900_bytes"
            ) from exc
        raise SnapshotReconciliationError("mssql_snapshot_reconciliation.unique_key_width_unverifiable") from exc
    if total > MSSQL_CLUSTERED_KEY_MAX_BYTES:
        raise SnapshotReconciliationError("mssql_snapshot_reconciliation.clustered_unique_key_exceeds_900_bytes")
    return total


def validate_text_key_metadata(
    unique_key: Sequence[str],
    expected_types: Mapping[str, str],
    metadata: Mapping[str, Mapping[str, Any]],
) -> None:
    """Require exact target collation metadata for every collated key."""

    for key in (str(value) for value in unique_key):
        expected = expected_types.get(key)
        if expected is None or not is_text_key_type(expected):
            continue
        actual = metadata.get(key.lower()) or {}
        if str(actual.get("collation_name") or "").casefold() != MSSQL_TEXT_KEY_COLLATION.casefold():
            raise SnapshotReconciliationError("mssql_snapshot_reconciliation.text_key_binary_collation_required")


def _maximum_key_bytes(dtype: str) -> int:
    try:
        return mssql_index_key_column_bytes(dtype)
    except MssqlIndexKeyContractError as exc:
        if exc.code == "mssql.index_key.unbounded_type":
            raise SnapshotReconciliationError(
                "mssql_snapshot_reconciliation.unique_key_indexable_type_required"
            ) from exc
        raise SnapshotReconciliationError("mssql_snapshot_reconciliation.unique_key_width_unverifiable") from exc


def _normalize_key_type(dtype: str) -> str:
    value = re.sub(r"\s+", "", str(dtype).strip().lower())
    if value.startswith("float("):
        return "float"
    for base in ("datetime2", "datetimeoffset", "time"):
        if value.startswith(f"{base}("):
            return base
    return value


__all__ = [
    "business_columns",
    "business_schema",
    "explicit_mssql_column_types",
    "is_text_key_type",
    "key_column_definition",
    "MSSQL_CLUSTERED_KEY_MAX_BYTES",
    "MSSQL_TEXT_KEY_COLLATION",
    "require_clustered_key_width",
    "require_indexable_key_types",
    "require_lossless_target_projection",
    "resolved_business_nullability",
    "resolved_mssql_target_column_type",
    "resolved_source_type",
    "resolved_target_type",
    "resolved_text_key_columns",
    "validate_text_key_metadata",
]
