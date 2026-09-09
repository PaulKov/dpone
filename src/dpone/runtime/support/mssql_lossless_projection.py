"""Lossless type projection for PostgreSQL→MSSQL native staging."""

from __future__ import annotations

import re

from dpone._compat import StrEnum
from dpone.contracts.mssql_lossless_type_contract import (
    MssqlLosslessProjectionError,
    validate_mssql_lossless_projection,
)
from dpone.runtime.incremental_snapshot import SnapshotReconciliationError
from dpone.runtime.support.mssql_types import MSSQLTypeMapper
from dpone.type_system.source_sink.postgres_mssql import PostgresMssqlTypeMapper

_MSSQL_SOURCE_BASES = frozenset(
    {
        "bigint",
        "binary",
        "bit",
        "char",
        "date",
        "datetime2",
        "datetimeoffset",
        "decimal",
        "float",
        "int",
        "money",
        "nchar",
        "numeric",
        "nvarchar",
        "real",
        "smallint",
        "smallmoney",
        "time",
        "tinyint",
        "uniqueidentifier",
        "varbinary",
        "varchar",
    }
)


class MssqlLosslessGuardMode(StrEnum):
    """Required runtime proof, or rejection, for one requested projection."""

    STRUCTURAL = "structural"
    VALUE_GUARDED = "value_guarded"
    FORBIDDEN = "forbidden"


def _normalize(dtype: str) -> str:
    return re.sub(r"\s+", "", str(dtype).strip().lower())


def resolved_source_type(source_type: str) -> str:
    """Return the exact MSSQL wire-native source type before overrides."""

    if MSSQLTypeMapper.is_clickhouse_type(source_type):
        return MSSQLTypeMapper.to_mssql(source_type)
    normalized = _normalize(source_type)
    base = normalized.split("(", 1)[0]
    if base in _MSSQL_SOURCE_BASES:
        return MSSQLTypeMapper.to_mssql(normalized)
    decision = PostgresMssqlTypeMapper().resolve(source_type)
    if not decision.requires_explicit_contract:
        return MSSQLTypeMapper.to_mssql(decision.target_type)
    return MSSQLTypeMapper.to_mssql(source_type)


def resolved_mssql_base_type(dtype: str) -> str:
    """Return the base of an already-resolved MSSQL physical type."""

    return _normalize(dtype).split("(", 1)[0]


def lossless_target_projection_guard_mode(
    source_type: str,
    target_type: str,
    *,
    column: str,
) -> MssqlLosslessGuardMode:
    """Classify whether structural or batch-value evidence proves safety.

    Structurally forbidden projections are explicit so callers can translate
    them to their own fail-closed error before staging or target mutation.
    """

    try:
        validate_mssql_lossless_projection(
            source_type,
            target_type,
            column=column,
            allow_value_guarded=False,
        )
    except MssqlLosslessProjectionError:
        try:
            validate_mssql_lossless_projection(
                source_type,
                target_type,
                column=column,
                allow_value_guarded=True,
            )
        except MssqlLosslessProjectionError:
            return MssqlLosslessGuardMode.FORBIDDEN
        return MssqlLosslessGuardMode.VALUE_GUARDED
    return MssqlLosslessGuardMode.STRUCTURAL


def require_lossless_target_projection(source_type: str, target_type: str, *, column: str) -> None:
    """Reject target declarations that can round or reinterpret source values.

    Bounded text and binary remain value-dependent: native staging proves
    their byte/code-point round-trip for every row before target mutation.
    """

    try:
        validate_lossless_target_projection(source_type, target_type, column=column)
    except MssqlLosslessProjectionError as exc:
        raise SnapshotReconciliationError(
            "mssql_snapshot_reconciliation.lossy_target_type_forbidden:"
            f"{exc.column}:{exc.source_type}->{exc.target_type}"
        ) from exc


def validate_lossless_target_projection(source_type: str, target_type: str, *, column: str) -> None:
    """Validate projection without coupling callers to snapshot reconciliation."""

    validate_mssql_lossless_projection(
        source_type,
        target_type,
        column=column,
        allow_value_guarded=True,
    )


__all__ = [
    "MssqlLosslessGuardMode",
    "MssqlLosslessProjectionError",
    "lossless_target_projection_guard_mode",
    "require_lossless_target_projection",
    "resolved_mssql_base_type",
    "resolved_source_type",
    "validate_lossless_target_projection",
]
