"""Lossless raw-wire to native SQL Server projection primitives.

PostgreSQL and the generic streaming sources reach SQL Server through a
character ``bcp`` wire.  The wire table must never perform the final type
conversion: bounded strings can grow when :class:`BulkTextCodec` escapes
control characters, while numeric and temporal conversions can silently
round.  This module is the one value-integrity authority shared by generic
load strategies and the XMin snapshot finalizer.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any

from dpone.runtime.artifact_models import StagingTableArtifact
from dpone.runtime.incremental_snapshot import SnapshotReconciliationError
from dpone.runtime.support.bulk_text_codec import is_bulk_text_type
from dpone.runtime.support.mssql_hex_binary import is_mssql_hex_binary_wire_type
from dpone.runtime.support.mssql_lossless_projection import (
    MssqlLosslessGuardMode,
    lossless_target_projection_guard_mode,
    resolved_mssql_base_type,
    resolved_source_type,
)
from dpone.runtime.support.mssql_snapshot_projection import (
    MSSQL_TEXT_KEY_COLLATION,
    is_text_key_type,
    resolved_mssql_target_column_type,
    resolved_target_type,
)

_INTERNAL_HASH_COLUMNS = frozenset({"__dpone__delta_hash", "__dpone__key_hash", "__dpone__row_hash"})
_CHARACTER_TYPES = frozenset({"char", "nchar", "nvarchar", "varchar"})


def resolve_native_column_types(
    load_config: Any,
    schema: Sequence[tuple[str, str]],
    *,
    fixed_types: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Resolve and structurally validate every native staging type.

    This function is intentionally callable before a wire table is created.
    Invalid or structurally narrowing physical overrides therefore fail before
    staging and before any target DDL/DML.
    """

    pinned = {str(name): str(dtype) for name, dtype in (fixed_types or {}).items()}
    return {
        str(column): pinned.get(str(column)) or resolved_target_type(load_config, str(column), str(dtype))
        for column, dtype in schema
    }


def mssql_unique_keys(load_config: Any) -> list[str]:
    """Return the authored unique key in stable order."""

    raw = getattr(load_config, "unique_key", None)
    if raw is None:
        return []
    values = [raw] if isinstance(raw, str) else list(raw)
    return [str(value) for value in values]


def mssql_equality_keys(load_config: Any, unique_keys: Sequence[str]) -> list[str]:
    """Return every column whose SQL identity must be byte-exact."""

    output = list(unique_keys)
    strategy = getattr(load_config, "load_strategy", None)
    if str(getattr(strategy, "value", strategy)).strip().lower() != "partition_replace":
        return output
    raw = getattr(load_config, "partition", None) or {}
    column = str(raw.get("column") or "").strip() if isinstance(raw, Mapping) else ""
    if column and column not in output:
        output.append(column)
    return output


def mssql_equality_key_collations(
    load_config: Any,
    column_types: Mapping[str, str],
    *,
    unique_keys: Sequence[str] | None = None,
) -> dict[str, str]:
    """Resolve the exact SQL collation for every textual equality authority."""

    keys = list(unique_keys) if unique_keys is not None else mssql_unique_keys(load_config)
    equality_keys = mssql_equality_keys(load_config, keys)
    if any(key not in column_types for key in equality_keys):
        _raise("mssql_native_projection", "equality_key_missing_from_schema")
    return {key: MSSQL_TEXT_KEY_COLLATION for key in equality_keys if is_text_key_type(column_types[key])}


def project_mssql_schema_evolution_columns(
    load_config: Any,
    columns: Sequence[tuple[str, str, bool, str | None]],
) -> tuple[tuple[str, str, bool, str | None], ...]:
    """Return the exact value-guarded MSSQL shape for source columns.

    Explicit bounded text/binary narrowing is admitted only by the shared
    losslessness contract and is checked again for every value before native
    staging. Numeric, temporal, and other structurally lossy overrides fail
    before any staging or target mutation. The target comparator still owns
    drift detection for the resulting exact physical shape.
    """

    projected = tuple(
        (
            str(column),
            resolved_mssql_target_column_type(load_config, str(column), str(dtype)),
            bool(nullable),
            str(collation) if collation is not None else None,
        )
        for column, dtype, nullable, collation in columns
    )
    target_types = {column: dtype for column, dtype, _nullable, _collation in projected}
    equality_collations = mssql_equality_key_collations(load_config, target_types)
    return tuple(
        (
            column,
            dtype,
            nullable,
            equality_collations.get(column, collation),
        )
        for column, dtype, nullable, collation in projected
    )


def validate_native_conversions(
    strategy: Any,
    raw: StagingTableArtifact,
    schema: Sequence[tuple[str, str]],
    target_types: Mapping[str, str],
    *,
    error_prefix: str,
    validate_internal_hashes: bool = True,
) -> None:
    """Prove that every decoded wire value converts without loss.

    SQL Server's implicit and explicit casts may truncate strings/binary or
    round decimal and temporal values.  The guards operate on the decoded raw
    expression and run before a native row or target row is written.
    """

    too_long: list[str] = []
    invalid_hash: list[str] = []
    invalid: list[str] = []
    lossy: list[str] = []
    for column, dtype in schema:
        name = str(column)
        decoded = decoded_staging_expression(strategy, raw, name, "r")
        target = str(target_types[name])
        source = resolved_source_type(str(dtype))
        if validate_internal_hashes and name.lower() in _INTERNAL_HASH_COLUMNS:
            invalid_hash.append(
                f"({decoded} IS NULL OR DATALENGTH(CONVERT(varchar(max), {decoded})) <> 64 "
                f"OR {decoded} COLLATE Latin1_General_100_BIN2 LIKE '%[^0123456789abcdefABCDEF]%')"
            )
        guard_mode = lossless_target_projection_guard_mode(
            source,
            target,
            column="<runtime-value-guard>",
        )
        if guard_mode is MssqlLosslessGuardMode.FORBIDDEN:
            _raise(error_prefix, "lossy_target_type_forbidden")
        if guard_mode is MssqlLosslessGuardMode.VALUE_GUARDED:
            if overflow := bounded_character_overflow(target, decoded):
                too_long.append(overflow)
            if overflow := bounded_binary_overflow(target, decoded):
                too_long.append(overflow)
            converted = try_convert(target, decoded)
            invalid.append(f"({decoded} IS NOT NULL AND {converted} IS NULL)")
            lossy.append(lossless_roundtrip_mismatch(source, target, decoded))
        else:
            # The structural contract proves source -> target is domain safe.
            # Validate only the character-wire boundary of the declared source
            # domain; repeating a target round-trip for every safe column burns
            # CPU and cannot strengthen the proof.
            if overflow := bounded_character_overflow(source, decoded):
                too_long.append(overflow)
            if overflow := bounded_binary_overflow(source, decoded):
                too_long.append(overflow)
            if resolved_mssql_base_type(source) not in _CHARACTER_TYPES:
                converted_source = try_convert(source, decoded)
                invalid.append(f"({decoded} IS NOT NULL AND {converted_source} IS NULL)")
        if floating_loss := floating_point_text_loss(
            source,
            target,
            decoded,
        ):
            lossy.append(floating_loss)
    failures = _conversion_failure_flags(
        strategy,
        raw,
        invalid_hash=invalid_hash,
        too_long=too_long,
        invalid=invalid,
        lossy=lossy,
        error_prefix=error_prefix,
    )
    if failures["invalid_hash"]:
        _raise(error_prefix, "internal_hash_invalid")
    if failures["too_long"]:
        _raise(error_prefix, "value_too_long")
    if failures["invalid"]:
        _raise(error_prefix, "value_invalid")
    if failures["lossy"]:
        _raise(error_prefix, "value_lossy")


def native_select_expression(
    strategy: Any,
    raw: StagingTableArtifact,
    column: str,
    target_type: str,
    *,
    alias: str = "r",
    output_column: str | None = None,
) -> str:
    """Render one decoded, explicitly converted native projection."""

    target_column = output_column or column
    return (
        f"{native_value_expression(strategy, raw, column, target_type, alias=alias)} "
        f"AS {strategy.connector.quote_identifier(target_column)}"
    )


def native_value_expression(
    strategy: Any,
    raw: StagingTableArtifact,
    column: str,
    target_type: str,
    *,
    alias: str = "r",
) -> str:
    """Render one decoded, explicitly converted scalar without a SELECT alias."""

    decoded = decoded_staging_expression(strategy, raw, column, alias)
    return convert(target_type, decoded)


def decoded_staging_expression(
    strategy: Any,
    staging: StagingTableArtifact,
    column: str,
    alias: str,
) -> str:
    """Return one codec-decoded wire scalar without a SELECT alias."""

    quoted = f"{alias}.{strategy.connector.quote_identifier(column)}"
    dtype = (getattr(staging, "column_types", {}) or {}).get(column, "")
    codec = getattr(staging, "bulk_text_codec", None)
    if codec is not None and is_bulk_text_type(str(dtype)) and not column.lower().startswith("__dpone__"):
        return str(codec.mssql_decode_expression(quoted))
    return quoted


def bounded_character_overflow(dtype: str, expression: str) -> str | None:
    """Render an exact bounded-character and ANSI-codepage guard."""

    match = re.fullmatch(r"\s*n?(?:var)?char\s*\(\s*(\d+)\s*\)\s*", dtype, re.IGNORECASE)
    if match is None:
        return None
    length = int(match.group(1))
    normalized = dtype.strip().lower()
    if normalized.startswith(("nvarchar", "nchar")):
        return f"DATALENGTH({expression}) > {length * 2}"
    ansi = f"CONVERT(varchar(max), {expression})"
    roundtrip = f"CONVERT(nvarchar(max), {ansi})"
    source_unicode = f"CONVERT(nvarchar(max), {expression})"
    return (
        f"(DATALENGTH({ansi}) > {length} OR "
        f"CONVERT(varbinary(max), {roundtrip}) <> CONVERT(varbinary(max), {source_unicode}))"
    )


def bounded_binary_overflow(dtype: str, expression: str) -> str | None:
    """Render a non-truncating bounded binary guard."""

    match = re.fullmatch(r"\s*(?:var)?binary\s*\(\s*(\d+)\s*\)\s*", dtype, re.IGNORECASE)
    if match is None:
        return None
    decoded_binary = try_convert("varbinary(max)", expression)
    return f"DATALENGTH({decoded_binary}) > {int(match.group(1))}"


def lossless_roundtrip_mismatch(source: str, target: str, expression: str) -> str:
    """Render source→target→source equality, preserving text/binary bytes."""

    source_value = try_convert(source, expression)
    target_value = try_convert(target, expression)
    roundtrip = try_convert(source, target_value)
    source_base = source.strip().lower().split("(", 1)[0]
    if source_base in {"char", "nchar", "varchar", "nvarchar", "binary", "varbinary"}:
        differs = f"CONVERT(varbinary(max), {roundtrip}) <> CONVERT(varbinary(max), {source_value})"
    else:
        differs = f"{roundtrip} <> {source_value}"
    return (
        f"({expression} IS NOT NULL AND "
        f"({source_value} IS NULL OR {target_value} IS NULL OR {roundtrip} IS NULL OR {differs}))"
    )


def floating_point_text_loss(source: str, target: str, expression: str) -> str | None:
    """Reject text values whose IEEE identity cannot survive SQL conversion.

    SQL Server's character-to-``real``/``float`` conversion silently maps
    PostgreSQL subnormals to zero.  It also normalizes ``-0`` to ``+0``.  A
    source→target→source cast cannot discover either loss because the first
    SQL Server cast has already discarded the bits, so inspect the immutable
    source mantissa before native materialization.
    """

    source_base = source.strip().lower().split("(", 1)[0]
    target_base = target.strip().lower().split("(", 1)[0]
    if source_base not in {"real", "float"} or target_base not in {"real", "float"}:
        return None
    text = f"LOWER(LTRIM(RTRIM(CONVERT(nvarchar(max), {expression}))))"
    exponent = f"CHARINDEX(N'e', {text})"
    mantissa = f"LEFT({text}, CASE WHEN {exponent} > 0 THEN {exponent} - 1 ELSE LEN({text}) END)"
    converted = try_convert(target, expression)
    return (
        f"({expression} IS NOT NULL AND {converted} = CONVERT({target}, 0) AND "
        f"({mantissa} LIKE N'%[1-9]%' OR LEFT({text}, 1) = N'-'))"
    )


def convert(dtype: str, expression: str) -> str:
    """Render a strict conversion, including hex character-wire decoding."""

    style = ", 2" if is_mssql_hex_binary_wire_type(dtype) else ""
    return f"CONVERT({dtype}, {expression}{style})"


def try_convert(dtype: str, expression: str) -> str:
    """Render a non-throwing conversion for validation predicates."""

    style = ", 2" if is_mssql_hex_binary_wire_type(dtype) else ""
    return f"TRY_CONVERT({dtype}, {expression}{style})"


def _conversion_failure_flags(
    strategy: Any,
    raw: StagingTableArtifact,
    *,
    invalid_hash: Sequence[str],
    too_long: Sequence[str],
    invalid: Sequence[str],
    lossy: Sequence[str],
    error_prefix: str,
) -> dict[str, bool]:
    """Evaluate every lossless-conversion category in one raw-table scan."""

    categories = {
        "invalid_hash": invalid_hash,
        "too_long": too_long,
        "invalid": invalid,
        "lossy": lossy,
    }
    projections = ", ".join(
        (
            "CONVERT(int, 0)"
            if not predicates
            else f"COALESCE(MAX(CASE WHEN {' OR '.join(predicates)} THEN 1 ELSE 0 END), 0)"
        )
        + f" AS {strategy.connector.quote_identifier(name)}"
        for name, predicates in categories.items()
    )
    rows = strategy.connector.get_records(
        f"SELECT {projections} FROM {strategy._staging_name(raw)} AS r",
        as_dict=True,
    )
    if len(rows) != 1 or not isinstance(rows[0], Mapping):
        _raise(error_prefix, "conversion_evidence_unavailable")
    result: dict[str, bool] = {}
    for name in categories:
        value = rows[0].get(name)
        if isinstance(value, bool) or not isinstance(value, int) or value not in {0, 1}:
            _raise(error_prefix, "conversion_evidence_invalid")
        result[name] = bool(value)
    return result


def _raise(prefix: str, suffix: str) -> None:
    separator = "" if prefix.endswith((".", "_")) else "."
    raise SnapshotReconciliationError(f"{prefix}{separator}{suffix}")


__all__ = [
    "bounded_binary_overflow",
    "bounded_character_overflow",
    "convert",
    "decoded_staging_expression",
    "floating_point_text_loss",
    "lossless_roundtrip_mismatch",
    "mssql_equality_key_collations",
    "mssql_equality_keys",
    "mssql_unique_keys",
    "native_select_expression",
    "native_value_expression",
    "project_mssql_schema_evolution_columns",
    "resolve_native_column_types",
    "try_convert",
    "validate_native_conversions",
]
