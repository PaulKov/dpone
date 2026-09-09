"""Technical columns policy helpers.

Why
----
dpone historically used a boolean flag ``sink.options.include_technical_columns`` to
control whether technical columns are ensured/populated. New generated columns
use the canonical ``__dpone__*`` namespace; legacy ``meta__*`` names are resolved
only in ``technical_columns_naming=legacy_compat`` mode.

For better UX, we introduce a tri-state mode ``sink.options.technical_columns``:

- required   -> always enabled
- optional   -> follow include_technical_columns (default: enabled)
- forbidden  -> always disabled

The boolean flag is still supported for backward compatibility.

This module centralizes parsing/resolution so sinks, validator and runtime logic
stay consistent (DRY).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from dpone._compat import StrEnum
from dpone.contracts.soft_delete import SoftDeleteMode as SoftDeleteMode
from dpone.contracts.soft_delete import SoftDeletePolicy as SoftDeletePolicy

_OFFSET_COMPANION_PREFIX = "__dpone__tz_offset_minutes__"


class TechnicalColumnsMode(StrEnum):
    REQUIRED = "required"
    OPTIONAL = "optional"
    FORBIDDEN = "forbidden"


class TechnicalColumnRole(StrEnum):
    """Stable logical roles for dpone-generated framework columns."""

    LOADED_AT = "loaded_at"
    UPDATED_AT = "updated_at"
    DELETED_AT = "deleted_at"
    XMIN = "xmin"
    DELETED_MARKER = "deleted_marker"
    IS_DELETED = "is_deleted"
    RUN_ID = "run_id"
    LOAD_ID = "load_id"
    ROW_ID = "row_id"
    PARENT_ROW_ID = "parent_row_id"
    ROOT_ROW_ID = "root_row_id"
    LIST_INDEX = "list_index"
    OP = "op"
    META = "meta"
    ROW_HASH = "row_hash"
    VALID_FROM_AT = "valid_from_at"
    VALID_TO_AT = "valid_to_at"
    IS_CURRENT = "is_current"
    EXTRACTED_AT = "extracted_at"


@dataclass(frozen=True, slots=True)
class TechnicalColumnDefinition:
    """A single canonical framework column definition."""

    role: TechnicalColumnRole
    name: str
    logical_type: str
    nullable: bool = True
    description: str = ""


class TechnicalColumnCatalog:
    """Single source of truth for dpone framework column names and types."""

    _DEFINITIONS: tuple[TechnicalColumnDefinition, ...] = (
        TechnicalColumnDefinition(
            TechnicalColumnRole.LOADED_AT,
            "__dpone__loaded_at",
            "timestamp",
            nullable=False,
            description="UTC timestamp when the row was finalized in the target.",
        ),
        TechnicalColumnDefinition(
            TechnicalColumnRole.DELETED_AT,
            "__dpone__deleted_at",
            "timestamp",
            description=(
                "UTC time when snapshot reconciliation detected that the row was absent; "
                "it is not the source delete-event time."
            ),
        ),
        TechnicalColumnDefinition(
            TechnicalColumnRole.UPDATED_AT,
            "__dpone__updated_at",
            "timestamp",
            nullable=False,
            description="UTC timestamp when a framework state/audit row was last updated.",
        ),
        TechnicalColumnDefinition(
            TechnicalColumnRole.XMIN,
            "__dpone__xmin",
            "bigint",
            description="PostgreSQL xmin value captured by the XMin source strategy.",
        ),
        TechnicalColumnDefinition(
            TechnicalColumnRole.DELETED_MARKER,
            "__dpone__deleted_marker",
            "boolean",
            description="Internal staged marker for delete-aware reconciliation paths.",
        ),
        TechnicalColumnDefinition(
            TechnicalColumnRole.IS_DELETED,
            "__dpone__is_deleted",
            "boolean",
            nullable=False,
            description="Current soft-delete flag when flag storage is selected.",
        ),
        TechnicalColumnDefinition(
            TechnicalColumnRole.RUN_ID,
            "__dpone__run_id",
            "varchar(26)",
            nullable=False,
            description="ULID identifying a full dpone process execution.",
        ),
        TechnicalColumnDefinition(
            TechnicalColumnRole.LOAD_ID,
            "__dpone__load_id",
            "varchar(26)",
            nullable=False,
            description="ULID identifying an atomic load package.",
        ),
        TechnicalColumnDefinition(
            TechnicalColumnRole.ROW_ID,
            "__dpone__row_id",
            "varchar(64)",
            description="Deterministic SHA-256 hex lineage identifier for a row.",
        ),
        TechnicalColumnDefinition(
            TechnicalColumnRole.PARENT_ROW_ID,
            "__dpone__parent_row_id",
            "varchar(64)",
            description="Deterministic SHA-256 hex lineage identifier for the parent row.",
        ),
        TechnicalColumnDefinition(
            TechnicalColumnRole.ROOT_ROW_ID,
            "__dpone__root_row_id",
            "varchar(64)",
            description="Deterministic SHA-256 hex lineage identifier for the root row.",
        ),
        TechnicalColumnDefinition(
            TechnicalColumnRole.LIST_INDEX,
            "__dpone__list_index",
            "integer",
            description="Stable zero-based index for nested list normalization.",
        ),
        TechnicalColumnDefinition(
            TechnicalColumnRole.OP,
            "__dpone__op",
            "varchar(32)",
            description="Normalized operation marker such as insert, update or delete.",
        ),
        TechnicalColumnDefinition(
            TechnicalColumnRole.META,
            "__dpone__meta",
            "json",
            description="Optional row-level diagnostics metadata.",
        ),
        TechnicalColumnDefinition(
            TechnicalColumnRole.ROW_HASH,
            "__dpone__row_hash",
            "varchar(64)",
            description="Deterministic SHA-256 hex hash used by diff/SCD strategies.",
        ),
        TechnicalColumnDefinition(
            TechnicalColumnRole.VALID_FROM_AT,
            "__dpone__valid_from_at",
            "timestamp",
            nullable=False,
            description="UTC timestamp when an SCD2 version becomes valid.",
        ),
        TechnicalColumnDefinition(
            TechnicalColumnRole.VALID_TO_AT,
            "__dpone__valid_to_at",
            "timestamp",
            description="UTC timestamp when an SCD2 version stops being valid.",
        ),
        TechnicalColumnDefinition(
            TechnicalColumnRole.IS_CURRENT,
            "__dpone__is_current",
            "boolean",
            nullable=False,
            description="SCD2 current-version flag.",
        ),
        TechnicalColumnDefinition(
            TechnicalColumnRole.EXTRACTED_AT,
            "__dpone__extracted_at",
            "timestamp",
            nullable=False,
            description="UTC timestamp when the source row was extracted.",
        ),
    )

    _LEGACY_NAMES: Mapping[str, TechnicalColumnRole] = {
        "meta__load_dtm": TechnicalColumnRole.LOADED_AT,
        "meta__update_dtm": TechnicalColumnRole.UPDATED_AT,
        "meta__delete_dtm": TechnicalColumnRole.DELETED_AT,
        "meta__xmin": TechnicalColumnRole.XMIN,
        "__dpone_deleted_marker": TechnicalColumnRole.DELETED_MARKER,
    }

    def __init__(self) -> None:
        self._by_role = {definition.role: definition for definition in self._DEFINITIONS}
        self._by_name = {definition.name.lower(): definition for definition in self._DEFINITIONS}

    def definition(self, role: TechnicalColumnRole | str) -> TechnicalColumnDefinition:
        normalized = TechnicalColumnRole(role)
        return self._by_role[normalized]

    def definitions(self) -> tuple[TechnicalColumnDefinition, ...]:
        return self._DEFINITIONS

    def name(self, role: TechnicalColumnRole | str) -> str:
        return self.definition(role).name

    def schema_columns(self, roles: tuple[TechnicalColumnRole, ...]) -> list[tuple[str, str]]:
        return [(self.definition(role).name, self.definition(role).logical_type) for role in roles]

    def is_canonical(self, name: str) -> bool:
        return str(name).lower() in self._by_name

    def resolve_name(self, name: str, *, naming: str = "canonical") -> str:
        raw = str(name)
        if str(naming).strip().lower() != "legacy_compat":
            return raw
        role = self._LEGACY_NAMES.get(raw.lower())
        if role is None:
            return raw
        return self.name(role)


def resolve_technical_column_name(name: str, *, naming: str = "canonical") -> str:
    """Resolve a technical column name according to the selected naming mode."""

    return TechnicalColumnCatalog().resolve_name(name, naming=naming)


def offset_minutes_column_name(column: str) -> str:
    """Return the canonical generated companion for a source offset."""

    return f"{_OFFSET_COMPANION_PREFIX}{column}"


def is_offset_minutes_column(column: str) -> bool:
    """Return whether ``column`` is a framework offset companion."""

    return str(column).startswith(_OFFSET_COMPANION_PREFIX)


def _parse_mode(value: Any) -> TechnicalColumnsMode | None:
    if value is None:
        return None
    if isinstance(value, TechnicalColumnsMode):
        return value
    if isinstance(value, bool):
        return TechnicalColumnsMode.REQUIRED if value else TechnicalColumnsMode.FORBIDDEN
    if isinstance(value, int | float):
        return TechnicalColumnsMode.REQUIRED if bool(value) else TechnicalColumnsMode.FORBIDDEN
    if isinstance(value, str):
        s = value.strip().lower()
        if not s:
            return None
        if s in {"required", "require", "req", "on", "enabled"}:
            return TechnicalColumnsMode.REQUIRED
        if s in {"optional", "opt", "default"}:
            return TechnicalColumnsMode.OPTIONAL
        if s in {"forbidden", "forbid", "off", "disabled"}:
            return TechnicalColumnsMode.FORBIDDEN
    return None


def parse_mode(value: Any) -> TechnicalColumnsMode:
    """Parses a technical columns mode.

    Raises:
        ValueError: if value is present but cannot be parsed.
    """

    m = _parse_mode(value)
    if m is None:
        raise ValueError(f"Invalid technical_columns mode: {value!r}. Expected one of: required|optional|forbidden")
    return m


def _parse_boolish(value: Any) -> tuple[bool | None, str | None]:
    """Parses a bool-like value.

    Returns:
        (parsed_value, warning)

    warning is returned when value is not a clean boolean but is still coerced.
    """

    if value is None:
        return None, None
    if isinstance(value, bool):
        return value, None
    if isinstance(value, int | float):
        return bool(value), "coerced from numeric"
    if isinstance(value, str):
        s = value.strip().lower()
        if s in {"false", "0", "no", "off"}:
            return False, "coerced from string"
        if s in {"true", "1", "yes", "on"}:
            return True, "coerced from string"
        # Backward compatible fallback: empty string treated as True.
        if s == "":
            return True, "empty string treated as true"
        # Unknown string: keep old behavior (truthiness) but flag warning.
        return bool(s), "unknown string coerced by truthiness"
    return bool(value), "coerced by truthiness"


@dataclass(frozen=True, slots=True)
class TechnicalColumnsResolution:
    mode: TechnicalColumnsMode | None
    enabled: bool
    include_flag_raw: Any = None
    warning: str | None = None


def resolve_technical_columns(opts: Mapping[str, Any]) -> TechnicalColumnsResolution:
    """Resolves technical columns settings from sink options.

    Precedence:
    1) sink.options.technical_columns (required|optional|forbidden)
    2) sink.options.include_technical_columns (bool-ish, default: True)

    Returns:
        TechnicalColumnsResolution(mode, enabled, warning)

    Raises:
        ValueError: if technical_columns is present but invalid.
    """

    raw_mode = None
    # Support both keys for future flexibility
    if isinstance(opts, Mapping):
        if "technical_columns" in opts and opts.get("technical_columns") is not None:
            raw_mode = opts.get("technical_columns")
        elif "technical_columns_mode" in opts and opts.get("technical_columns_mode") is not None:
            raw_mode = opts.get("technical_columns_mode")

    mode: TechnicalColumnsMode | None = None
    if raw_mode is not None:
        mode = parse_mode(raw_mode)

    include_raw = opts.get("include_technical_columns") if isinstance(opts, Mapping) else None
    include_flag, include_warn = _parse_boolish(include_raw)

    # Resolve effective enabled state
    if mode == TechnicalColumnsMode.REQUIRED:
        enabled = True
    elif mode == TechnicalColumnsMode.FORBIDDEN:
        enabled = False
    else:
        enabled = include_flag if include_flag is not None else True

    # Detect conflicting declarations (helps UX / avoids surprises)
    conflict_warn: str | None = None
    if mode == TechnicalColumnsMode.REQUIRED and include_flag is False:
        conflict_warn = "technical_columns=required conflicts with include_technical_columns=false (mode wins)"
    if mode == TechnicalColumnsMode.FORBIDDEN and include_flag is True:
        conflict_warn = "technical_columns=forbidden conflicts with include_technical_columns=true (mode wins)"

    warn = conflict_warn or include_warn
    return TechnicalColumnsResolution(
        mode=mode,
        enabled=bool(enabled),
        include_flag_raw=include_raw,
        warning=warn,
    )


def include_technical_columns(opts: Mapping[str, Any]) -> bool:
    """Convenience wrapper: returns resolved enabled flag."""

    return resolve_technical_columns(opts).enabled
