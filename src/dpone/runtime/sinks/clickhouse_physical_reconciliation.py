"""ClickHouse physical design introspection and safe migration rendering."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from dpone.readiness.physical_state import PhysicalColumnState, PhysicalTableState, TableSettingValue

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig

_SETTING_ITEM = re.compile(r"\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.+?)\s*$")
_ONLINE_SAFE_TABLE_SETTINGS = {
    "max_bytes_to_merge_at_max_space_in_pool",
    "max_parts_in_total",
    "merge_max_block_size",
    "min_bytes_for_wide_part",
    "min_rows_for_wide_part",
    "old_parts_lifetime",
    "parts_to_delay_insert",
    "parts_to_throw_insert",
    "ratio_of_defaults_for_sparse_serialization",
}


@dataclass(frozen=True, slots=True)
class ClickHousePhysicalIntrospector:
    connector: Any

    def inspect(self, load_config: LoadConfig) -> PhysicalTableState:
        table_row = self._table_row(load_config)
        if not table_row:
            return PhysicalTableState(sink_type="clickhouse", table=_qualified_table(load_config))
        create_query = str(table_row.get("create_table_query") or "")
        settings, settings_error = parse_clickhouse_table_settings(create_query)
        return PhysicalTableState(
            sink_type="clickhouse",
            table=_qualified_table(load_config),
            columns=self._columns(load_config),
            engine=_optional_str(table_row.get("engine")),
            engine_full=_optional_str(table_row.get("engine_full")),
            partition_by=_optional_str(table_row.get("partition_key")),
            order_by=tuple(_list_option(table_row.get("sorting_key"))),
            primary_key=tuple(_list_option(table_row.get("primary_key"))),
            ttl=parse_clickhouse_table_ttl(create_query),
            table_settings=settings,
            table_settings_error=settings_error,
        )

    def _table_row(self, load_config: LoadConfig) -> Mapping[str, Any]:
        rows = self.connector.get_records(
            "SELECT engine, engine_full, partition_key, sorting_key, primary_key, create_table_query "
            "FROM system.tables "
            f"WHERE database = '{_sql_literal(load_config.target_schema)}' "
            f"AND name = '{_sql_literal(load_config.target_table)}'"
        )
        if not rows:
            return {}
        return _row_mapping(
            rows[0],
            ("engine", "engine_full", "partition_key", "sorting_key", "primary_key", "create_table_query"),
        )

    def _columns(self, load_config: LoadConfig) -> Mapping[str, PhysicalColumnState]:
        rows = self.connector.get_records(
            "SELECT name, type, position FROM system.columns "
            f"WHERE database = '{_sql_literal(load_config.target_schema)}' "
            f"AND table = '{_sql_literal(load_config.target_table)}' ORDER BY position"
        )
        columns = [_column(_row_mapping(row, ("name", "type", "position"))) for row in rows]
        return {column.name: column for column in columns}


@dataclass(frozen=True, slots=True)
class ClickHousePhysicalMigrationDialect:
    """Render only online-safe ClickHouse physical changes."""

    def is_online_safe_table_setting(self, setting: str) -> bool:
        return setting in _ONLINE_SAFE_TABLE_SETTINGS

    def is_safe_window_table_setting(self, setting: str) -> bool:
        return False

    def render_table_setting_update(self, *, table: str, setting: str, value: TableSettingValue) -> str:
        return f"ALTER TABLE {_quote_table(table)} MODIFY SETTING {setting} = {_literal(value)}"


def parse_clickhouse_table_settings(create_query: str) -> tuple[Mapping[str, TableSettingValue], str | None]:
    match = re.search(r"\bSETTINGS\b(.+)$", create_query, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return {}, None
    settings: dict[str, TableSettingValue] = {}
    for item in _split_settings(match.group(1).strip().rstrip(";")):
        parsed = _SETTING_ITEM.match(item)
        if parsed is None:
            return {}, f"Cannot parse ClickHouse table setting: {item}"
        settings[parsed.group(1)] = _parse_literal(parsed.group(2).strip())
    return dict(sorted(settings.items())), None


def parse_clickhouse_table_ttl(create_query: str) -> str | None:
    """Extract table-level TTL expression from CREATE TABLE (after ENGINE, before SETTINGS)."""
    engine_match = re.search(r"\bENGINE\b\s*=", create_query, flags=re.IGNORECASE)
    if not engine_match:
        return None
    tail = create_query[engine_match.start() :]
    settings_match = re.search(r"\bSETTINGS\b", tail, flags=re.IGNORECASE)
    if settings_match:
        tail = tail[: settings_match.start()]
    match = re.search(r"\bTTL\b\s+(.+?)\s*$", tail, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return None
    expression = " ".join(match.group(1).split())
    return expression or None


def _column(row: Mapping[str, Any]) -> PhysicalColumnState:
    dtype = str(row.get("type") or "")
    return PhysicalColumnState(
        name=str(row.get("name") or ""),
        target_type=dtype,
        nullable=dtype.startswith("Nullable("),
        position=_optional_int(row.get("position")),
    )


def _split_settings(value: str) -> list[str]:
    items: list[str] = []
    current: list[str] = []
    quoted = False
    for char in value:
        if char == "'":
            quoted = not quoted
        if char == "," and not quoted:
            items.append("".join(current).strip())
            current = []
        else:
            current.append(char)
    if current:
        items.append("".join(current).strip())
    return [item for item in items if item]


def _parse_literal(value: str) -> TableSettingValue:
    if value.startswith("'") and value.endswith("'"):
        return value[1:-1].replace("''", "'")
    if value in {"0", "1"}:
        return int(value)
    try:
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError:
            return value


def _row_mapping(row: Any, columns: tuple[str, ...]) -> Mapping[str, Any]:
    if isinstance(row, Mapping):
        return row
    return {column: row[index] if index < len(row) else None for index, column in enumerate(columns)}


def _list_option(value: Any) -> list[str]:
    if value is None:
        return []
    text = str(value).strip()
    if not text or text.lower() == "tuple()":
        return []
    return [item.strip().strip("`") for item in text.split(",") if item.strip()]


def _quote_table(table: str) -> str:
    return ".".join(f"`{part.strip('`')}`" for part in table.split("."))


def _literal(value: TableSettingValue) -> str:
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int | float):
        return str(value)
    return "'" + value.replace("'", "''") + "'"


def _sql_literal(value: str) -> str:
    return str(value).replace("'", "\\'")


def _qualified_table(load_config: LoadConfig) -> str:
    return f"{load_config.target_schema}.{load_config.target_table}"


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _optional_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


__all__ = [
    "ClickHousePhysicalIntrospector",
    "ClickHousePhysicalMigrationDialect",
    "parse_clickhouse_table_settings",
    "parse_clickhouse_table_ttl",
]
