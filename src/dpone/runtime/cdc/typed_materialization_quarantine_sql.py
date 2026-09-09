"""Parse-quarantine SQL for ClickHouse CDC typed materialization."""

from __future__ import annotations

from typing import TYPE_CHECKING

from .typed_materialization_common import quote_identifier, quote_literal
from .typed_materialization_projection import ClickHouseCdcPayloadProjector
from .typed_materialization_sql import latest_rows_select_sql

if TYPE_CHECKING:
    from dpone.ports.cdc_typed_materialization import ClickHouseCdcTypedPlanPort, ClickHouseCdcTypedPolicyPort

_QUARANTINE_COLUMNS: tuple[tuple[str, str], ...] = (
    ("column_name", "LowCardinality(String)"),
    ("clickhouse_type", "String"),
    ("dpone_cdc_unique_key_hash", "String"),
    ("dpone_cdc_unique_key_json", "String"),
    ("dpone_cdc_position", "String"),
    ("dpone_cdc_event_hash", "String"),
    ("dpone_cdc_payload_json", "String"),
    ("dpone_cdc_ingested_at", "DateTime64(3, 'UTC')"),
    ("reason", "String"),
    ("quarantined_at", "DateTime64(3, 'UTC')"),
)


def create_parse_quarantine_table_sql(qualified_quarantine_table: str) -> str:
    columns = ",\n    ".join(f"{quote_identifier(name)} {kind}" for name, kind in _QUARANTINE_COLUMNS)
    return f"""
CREATE TABLE IF NOT EXISTS {qualified_quarantine_table} (
    {columns}
)
ENGINE = MergeTree
ORDER BY (`column_name`, `dpone_cdc_unique_key_hash`, `dpone_cdc_event_hash`)
""".strip()


def parse_failure_counts_sql(
    *,
    plan: ClickHouseCdcTypedPlanPort,
    policy: ClickHouseCdcTypedPolicyPort,
) -> str:
    selects = [
        f"""
SELECT
    {quote_literal(column.name)} AS column_name,
    {quote_literal(column.clickhouse_type)} AS clickhouse_type,
    count() AS failed_rows
FROM ({latest_rows_select_sql(plan)})
WHERE rn = 1{_delete_filter(policy)} AND {predicate}
""".strip()
        for column in plan.columns
        if (predicate := ClickHouseCdcPayloadProjector.parse_failure_predicate(column, "dpone_cdc_payload_json"))
    ]
    if not selects:
        return "SELECT '' AS column_name, '' AS clickhouse_type, 0 AS failed_rows WHERE 0"
    return "\nUNION ALL\n".join(selects)


def insert_parse_quarantine_sql(
    *,
    plan: ClickHouseCdcTypedPlanPort,
    policy: ClickHouseCdcTypedPolicyPort,
    qualified_quarantine_table: str,
) -> str:
    target_columns = ", ".join(quote_identifier(name) for name, _ in _QUARANTINE_COLUMNS)
    selects = [
        f"""
SELECT
    {quote_literal(column.name)} AS {quote_identifier("column_name")},
    {quote_literal(column.clickhouse_type)} AS {quote_identifier("clickhouse_type")},
    {quote_identifier("dpone_cdc_unique_key_hash")},
    {quote_identifier("dpone_cdc_unique_key_json")},
    {quote_identifier("dpone_cdc_position")},
    {quote_identifier("dpone_cdc_event_hash")},
    {quote_identifier("dpone_cdc_payload_json")},
    {quote_identifier("dpone_cdc_ingested_at")},
    'typed_projection_parse_failed' AS {quote_identifier("reason")},
    now64(3, 'UTC') AS {quote_identifier("quarantined_at")}
FROM ({latest_rows_select_sql(plan)})
WHERE rn = 1{_delete_filter(policy)} AND {predicate}
""".strip()
        for column in plan.columns
        if (predicate := ClickHouseCdcPayloadProjector.parse_failure_predicate(column, "dpone_cdc_payload_json"))
    ]
    if not selects:
        return f"INSERT INTO {qualified_quarantine_table} ({target_columns}) SELECT * WHERE 0"
    return f"INSERT INTO {qualified_quarantine_table} ({target_columns})\n" + "\nUNION ALL\n".join(selects)


def _delete_filter(policy: ClickHouseCdcTypedPolicyPort) -> str:
    return "" if policy.delete_mode == "tombstone" else " AND dpone_cdc_deleted = 0"


__all__ = [
    "create_parse_quarantine_table_sql",
    "insert_parse_quarantine_sql",
    "parse_failure_counts_sql",
]
