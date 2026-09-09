"""SQL rendering for ClickHouse CDC typed materialization."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from .typed_materialization_common import quote_identifier
from .typed_materialization_projection import ClickHouseCdcPayloadProjector

if TYPE_CHECKING:
    from dpone.ports.cdc_typed_materialization import (
        ClickHouseCdcTypedColumnPort,
        ClickHouseCdcTypedPlanPort,
        ClickHouseCdcTypedPolicyPort,
    )

METADATA_COLUMNS: tuple[tuple[str, str], ...] = (
    ("dpone_cdc_unique_key_hash", "String"),
    ("dpone_cdc_unique_key_json", "String"),
    ("dpone_cdc_operation", "LowCardinality(String)"),
    ("dpone_cdc_position", "String"),
    ("dpone_cdc_event_hash", "String"),
    ("dpone_cdc_payload_json", "String"),
    ("dpone_cdc_deleted", "UInt8"),
    ("dpone_cdc_ingested_at", "DateTime64(3, 'UTC')"),
    ("dpone_cdc_materialized_at", "DateTime64(3, 'UTC')"),
)


def create_table_sql(qualified_table: str, columns: Sequence[ClickHouseCdcTypedColumnPort]) -> str:
    metadata = [f"{quote_identifier(name)} {kind}" for name, kind in METADATA_COLUMNS]
    typed = [column.ddl_fragment for column in columns]
    all_columns = ",\n    ".join((*metadata, *typed))
    return f"""
CREATE TABLE {qualified_table} (
    {all_columns}
)
ENGINE = MergeTree
ORDER BY (`dpone_cdc_unique_key_hash`)
""".strip()


def insert_latest_sql(
    *,
    plan: ClickHouseCdcTypedPlanPort,
    policy: ClickHouseCdcTypedPolicyPort,
) -> str:
    metadata_names = [name for name, _ in METADATA_COLUMNS]
    typed_names = [column.name for column in plan.columns]
    insert_columns = ", ".join(quote_identifier(name) for name in (*metadata_names, *typed_names))
    metadata_select = ",\n    ".join(
        quote_identifier(name) for name in metadata_names if name != "dpone_cdc_materialized_at"
    )
    typed_select = ClickHouseCdcPayloadProjector(plan.columns).select_expressions(
        payload_column="dpone_cdc_payload_json"
    )
    deleted_filter = "" if policy.delete_mode == "tombstone" else " AND dpone_cdc_deleted = 0"
    return f"""
INSERT INTO {plan.qualified_shadow_table} ({insert_columns})
SELECT
    {metadata_select},
    now64(3, 'UTC') AS `dpone_cdc_materialized_at`,
    {typed_select}
FROM ({latest_rows_select_sql(plan)})
WHERE rn = 1{deleted_filter}
""".strip()


def latest_rows_select_sql(plan: ClickHouseCdcTypedPlanPort) -> str:
    return f"""
SELECT
    *,
    row_number() OVER (
        PARTITION BY dpone_cdc_unique_key_hash
        ORDER BY
            dpone_cdc_ingested_at DESC,
            length(dpone_cdc_position) DESC,
            dpone_cdc_position DESC,
            dpone_cdc_sequence DESC,
            dpone_cdc_event_hash DESC
    ) AS rn
FROM {plan.qualified_cdc_table}
""".strip()


__all__ = [
    "METADATA_COLUMNS",
    "create_table_sql",
    "insert_latest_sql",
    "latest_rows_select_sql",
]
