"""Strategy behavior helpers for integration matrix certification."""

from __future__ import annotations

import zlib
from typing import Any

from dpone.integration_matrix_counts import (
    _changed_count,
    _delete_count,
    _insert_count,
    _source_snapshot_count_after_delta,
    _update_count,
)


def _mock_actual_row_count(
    strategy: str, *, sink: str, row_count: int, change_ratio: float, delete_ratio: float
) -> int:
    if sink == "kafka":
        if strategy == "full_refresh":
            return row_count
        return _changed_count(row_count, change_ratio=change_ratio)
    if strategy == "incremental_append":
        return row_count + _changed_count(row_count, change_ratio=change_ratio)
    if strategy == "scd2":
        return (
            row_count
            + _update_count(row_count, change_ratio=change_ratio, delete_ratio=delete_ratio)
            + _insert_count(row_count, change_ratio=change_ratio, delete_ratio=delete_ratio)
        )
    if strategy == "full_refresh":
        return row_count
    return _source_snapshot_count_after_delta(row_count, change_ratio=change_ratio, delete_ratio=delete_ratio)


def _mock_strategy_checksum(
    strategy: str,
    *,
    source: str,
    sink: str,
    row_count: int,
    change_ratio: float,
    delete_ratio: float,
) -> str:
    changed_count = _changed_count(row_count, change_ratio=change_ratio)
    deleted_count = _delete_count(row_count, delete_ratio=delete_ratio)
    inserted_count = _insert_count(row_count, change_ratio=change_ratio, delete_ratio=delete_ratio)
    updated_count = _update_count(row_count, change_ratio=change_ratio, delete_ratio=delete_ratio)
    token = (
        f"{source}:{sink}:{strategy}:rows={row_count}:changed={changed_count}:deleted={deleted_count}:"
        f"inserted={inserted_count}:updated={updated_count}:actual="
        f"{_mock_actual_row_count(strategy, sink=sink, row_count=row_count, change_ratio=change_ratio, delete_ratio=delete_ratio)}"
    )
    return f"{zlib.crc32(token.encode('utf-8')):08x}"


def _mock_quality_checks(strategy: str, *, sink: str) -> tuple[str, ...]:
    common = ("wide_columns_preserved", "no_unexpected_null_density")
    if sink == "kafka":
        checks = ["event_count_matches_expected", "keyed_events_present", *common]
        if strategy in {"incremental_append", "incremental_merge", "replace", "snapshot_diff", "xmin", "cdc"}:
            checks.append("delete_events_present_when_expected")
        return tuple(checks)
    if strategy == "incremental_append":
        return ("append_count_matches_delta", "existing_rows_preserved", *common)
    if strategy == "full_refresh":
        return ("row_count_matches_full_source", "checksum_matches_full_source", *common)
    if strategy == "snapshot_diff":
        return (
            "row_count_matches_source_snapshot",
            "checksum_matches_source_snapshot",
            "snapshot_diff_row_hash_matches",
            "delete_keys_absent",
            *common,
        )
    if strategy == "scd2":
        return (
            "scd2_current_rows_match_source",
            "scd2_history_closed",
            "row_hash_changes_create_new_versions",
            *common,
        )
    if strategy == "backfill":
        return (
            "row_count_matches_source_snapshot",
            "checksum_matches_source_snapshot",
            "backfill_chunk_state_committed",
            "delete_keys_absent",
            *common,
        )
    return (
        "row_count_matches_source_snapshot",
        "checksum_matches_source_snapshot",
        "delete_keys_absent",
        *common,
    )


def _mock_kafka_events(strategy: str, rows: tuple[dict[str, Any], ...]) -> tuple[dict[str, Any], ...]:
    events: list[dict[str, Any]] = []
    for row in rows:
        op = _kafka_event_op(strategy, row)
        key = row["id"] if op != "snapshot" else row["id"]
        events.append(
            {
                "key": key,
                "op": op,
                "strategy": strategy,
                "metadata": {
                    "source_family": row["source_family"],
                    "business_date": row["business_date"],
                    "schema_version": 1,
                },
                "data": row if op != "delete" else None,
            }
        )
    return tuple(events)


def _kafka_event_op(strategy: str, row: dict[str, Any]) -> str:
    source_op = str(row.get("__dpone__op", "")).strip()
    if strategy == "full_refresh":
        return "snapshot"
    if strategy == "incremental_append":
        return source_op or "insert"
    if strategy in {"incremental_merge", "snapshot_diff", "xmin"}:
        return "delete" if source_op == "delete" else "upsert"
    if strategy == "replace":
        return "delete" if source_op == "delete" else "replace"
    if strategy == "partition_replace":
        return "partition_replace"
    if strategy == "cdc":
        return source_op or "update"
    return source_op or "upsert"


def _mock_strategy_notes(strategy: str, *, sink: str) -> tuple[str, ...]:
    if sink == "kafka":
        return (
            "Kafka sink is modeled as an append-only event log.",
            f"Strategy {strategy} produces keyed events and does not mutate target rows.",
        )
    if strategy == "full_refresh":
        return ("Target rows are replaced by the full source boundary after staging validation.",)
    if strategy == "incremental_append":
        return ("Source delta rows are appended; existing target rows are preserved.",)
    if strategy == "incremental_merge":
        return (
            "Rows are delete-aware upserted by id: delete keys are reconciled, matches update, and new ids insert.",
        )
    if strategy == "snapshot_diff":
        return (
            "A complete source snapshot is compared with target rows by id and __dpone__row_hash; target-only keys follow delete_policy.",
        )
    if strategy == "scd2":
        return (
            "Changed current rows are expired and new current versions are inserted with canonical __dpone__ SCD2 columns.",
        )
    if strategy == "backfill":
        return (
            "Historical chunks are executed through an inner staged strategy and committed independently for resumability.",
        )
    if strategy == "xmin":
        return (
            "Postgres XMin rows are treated as a bounded transaction-id delta and paired with snapshot reconciliation for physical deletes.",
        )
    if strategy == "cdc":
        return ("CDC insert, update, and delete events are applied with typed event semantics after sink success.",)
    if strategy == "replace":
        return ("Only the deterministic predicate window business_date = 2026-06-03 is replaced through staging.",)
    if strategy == "partition_replace":
        return ("Only partitions represented by staging business_date values are replaced through staging.",)
    return (f"Unsupported strategy {strategy}.",)


def _default_merge_policy_for_sink(sink: str) -> str:
    defaults = {
        "mssql": "delete_insert",
        "postgres": "delete_insert",
        "bigquery": "delete_insert",
        "clickhouse": "lightweight_delete_insert",
        "kafka": "event_upsert",
    }
    return defaults[sink]


__all__ = [
    "_mock_actual_row_count",
    "_mock_strategy_checksum",
    "_mock_quality_checks",
    "_mock_kafka_events",
    "_kafka_event_op",
    "_mock_strategy_notes",
    "_default_merge_policy_for_sink",
]
