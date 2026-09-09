"""Legacy PostgreSQL XMin artifact selection inside one RR snapshot."""

from __future__ import annotations

from typing import Any

from psycopg import sql

from dpone.runtime.sources.strategies.postgres.postgres_snapshot_lease import (
    PostgresRepeatableReadSnapshotLease,
)
from dpone.runtime.state import XMinState
from dpone.runtime.streaming_rows import StreamingRowsArtifact


class PostgresXMinLegacyArtifactExtractor:
    """Choose file or streaming transport without changing snapshot ownership."""

    def __init__(self, strategy: Any) -> None:
        self._strategy = strategy

    def extract(
        self,
        *,
        load_config: Any,
        schema: list[tuple[str, str]],
        relation_schema: list[tuple[str, str]],
        previous_state: XMinState | None,
        snapshot_xmin: int,
        prev_low: int,
        curr_low: int,
        safe_state: XMinState,
        force_full_refresh: bool,
        snapshot_lease: PostgresRepeatableReadSnapshotLease,
    ) -> tuple[Any, list[tuple[str, str]], list[tuple[str, str]]]:
        """Build one artifact while the caller retains the exact RR lease."""

        strategy = self._strategy
        if force_full_refresh or strategy.xmin_manager.should_perform_full_refresh(safe_state):
            select_query = strategy.format_select_query(
                load_config.source_schema,
                load_config.source_table,
                [column for column, _dtype in schema],
                load_config.custom_predicate,
            )
            artifact = strategy._export_to_file(
                select_query,
                schema,
                load_config,
                batch_size=load_config.batch_size,
                snapshot_lease=snapshot_lease,
            )
            return artifact, schema, relation_schema

        query = strategy.xmin_manager.build_incremental_query(
            load_config.source_schema,
            load_config.source_table,
            previous_state,
            snapshot_xmin,
            columns=None,
            custom_predicate=load_config.custom_predicate,
        )
        delta_size_threshold = load_config.options.get("delta_size_threshold", 1_000_000)
        delta_row_count = self._estimate_delta_rows(
            load_config,
            prev_low=prev_low,
            curr_low=curr_low,
            threshold=delta_size_threshold,
        )
        schema = strategy._schema_with_meta_xmin(schema)
        relation_schema = strategy._schema_with_meta_xmin(relation_schema)
        if delta_row_count >= delta_size_threshold:
            strategy.logger.log_etl_progress(
                "INCREMENTAL_LARGE_DELTA_DETECTED",
                {
                    "Source": f"{load_config.source_schema}.{load_config.source_table}",
                    "Delta_Rows": delta_row_count,
                    "Threshold": delta_size_threshold,
                    "Mode": "File Export → GCS → BigQuery (Memory-safe)",
                    "Reason": "⚠️  Large delta detected, switching to file-based approach to prevent OOM",
                },
            )
            artifact = strategy._export_to_file(
                query,
                schema,
                load_config,
                batch_size=load_config.batch_size,
                snapshot_lease=snapshot_lease,
            )
        else:
            strategy.logger.log_etl_progress(
                "INCREMENTAL_STREAMING",
                {
                    "Source": f"{load_config.source_schema}.{load_config.source_table}",
                    "Delta_Rows": delta_row_count,
                    "Threshold": delta_size_threshold,
                    "Mode": "Streaming → Staging → Merge",
                    "Reason": "Small delta, using memory-efficient streaming",
                },
            )
            artifact = StreamingRowsArtifact(
                iterator=strategy.connector.get_records_iterator(query, params=None),
                extraction_lifecycle=snapshot_lease.lifecycle,
                on_success=strategy.connector.commit_transaction,
                on_abort=strategy.connector.rollback,
            )
        return artifact, schema, relation_schema

    def _estimate_delta_rows(
        self,
        load_config: Any,
        *,
        prev_low: int,
        curr_low: int,
        threshold: int,
    ) -> int:
        strategy = self._strategy
        try:
            count_query = sql.SQL(
                "SELECT COUNT(*) FROM {schema}.{table} AS t "
                "WHERE (t.xmin::text::bigint >= {prev_xmin} AND t.xmin::text::bigint < {curr_xmin})"
            ).format(
                schema=sql.Identifier(load_config.source_schema),
                table=sql.Identifier(load_config.source_table),
                prev_xmin=sql.Literal(prev_low),
                curr_xmin=sql.Literal(curr_low),
            )
            count_result = strategy.connector.get_records(count_query, params=None)
            row_count = int(count_result[0][0]) if count_result else 0
            strategy.logger.info(f"Delta size estimate: {row_count:,} rows (threshold: {threshold:,})")
            return row_count
        except Exception as exc:
            strategy.logger.warning(f"Failed to estimate delta size: {exc}. Falling back to streaming.")
            return 0


__all__ = ["PostgresXMinLegacyArtifactExtractor"]
