from __future__ import annotations

from dataclasses import dataclass

from dpone.runtime.sinks.merge_policy import DEFAULT_MERGE_POLICY_BY_SINK
from dpone.strategy_intelligence.clickhouse_mssql_policy import resolve_clickhouse_mssql_auto_strategy
from dpone.strategy_intelligence.models import AdaptiveBatchingPlan, StrategyDecision, StrategySignal
from dpone.strategy_intelligence.native_paths import NativeFastPathCatalog
from dpone.strategy_intelligence.native_transfer import NativeTransferPlanBuilder, NativeTransferRequest

_DB_SINKS = {"mssql", "postgres", "clickhouse", "bigquery"}
_PARTITION_REPLACE_SINKS = {"mssql", "postgres", "clickhouse", "bigquery"}


@dataclass(frozen=True, slots=True)
class StrategyContext:
    source_type: str
    sink_type: str
    requested_mode: str = "auto"
    requested_merge_policy: str = "auto"
    unique_key: tuple[str, ...] = ()
    estimated_rows: int | None = None
    changed_percent: float | None = None
    delete_percent: float | None = None
    partition_column: str | None = None
    cdc_available: bool = False
    source_cursor: str | None = None
    source_table: str = ""
    target_table: str = ""
    source_options: dict | None = None
    sink_options: dict | None = None


class StrategyAdvisor:
    """Select explainable load strategies from manifest/runtime hints."""

    def __init__(
        self,
        native_paths: NativeFastPathCatalog | None = None,
        native_transfer_builder: NativeTransferPlanBuilder | None = None,
    ) -> None:
        self._native_paths = native_paths or NativeFastPathCatalog()
        self._native_transfer_builder = native_transfer_builder or NativeTransferPlanBuilder()

    def advise(self, context: StrategyContext) -> StrategyDecision:
        source = _normalize(context.source_type)
        sink = _normalize(context.sink_type)
        requested = _normalize_mode(context.requested_mode)
        reasons: list[StrategySignal] = []
        warnings: list[StrategySignal] = []

        strategy = self._resolve_strategy(context, source, sink, requested, reasons, warnings)
        self._source_contract_warnings(context, source, sink, warnings)
        merge_policy = self._merge_policy(context, strategy, sink)
        safety_gates = self._safety_gates(context, strategy, sink)
        batching = self._adaptive_batching(context, sink)
        native_transfer_plan = self._native_transfer_plan(context, source, sink, strategy, merge_policy)
        fast_path = self._native_paths.resolve(source, sink)
        native_fast_path = (native_transfer_plan or {}).get("fast_path_id") or fast_path.path_id

        return StrategyDecision(
            source_type=source,
            sink_type=sink,
            requested_mode=requested,
            strategy_mode=strategy,
            merge_policy=merge_policy,
            native_fast_path=native_fast_path,
            adaptive_batching=batching,
            reasons=tuple(reasons),
            safety_gates=tuple(safety_gates),
            warnings=tuple(warnings),
            native_transfer_plan=native_transfer_plan,
        )

    @staticmethod
    def _merge_policy(context: StrategyContext, strategy: str, sink: str) -> str | None:
        """Preserve an authored merge protocol instead of replacing it with a sink default."""

        if strategy not in {"incremental_merge", "partition_replace"}:
            return None
        requested = _normalize_merge_policy(context.requested_merge_policy)
        if requested != "auto":
            return requested
        return DEFAULT_MERGE_POLICY_BY_SINK.get(sink)

    def _source_contract_warnings(
        self,
        context: StrategyContext,
        source: str,
        sink: str,
        warnings: list[StrategySignal],
    ) -> None:
        if source in {"rest", "api", "kafka"}:
            warnings.append(
                StrategySignal(
                    code="dirty_source_contract_recommended",
                    severity="medium",
                    message="Semi-structured/event sources should use schema_contract enforcement and quarantine.",
                    action="Configure schema_contract.enforcement=quarantine for dirty feeds or strict for certified feeds.",
                )
            )
        if sink in _DB_SINKS and _is_event_source(context) and not _has_event_boundary(context):
            warnings.append(
                StrategySignal(
                    code="event_boundary_required",
                    severity="error",
                    message=(
                        "Event-log loads need source_cursor, partition_column, or unique_key before production scheduling."
                    ),
                    action=(
                        "Declare source_cursor for incremental_append or partition_column/date window for partition_replace."
                    ),
                )
            )

    def _resolve_strategy(
        self,
        context: StrategyContext,
        source: str,
        sink: str,
        requested: str,
        reasons: list[StrategySignal],
        warnings: list[StrategySignal],
    ) -> str:
        if requested != "auto":
            if requested == "partition_replace" and sink not in _PARTITION_REPLACE_SINKS:
                warnings.append(
                    StrategySignal(
                        code="partition_replace_not_supported",
                        severity="error",
                        message=f"partition_replace is not supported for sink.type={sink}",
                        action="Use incremental_merge for event-log sinks such as Kafka.",
                    )
                )
                return "incremental_merge"
            reasons.append(
                StrategySignal(
                    code="explicit_strategy",
                    severity="info",
                    message=f"Using explicitly requested strategy {requested}.",
                )
            )
            return requested

        if source == "clickhouse" and sink == "mssql":
            return resolve_clickhouse_mssql_auto_strategy(context, reasons, warnings)

        if context.cdc_available and context.unique_key:
            reasons.append(
                StrategySignal(
                    code="cdc_available",
                    severity="high",
                    message="CDC is available and unique_key is configured, so apply change events instead of snapshot diff.",
                )
            )
            return "cdc_apply"

        if sink in _PARTITION_REPLACE_SINKS and context.partition_column and _is_large_partition_delta(context):
            reasons.append(
                StrategySignal(
                    code="partition_replace_large_delta",
                    severity="high",
                    message="Large partitioned delta should replace affected partitions instead of row-wise merge.",
                    action="Use staging table and partition_replace finalization.",
                )
            )
            return "partition_replace"

        if context.unique_key and sink in _DB_SINKS | {"kafka"}:
            reasons.append(
                StrategySignal(
                    code="unique_key_upsert",
                    severity="medium",
                    message="unique_key is configured, so incremental_merge is the safest current-state strategy.",
                )
            )
            return "incremental_merge"

        if context.source_cursor:
            reasons.append(
                StrategySignal(
                    code="cursor_append",
                    severity="medium",
                    message="Cursor is configured but no unique_key is available, so append-only incremental load is safest.",
                )
            )
            return "incremental_append"

        reasons.append(
            StrategySignal(
                code="portable_full_refresh",
                severity="low",
                message="No cursor, CDC, partition, or unique_key hints were found; full_refresh is deterministic.",
            )
        )
        return "full_refresh"

    def _safety_gates(self, context: StrategyContext, strategy: str, sink: str) -> list[StrategySignal]:
        gates = [
            StrategySignal(
                code="staging_first_required",
                severity="high",
                message="Load data into staging before final target mutation.",
            )
        ]
        if strategy in {"incremental_merge", "snapshot_diff", "cdc_apply", "scd2"} and not context.unique_key:
            gates.append(
                StrategySignal(
                    code="unique_key_required",
                    severity="error",
                    message=f"{strategy} requires unique_key for deterministic finalization.",
                )
            )
        if (context.delete_percent or 0.0) > 0:
            gates.append(
                StrategySignal(
                    code="delete_semantics",
                    severity="high",
                    message="Deletes are expected; finalizer must apply configured hard/soft delete policy before state commit.",
                )
            )
        if sink == "clickhouse" and strategy == "incremental_merge":
            gates.append(
                StrategySignal(
                    code="clickhouse_lightweight_delete",
                    severity="medium",
                    message="ClickHouse incremental_merge uses lightweight delete + insert by default.",
                )
            )
        return gates

    def _adaptive_batching(self, context: StrategyContext, sink: str) -> AdaptiveBatchingPlan:
        estimated = context.estimated_rows or 0
        enabled = estimated >= 1_000_000 or sink in {"mssql", "clickhouse", "kafka"}
        if estimated >= 10_000_000:
            initial = 100_000
            workers = 4 if context.partition_column else 2
        elif estimated >= 1_000_000:
            initial = 50_000
            workers = 2 if context.partition_column else 1
        else:
            initial = 10_000
            workers = 1
        return AdaptiveBatchingPlan(
            enabled=enabled,
            initial_batch_size=initial,
            min_batch_size=5_000,
            max_batch_size=250_000,
            parallel_workers=workers,
            tuning_metric="rows_per_second_and_target_backpressure",
        )

    def _native_transfer_plan(
        self,
        context: StrategyContext,
        source: str,
        sink: str,
        strategy: str,
        merge_policy: str | None,
    ) -> dict | None:
        if not (source == "mssql" and sink == "clickhouse"):
            return None
        source_options = dict(context.source_options or {})
        sink_options = dict(context.sink_options or {})
        if merge_policy and "merge_policy" not in sink_options:
            sink_options["merge_policy"] = merge_policy
        plan = self._native_transfer_builder.build(
            NativeTransferRequest(
                source_type=source,
                sink_type=sink,
                source_table=context.source_table,
                target_table=context.target_table,
                strategy=strategy,
                unique_key=context.unique_key,
                source_options=source_options,
                sink_options=sink_options,
            )
        )
        return plan.to_dict()


def _is_large_partition_delta(context: StrategyContext) -> bool:
    estimated = context.estimated_rows or 0
    changed = context.changed_percent or 0.0
    return estimated >= 1_000_000 and changed >= 0.05


def _is_event_source(context: StrategyContext) -> bool:
    options = dict(context.source_options or {})
    return str(options.get("table_kind") or options.get("source_kind") or "").strip().lower() in {
        "event",
        "events",
        "event_log",
        "append_only_event",
    } or bool(options.get("event_table"))


def _has_event_boundary(context: StrategyContext) -> bool:
    return bool(context.source_cursor or context.partition_column or context.unique_key)


def _normalize(value: str) -> str:
    aliases = {"sqlserver": "mssql", "sql_server": "mssql", "bq": "bigquery", "api": "rest"}
    normalized = str(value).strip().lower().replace("-", "_")
    return aliases.get(normalized, normalized)


def _normalize_mode(value: str) -> str:
    normalized = str(value or "auto").strip().lower().replace("-", "_")
    return {"default": "auto"}.get(normalized, normalized)


def _normalize_merge_policy(value: str) -> str:
    normalized = str(value or "auto").strip().lower().replace("-", "_")
    return {
        "default": "auto",
        "update+insert": "update_insert",
        "delete+insert": "delete_insert",
        "delete_insert_merge": "delete_insert",
        "exchange": "shadow_swap",
        "upsert_events": "event_upsert",
    }.get(normalized, normalized)
