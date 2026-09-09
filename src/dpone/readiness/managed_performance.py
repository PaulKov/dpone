"""Performance recommendation service for managed UX."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone.config.load_strategy import LoadStrategy
from dpone.readiness.managed_models import PerformanceRecommendation
from dpone.readiness.managed_planning import ExecutionPlanService


class PerformanceAdvisor:
    """Generates actionable performance recommendations from manifest/runtime hints."""

    def advise(
        self,
        *,
        source_type: str,
        sink_type: str,
        strategy: str,
        options: Mapping[str, Any] | None = None,
    ) -> list[PerformanceRecommendation]:
        opts = dict(options or {})
        estimated = int(opts.get("estimated_rows") or 0)
        recs: list[PerformanceRecommendation] = []
        runtime_storage = opts.get("runtime_storage") or {}
        for warning in runtime_storage.get("warnings", ()) if isinstance(runtime_storage, Mapping) else ():
            recs.append(
                PerformanceRecommendation(
                    str(warning),
                    "medium",
                    "prevents worker-local disk failures",
                    "Runtime storage is falling back to an implicit or legacy work directory.",
                    "Set runtime.storage.work_dir to a mounted path with explicit min_free_bytes.",
                )
            )
        native_execution = opts.get("native_transfer_execution") or {}
        resource_policy = native_execution.get("resource_policy") if isinstance(native_execution, Mapping) else {}
        if isinstance(resource_policy, Mapping) and resource_policy.get("adaptive_sizing"):
            recs.append(
                PerformanceRecommendation(
                    "adaptive_native_transfer_sizing",
                    "medium",
                    "keeps file artifacts within worker disk budget",
                    "Native transfer can slice partitions by bytes and clean files after staging load.",
                    "Tune max_active_bytes, target_file_bytes, max_file_bytes, and disk_headroom_pct.",
                )
            )
        for item in opts.get("source_impact", ()) or ():
            if not isinstance(item, Mapping):
                continue
            recs.append(
                PerformanceRecommendation(
                    str(item.get("code")),
                    str(item.get("severity") or "medium"),
                    "reduces source-side scan pressure",
                    str(item.get("message")),
                    str(item.get("action")),
                )
            )
        if source_type == "postgres" and sink_type == "mssql":
            recs.append(
                PerformanceRecommendation(
                    "postgres_to_mssql_bcp",
                    "high",
                    "large speedup for 1M+ rows",
                    "Use PostgreSQL COPY export with MSSQL-friendly TSV and bcp import into staging.",
                    "Set source.options.export_format=mssql-delimited and sink.options.bulk.mode=bcp.",
                )
            )
            recs.append(
                PerformanceRecommendation(
                    "mssql_bulk_target_finalize_tuning",
                    "medium",
                    "medium-to-high impact on 1M+ row loads",
                    (
                        "Postgres -> MSSQL benchmarks usually bottleneck on SQL Server bcp load/finalize, "
                        "not PostgreSQL COPY."
                    ),
                    (
                        "Review bulk.bcp.batch_size, bulk.bcp.table_lock, SQL Server log throughput, "
                        "staging indexes, finalizer policy, and post-load statistics."
                    ),
                )
            )
        if source_type == "mssql" and sink_type == "clickhouse":
            bulk_wire = opts.get("native_transfer_bulk_wire") or {}
            native_format = str(bulk_wire.get("input_format") or "").lower() == "native"
            recs.append(
                PerformanceRecommendation(
                    "mssql_to_clickhouse_typed_wire",
                    "high",
                    "avoids Python row parsing and expensive MSSQL-side text escaping",
                    (
                        "Use bcp native typed binary wire with ClickHouse Native staging."
                        if native_format
                        else "Use typed binary wire or a certified typed raw route instead of source-escaped TSV."
                    ),
                    (
                        "Set wire.mode=typed_binary, source_native_format=bcp_native, "
                        "binary_format=native, and clickhouse_bulk.ingest_contract=typed_binary_staging."
                    ),
                )
            )
        if estimated >= 1_000_000 and "partition_column" not in opts:
            recs.append(
                PerformanceRecommendation(
                    "parallel_partitioning",
                    "medium",
                    "parallel scans reduce wall-clock time on large tables",
                    "Use JDBC/Spark-like partitioning with partition_column, lower_bound, upper_bound, num_partitions.",
                    "Add source.options.partition_column and explicit bounds.",
                )
            )
        if sink_type == "kafka":
            recs.append(
                PerformanceRecommendation(
                    "kafka_delivery_tuning",
                    "medium",
                    "improves producer throughput",
                    "Tune linger_ms, batch_num_messages, compression_type, and idempotence for delivery needs.",
                    "Use compression_type=zstd and linger_ms=20 for high-throughput batch loads.",
                )
            )
        if strategy == LoadStrategy.INCREMENTAL_MERGE.value and sink_type == "clickhouse":
            recs.append(
                PerformanceRecommendation(
                    "clickhouse_replace_shadow",
                    "high",
                    "avoids blocking target mutations",
                    "Use replace/shadow-table strategy instead of ALTER UPDATE mutations for ClickHouse.",
                    "Prefer replace with custom_predicate or append-only events with versioned collapsing downstream.",
                )
            )
        if not recs:
            recs.append(
                PerformanceRecommendation(
                    "baseline_batching",
                    "low",
                    "keeps resource use predictable",
                    "Use explicit batch_size and observe rows/sec in run artifacts.",
                    "Set source.options.batch_size according to source memory and target bulk path.",
                )
            )
        return recs

    def advise_manifest(self, path: str | Path, *, selector: str | None = None) -> list[PerformanceRecommendation]:
        plan = ExecutionPlanService().plan_manifest(path, selector=selector)
        return self.advise(
            source_type=str(plan["source"]["type"]),
            sink_type=str(plan["sink"]["type"]),
            strategy=str(plan["strategy"]["mode"]),
            options={
                "estimated_rows": plan.get("estimated_rows"),
                "runtime_storage": plan.get("runtime_storage"),
                "native_transfer_execution": plan.get("native_transfer_execution"),
                "native_transfer_bulk_wire": plan.get("native_transfer_bulk_wire"),
                "source_impact": plan.get("source_impact"),
                **dict(plan.get("partitioning") or {}),
            },
        )


__all__ = ["PerformanceAdvisor"]
