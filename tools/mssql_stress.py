#!/usr/bin/env python
"""Run local MSSQL bulk stress checks for dpone.

The harness intentionally exercises the public runtime pieces instead of
private shortcuts:

1. PostgreSQL synthetic source table is created with ``generate_series``.
2. The governed PostgreSQL snapshot source exports a bcp-friendly TSV artifact
   through native PostgreSQL ``COPY TO STDOUT``.
3. The standard ETL processor admits the load through the same external
   SQL Server fence/receipt catalog as production, then ``MSSQLSink`` loads the
   artifact through Microsoft ``bcp`` and finishes with set-based SQL.
4. ``MSSQLFullExtractStrategy`` exports SQL Server rows through ``bcp queryout``.
5. ``ClickHouseSink`` inserts file batches through the native ClickHouse driver.

The script is designed for local Docker infrastructure and CI runners with
real SQL endpoints. It does not require vendor API credentials.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from typing import Any

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.connectors import ClickHouseConnector, MSSQLConnector, PostgresConnector
from dpone.runtime.etl_logging.etl_logger import ETLLogger
from dpone.runtime.sinks import ClickHouseSink
from dpone.runtime.sinks.base import LoadPayload
from dpone.runtime.sources.strategies.mssql import MSSQLFullExtractStrategy
from dpone.strategy_intelligence.transfer_diagnostics import PhaseMetric, artifact_diagnostics, measure_phase
from mssql_stress_governance import (
    GovernedPostgresSnapshotSource,
    GovernedStandardEtlRunner,
    governed_mssql_campaign,
)

CURRENT_ARGS: argparse.Namespace | None = None


class StressLogger(ETLLogger):
    """Production ETL logger with compact stress-progress rendering.

    Inheriting the runtime logger keeps the benchmark aligned with the complete
    ETL lifecycle contract (`start`, `progress`, `error`, and `end`) while the
    small overrides retain line-oriented output for CI evidence.
    """

    def __init__(self) -> None:
        super().__init__(show_colors=False)

    def info(self, message: str, *args: Any, **_kwargs: Any) -> None:
        print(message % args if args else message)

    def warning(self, message: str, *args: Any, **_kwargs: Any) -> None:
        rendered = message % args if args else message
        print(f"WARNING: {rendered}")

    def log_etl_progress(self, event: str, payload: dict[str, Any]) -> None:
        print(json.dumps({"event": event, **payload}, default=str, ensure_ascii=False))


StageMetric = PhaseMetric


@dataclass(frozen=True)
class TransferStageResult:
    """Benchmark result for one source -> sink leg."""

    metric: StageMetric
    phases: list[StageMetric]
    artifact: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "metric": self.metric.to_dict(),
            "phases": [phase.to_dict() for phase in self.phases],
            "artifact": self.artifact,
        }


class MeasuredGovernedPostgresSnapshotSource(GovernedPostgresSnapshotSource):
    """Observe COPY throughput without bypassing standard ETL governance."""

    export_metric: StageMetric | None = None
    artifact: dict[str, Any] | None = None

    def extract(self, load_config: LoadConfig, last_state: Any | None) -> Any:
        rows = CURRENT_ARGS.rows if CURRENT_ARGS is not None else 0
        parent_extract = super().extract
        extracted, metric = measure_phase(
            "postgres_to_mssql.source_export",
            rows,
            lambda: parent_extract(load_config, last_state),
        )
        self.export_metric = metric
        self.artifact = artifact_diagnostics(extracted.artifact, phase_seconds=metric.seconds)
        return extracted


def timed_stage(name: str, rows: int, fn) -> StageMetric:
    _, metric = measure_phase(name, rows, fn)
    return metric


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="dpone MSSQL/Postgres/ClickHouse stress harness")
    parser.add_argument("--rows", type=int, default=250_000)
    parser.add_argument("--batch-size", type=int, default=100_000)
    parser.add_argument("--partition-column", default=None)
    parser.add_argument("--num-partitions", type=int, default=1)
    parser.add_argument("--lower-bound", type=int, default=1)
    parser.add_argument("--upper-bound", type=int, default=None)
    parser.add_argument("--export-workers", type=int, default=None)
    parser.add_argument("--load-workers", type=int, default=None)
    parser.add_argument("--partition-workers", type=int, default=None, help=argparse.SUPPRESS)
    parser.add_argument("--parallel-load-workers", type=int, default=None, help=argparse.SUPPRESS)
    parser.add_argument(
        "--optimizer-profile",
        choices=["high_throughput_safe"],
        default=None,
        help="Apply a safe native-transfer optimizer profile unless explicit settings override it.",
    )
    parser.add_argument("--slo-pg-mssql-rps", type=float, default=None)
    parser.add_argument("--slo-mssql-clickhouse-rps", type=float, default=None)
    parser.add_argument("--json-output", default=None)
    parser.add_argument("--pg-host", default="127.0.0.1")
    parser.add_argument("--pg-port", type=int, default=55432)
    parser.add_argument("--pg-database", default="dpone_it")
    parser.add_argument("--pg-user", default="dpone")
    parser.add_argument("--pg-password", default="dpone")
    parser.add_argument("--mssql-host", default="127.0.0.1")
    parser.add_argument("--mssql-port", type=int, default=51433)
    parser.add_argument("--mssql-database", default="dpone")
    parser.add_argument("--mssql-user", default="sa")
    parser.add_argument("--mssql-password", default="Dp0ne.Strong.Pw.2026!")
    parser.add_argument("--mssql-driver", default="ODBC Driver 18 for SQL Server")
    parser.add_argument("--bcp-path", default="bcp")
    parser.add_argument("--clickhouse-host", default="127.0.0.1")
    parser.add_argument("--clickhouse-port", type=int, default=59000)
    parser.add_argument("--clickhouse-database", default="dpone_it")
    parser.add_argument("--clickhouse-user", default="default")
    parser.add_argument("--clickhouse-password", default="dpone")
    parser.add_argument("--clickhouse-bulk-mode", default="auto", choices=["auto", "python", "client", "http"])
    parser.add_argument("--clickhouse-client-command", default=None)
    parser.add_argument("--clickhouse-client-host", default=None)
    parser.add_argument("--clickhouse-client-port", type=int, default=None)
    parser.add_argument("--clickhouse-http-host", default=None)
    parser.add_argument("--clickhouse-http-port", type=int, default=None)
    return parser.parse_args()


def main() -> int:
    global CURRENT_ARGS
    args = parse_args()
    CURRENT_ARGS = args
    logger = StressLogger()

    pg = PostgresConnector(
        host=args.pg_host,
        port=args.pg_port,
        database=args.pg_database,
        user=args.pg_user,
        password=args.pg_password,
        application_name="dpone-stress-pg",
    )
    mssql = MSSQLConnector(
        host=args.mssql_host,
        port=args.mssql_port,
        database=args.mssql_database,
        user=args.mssql_user,
        password=args.mssql_password,
        driver=args.mssql_driver,
        trust_server_certificate=True,
        application_name="dpone-stress-mssql",
        bcp_path=args.bcp_path,
        query_timeout=0,
    )
    clickhouse = ClickHouseConnector(
        host=args.clickhouse_host,
        port=args.clickhouse_port,
        database=args.clickhouse_database,
        user=args.clickhouse_user,
        password=args.clickhouse_password,
        application_name="dpone-stress-clickhouse",
        compression=True,
    )

    try:
        metrics: list[StageMetric] = []
        metrics.append(timed_stage("prepare_postgres_source", args.rows, lambda: prepare_postgres(pg, args.rows)))
        prepare_mssql(mssql)
        prepare_clickhouse(clickhouse, args.clickhouse_database)

        pg_to_mssql = run_postgres_to_mssql(pg, mssql, logger, args.batch_size)
        metrics.append(pg_to_mssql.metric)

        mssql_to_clickhouse = run_mssql_to_clickhouse(mssql, clickhouse, logger, args.batch_size)
        metrics.append(mssql_to_clickhouse.metric)

        mssql_count, mssql_count_metric = measure_phase(
            "mssql_count_reconciliation",
            args.rows,
            lambda: count_mssql(mssql, "dbo", "bench_orders"),
        )
        clickhouse_count, clickhouse_count_metric = measure_phase(
            "clickhouse_count_reconciliation",
            args.rows,
            lambda: count_clickhouse(clickhouse, args.clickhouse_database, "bench_orders_ch"),
        )

        result = {
            "rows": args.rows,
            "partitioning": partitioning_summary(args),
            "metrics": [metric.to_dict() for metric in metrics],
            "phase_metrics": [
                *(phase.to_dict() for phase in pg_to_mssql.phases),
                *(phase.to_dict() for phase in mssql_to_clickhouse.phases),
                mssql_count_metric.to_dict(),
                clickhouse_count_metric.to_dict(),
            ],
            "transfers": {
                "postgres_to_mssql_full_refresh": pg_to_mssql.to_dict(),
                "mssql_to_clickhouse_full_refresh": mssql_to_clickhouse.to_dict(),
            },
            "mssql_count": mssql_count,
            "clickhouse_count": clickhouse_count,
        }
        rendered = json.dumps(result, indent=2, ensure_ascii=False)
        print(rendered)
        if args.json_output:
            with open(args.json_output, "w", encoding="utf-8") as handle:
                handle.write(rendered + "\n")
        return evaluate_slos(args, metrics)
    finally:
        pg.close()
        mssql.close()


def prepare_postgres(connector: PostgresConnector, rows: int) -> None:
    connector.execute_query("DROP TABLE IF EXISTS public.bench_orders")
    connector.execute_query(
        f"""
        CREATE TABLE public.bench_orders AS
        SELECT
            gs::int AS id,
            (gs % 10000)::int AS customer_id,
            (timestamp '2026-01-01 00:00:00' + (gs || ' seconds')::interval)::timestamp AS updated_at,
            (gs::numeric / 100.0)::numeric(18,2) AS amount,
            ('order-' || gs::text) AS description
        FROM generate_series(1, {int(rows)}) AS gs
        """
    )
    connector.execute_query("ANALYZE public.bench_orders")


def prepare_mssql(connector: MSSQLConnector) -> None:
    connector.execute_query("DROP TABLE IF EXISTS [dbo].[bench_orders]")
    connector.execute_query("DROP TABLE IF EXISTS [staging].[bench_orders]")


def prepare_clickhouse(connector: ClickHouseConnector, database: str) -> None:
    connector.execute_query(f"CREATE DATABASE IF NOT EXISTS `{database}`")
    connector.execute_query(f"DROP TABLE IF EXISTS `{database}`.`bench_orders_ch`")


def create_mssql_benchmark_boundary(connector: MSSQLConnector) -> None:
    """Establish the indexed range-scan boundary after a committed load."""

    connector.execute_query("CREATE UNIQUE CLUSTERED INDEX [IX_dpone_bench_orders_id] ON [dbo].[bench_orders] ([id])")


def run_postgres_to_mssql(
    pg: PostgresConnector,
    mssql: MSSQLConnector,
    logger: StressLogger,
    batch_size: int,
) -> TransferStageResult:
    config = LoadConfig(
        source_conn_id="postgres_source",
        target_conn_id="mssql_sink",
        source_database=pg.database,
        target_database=mssql.database,
        source_schema="public",
        source_table="bench_orders",
        target_schema="dbo",
        target_table="bench_orders",
        staging_schema="staging",
        staging_database=mssql.database,
        load_strategy=LoadStrategy.FULL_REFRESH,
        batch_size=batch_size,
        export_format="csv",
        compress_export=False,
        options={
            "source_type": "postgres",
            "sink_type": "mssql",
            "batch_commit_mode": "whole",
            "technical_columns": "forbidden",
            "lineage": False,
            **mssql_bulk_options(),
            **partition_options(),
        },
    )
    expected_rows = CURRENT_ARGS.rows if CURRENT_ARGS is not None else 0
    with governed_mssql_campaign(mssql, target_database=mssql.database) as campaign:
        runner = GovernedStandardEtlRunner(
            campaign.route(target_schema=config.target_schema, target_table=config.target_table),
            pg,
            logger=logger,
            source_type=MeasuredGovernedPostgresSnapshotSource,
        )

        def execute_governed_load() -> dict[str, Any]:
            result = runner.run(config, label="mssql_stress_postgres_full_refresh")
            create_mssql_benchmark_boundary(mssql)
            return result

        result, total_metric = measure_phase(
            "postgres_to_mssql.governed_load",
            expected_rows,
            execute_governed_load,
        )

    export_metric = runner.source.export_metric
    diagnostics = runner.source.artifact
    if export_metric is None or diagnostics is None:
        raise RuntimeError("mssql_stress.governed_source_metrics_missing")
    rows = int(result.get("loaded_rows") or result.get("total_rows") or 0)
    load_seconds = max(total_metric.seconds - export_metric.seconds, 0.0)
    load_metric = StageMetric.from_elapsed(
        "postgres_to_mssql.target_load_finalize",
        rows,
        load_seconds,
    )
    return TransferStageResult(
        metric=StageMetric.from_elapsed("postgres_to_mssql_full_refresh", rows, total_metric.seconds),
        phases=[export_metric, load_metric],
        artifact=diagnostics,
    )


def run_mssql_to_clickhouse(
    mssql: MSSQLConnector,
    clickhouse: ClickHouseConnector,
    logger: StressLogger,
    batch_size: int,
) -> StageMetric:
    config = LoadConfig(
        source_conn_id="mssql_source",
        target_conn_id="clickhouse_sink",
        source_schema="dbo",
        source_table="bench_orders",
        target_schema=clickhouse.database,
        target_table="bench_orders_ch",
        staging_schema="staging",
        load_strategy=LoadStrategy.FULL_REFRESH,
        batch_size=batch_size,
        options={
            **mssql_bulk_options(),
            **clickhouse_bulk_options(),
            **partition_options(),
        },
    )
    source = MSSQLFullExtractStrategy(mssql, logger, sink_connector=clickhouse)
    sink = ClickHouseSink(clickhouse, logger=logger)

    extract, export_metric = measure_phase(
        "mssql_to_clickhouse.source_export",
        CURRENT_ARGS.rows if CURRENT_ARGS is not None else 0,
        lambda: source.extract(config, None),
    )
    diagnostics = artifact_diagnostics(extract.artifact, phase_seconds=export_metric.seconds)
    result, load_metric = measure_phase(
        "mssql_to_clickhouse.target_load_finalize",
        CURRENT_ARGS.rows if CURRENT_ARGS is not None else 0,
        lambda: sink.load(config, LoadPayload(artifact=extract.artifact, schema=extract.schema)),
    )
    rows = result.total_rows or result.inserted_rows
    seconds = export_metric.seconds + load_metric.seconds
    return TransferStageResult(
        metric=StageMetric.from_elapsed("mssql_to_clickhouse_full_refresh", rows, seconds),
        phases=[export_metric, load_metric],
        artifact=diagnostics,
    )


def count_mssql(connector: MSSQLConnector, schema: str, table: str) -> int:
    rows = connector.get_records(f"SELECT COUNT_BIG(*) FROM {connector.qualified_name(schema, table)}")
    return int(rows[0][0]) if rows else 0


def count_clickhouse(connector: ClickHouseConnector, schema: str, table: str) -> int:
    rows = connector.get_records(f"SELECT count() FROM `{schema}`.`{table}`")
    return int(rows[0][0]) if rows else 0


def partition_options() -> dict[str, Any]:
    if CURRENT_ARGS is None:
        raise RuntimeError("partition_options called before CLI arguments were parsed")
    args = CURRENT_ARGS
    if not args.partition_column or args.num_partitions <= 1:
        return {}
    export_workers = args.export_workers or args.partition_workers
    load_workers = args.load_workers or args.parallel_load_workers or export_workers
    options: dict[str, Any] = {
        "partitioning": {
            "strategy": "range",
            "column": args.partition_column,
            "bounds": {"lower": args.lower_bound, "upper": args.upper_bound or args.rows},
            "num_partitions": args.num_partitions,
        }
    }
    if export_workers:
        options["partitioning"]["export_workers"] = export_workers
    if load_workers:
        options["partitioning"]["load_workers"] = load_workers
    return options


def mssql_bulk_options() -> dict[str, Any]:
    if CURRENT_ARGS is None:
        raise RuntimeError("mssql_bulk_options called before CLI arguments were parsed")
    args = CURRENT_ARGS
    return {
        "bulk": {
            "mode": "bcp",
            "bcp": {
                "bcp_path": args.bcp_path,
                "batch_size": args.batch_size,
            },
        },
        **native_transfer_options(),
    }


def clickhouse_bulk_options() -> dict[str, Any]:
    if CURRENT_ARGS is None:
        raise RuntimeError("clickhouse_bulk_options called before CLI arguments were parsed")
    args = CURRENT_ARGS
    options: dict[str, Any] = {"clickhouse_bulk": {"mode": args.clickhouse_bulk_mode}, **native_transfer_options()}
    if args.clickhouse_client_command:
        options["clickhouse_bulk"].setdefault("client", {})["command"] = args.clickhouse_client_command
    if args.clickhouse_client_host:
        options["clickhouse_bulk"].setdefault("client", {})["host"] = args.clickhouse_client_host
    if args.clickhouse_client_port:
        options["clickhouse_bulk"].setdefault("client", {})["port"] = args.clickhouse_client_port
    if args.clickhouse_http_host:
        options["clickhouse_bulk"].setdefault("http", {})["host"] = args.clickhouse_http_host
    if args.clickhouse_http_port:
        options["clickhouse_bulk"].setdefault("http", {})["port"] = args.clickhouse_http_port
    return options


def native_transfer_options() -> dict[str, Any]:
    if CURRENT_ARGS is None:
        raise RuntimeError("native_transfer_options called before CLI arguments were parsed")
    profile = getattr(CURRENT_ARGS, "optimizer_profile", None)
    if not profile:
        return {}
    return {"native_transfer": {"optimizer_profile": str(profile)}}


def partitioning_summary(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "enabled": bool(args.partition_column and args.num_partitions > 1),
        "partition_column": args.partition_column,
        "lower_bound": args.lower_bound,
        "upper_bound": args.upper_bound or args.rows,
        "num_partitions": args.num_partitions,
        "export_workers": args.export_workers or args.partition_workers,
        "load_workers": args.load_workers
        or args.parallel_load_workers
        or args.export_workers
        or args.partition_workers,
    }


def evaluate_slos(args: argparse.Namespace, metrics: list[StageMetric]) -> int:
    thresholds = {
        "postgres_to_mssql_full_refresh": args.slo_pg_mssql_rps,
        "mssql_to_clickhouse_full_refresh": args.slo_mssql_clickhouse_rps,
    }
    failed: list[str] = []
    by_name = {metric.name: metric for metric in metrics}
    for name, threshold in thresholds.items():
        if threshold is None:
            continue
        metric = by_name[name]
        if metric.rows_per_second < threshold:
            failed.append(f"{name}: {metric.rows_per_second} rows/s < SLO {threshold} rows/s")
    if failed:
        print("SLO_FAILED " + "; ".join(failed), file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
