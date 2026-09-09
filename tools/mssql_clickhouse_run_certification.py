#!/usr/bin/env python3
"""Live ``dpone run`` certification for MSSQL -> ClickHouse native transfer.

The tool is intentionally a thin harness over public/runtime APIs:
- it prepares a disposable MSSQL source table and ClickHouse target table;
- it writes a normal dpone manifest with params credentials;
- it executes the manifest twice through ``dpone.api.run``;
- it verifies SQL-backed partition checkpoints and runtime evidence.
"""

from __future__ import annotations

import argparse
import json
import os
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from dpone.api import run as dpone_run
from dpone.runtime.lineage.partition_checkpoint import PartitionCheckpoint, PartitionCheckpointStatus
from dpone.runtime.lineage.partition_checkpoint_sql_store import (
    MSSQLCheckpointDialect,
    SqlPartitionCheckpointStore,
)
from dpone.services.run_manifest import RunManifestResult
from dpone.strategy_intelligence.run_certification import (
    NativeTransferRunCertificationRequest,
    NativeTransferRunCertificationResult,
    NativeTransferRunCertificationService,
    NativeTransferRunEvidenceWriter,
)

RunManifestCallback = Callable[[Path, str], RunManifestResult]


@dataclass(frozen=True, slots=True)
class LiveRunCertificationConfig:
    """Configuration for one disposable certification run."""

    rows: int
    source_schema: str
    source_table: str
    target_database: str
    target_table: str
    state_schema: str
    checkpoint_table: str
    evidence_dir: Path
    mssql_params: dict[str, Any]
    clickhouse_params: dict[str, Any]
    partition_column: str = "order_id"
    target_rows_per_partition: int = 5000
    export_workers: int = 2
    load_workers: int = 2
    bcp_batch_size: int = 10000
    bcp_packet_size: int = 65535
    optimizer_profile: str = "high_throughput_safe"
    strategy_mode: str = "incremental_append"
    unique_key: list[str] = field(default_factory=lambda: ["order_id"])

    @property
    def runtime_report_dir(self) -> Path:
        return self.evidence_dir / "runtime"


def write_manifest(path: Path, config: LiveRunCertificationConfig) -> Path:
    """Write the normal user-facing dpone manifest used by certification."""

    source_connection_id = _json_connection_id(config.mssql_params)
    manifest = {
        "name": "mssql_to_clickhouse_native_run_cert",
        "runtime": {
            "compatibility": {
                "legacy_runtime_connections": "explicit_only",
            }
        },
        "source": {
            "type": "mssql",
            "connection_type": "params",
            "connection_id": source_connection_id,
            "table": {"schema": config.source_schema, "name": config.source_table},
            "options": {
                "native_transfer": {"optimizer_profile": config.optimizer_profile},
                "extract_mode": "bcp_queryout",
                "mssql_export_mode": "bcp",
                "bulk": {
                    "mode": "bcp",
                    "bcp": {
                        "batch_size": config.bcp_batch_size,
                        "packet_size": config.bcp_packet_size,
                        "timeout_seconds": 3600,
                    },
                },
                "partitioning": {
                    "strategy": "auto",
                    "column": config.partition_column,
                    "bounds": "auto",
                    "target_rows_per_partition": config.target_rows_per_partition,
                    "max_partitions": 256,
                    "export_workers": config.export_workers,
                    "load_workers": config.load_workers,
                },
            },
        },
        "sink": {
            "type": "clickhouse",
            "connection_type": "params",
            "connection_id": _json_connection_id(config.clickhouse_params),
            "table": {"schema": config.target_database, "name": config.target_table},
            "strategy": {
                "mode": config.strategy_mode,
                "unique_key": config.unique_key,
            },
            "options": {
                "native_transfer": {
                    "optimizer_profile": config.optimizer_profile,
                    "require_artifact_checksum": True,
                },
                "runtime_evidence": {
                    "output_dir": str(config.runtime_report_dir),
                },
                "clickhouse_bulk": {
                    "mode": "http",
                    "http": {
                        "host": config.clickhouse_params.get("http_host") or config.clickhouse_params.get("host"),
                        "port": int(config.clickhouse_params.get("http_port", 8123)),
                        "database": config.target_database,
                        "user": config.clickhouse_params.get("username")
                        or config.clickhouse_params.get("user", "default"),
                        "password": config.clickhouse_params.get("password", ""),
                    },
                    "insert_settings": {
                        "async_insert": 1,
                        "wait_for_async_insert": 1,
                        "max_insert_block_size": 1_000_000,
                        "input_format_parallel_parsing": 1,
                    },
                },
            },
        },
        "state": {
            "type": "mssql",
            "connection_type": "params",
            "connection_id": source_connection_id,
            "vault_mount_point": "local-params-explicit",
            "table": {"schema": config.state_schema},
            "partition_checkpoint_table": {
                "schema": config.state_schema,
                "name": config.checkpoint_table,
            },
        },
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(manifest, sort_keys=False, allow_unicode=True), encoding="utf-8")
    return path


def certify_dpone_run(
    *,
    manifest_path: Path,
    evidence_dir: Path,
    runtime_report_dir: Path,
    checkpoint_store: Any,
    run_once: Callable[..., RunManifestResult] | None = None,
) -> NativeTransferRunCertificationResult:
    """Run the certification service around two public ``dpone run`` executions."""

    callback = run_once or _default_run_once

    def _run(run_id: str) -> RunManifestResult:
        return callback(manifest_path, run_id=run_id)

    result = NativeTransferRunCertificationService().certify(
        NativeTransferRunCertificationRequest(
            manifest_path=manifest_path,
            run_once=_run,
            runtime_report_dir=runtime_report_dir,
            checkpoint_store=checkpoint_store,
        )
    )
    NativeTransferRunEvidenceWriter(evidence_dir).write(result)
    return result


def run_live_certification(config: LiveRunCertificationConfig) -> NativeTransferRunCertificationResult:
    """Prepare live fixtures, execute dpone twice, and write certification evidence."""

    config.evidence_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = write_manifest(config.evidence_dir / "mssql_to_clickhouse_native_run_cert.yml", config)
    mssql = _mssql_connector(config)
    clickhouse = _clickhouse_connector(config)
    checkpoint_store = SqlPartitionCheckpointStore(
        connector=mssql,
        dialect=MSSQLCheckpointDialect(),
        schema=config.state_schema,
        table=config.checkpoint_table,
    )
    try:
        _prepare_mssql_source(mssql, config)
        _prepare_clickhouse_target(clickhouse, config)
        return certify_dpone_run(
            manifest_path=manifest_path,
            evidence_dir=config.evidence_dir,
            runtime_report_dir=config.runtime_report_dir,
            checkpoint_store=checkpoint_store,
        )
    finally:
        _close_quietly(clickhouse)
        _close_quietly(mssql)


def build_config(args: argparse.Namespace) -> LiveRunCertificationConfig:
    suffix = args.suffix or uuid.uuid4().hex[:8]
    evidence_dir = Path(args.output_dir).expanduser().resolve()
    source_schema = args.source_schema or f"nt_run_{suffix}"
    source_table = args.source_table or "orders"
    target_database = args.clickhouse_database
    target_table = args.target_table or f"nt_run_orders_{suffix}"
    checkpoint_table = args.checkpoint_table or f"dpone_partition_checkpoints_{suffix}"
    return LiveRunCertificationConfig(
        rows=args.rows,
        source_schema=source_schema,
        source_table=source_table,
        target_database=target_database,
        target_table=target_table,
        state_schema=args.state_schema,
        checkpoint_table=checkpoint_table,
        evidence_dir=evidence_dir,
        mssql_params=_mssql_params_from_env(args),
        clickhouse_params=_clickhouse_params_from_env(args),
        target_rows_per_partition=args.target_rows_per_partition,
        export_workers=args.export_workers,
        load_workers=args.load_workers,
        bcp_batch_size=args.bcp_batch_size,
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=1000)
    parser.add_argument("--output-dir", default="test_artifacts/live_certification/benchmarks/native_dpone_run_latest")
    parser.add_argument("--suffix")
    parser.add_argument("--source-schema")
    parser.add_argument("--source-table")
    parser.add_argument("--target-table")
    parser.add_argument("--state-schema", default="etl_state")
    parser.add_argument("--checkpoint-table")
    parser.add_argument("--target-rows-per-partition", type=int, default=5000)
    parser.add_argument("--export-workers", type=int, default=2)
    parser.add_argument("--load-workers", type=int, default=2)
    parser.add_argument("--bcp-batch-size", type=int, default=10000)
    parser.add_argument("--mssql-host", default=os.getenv("DPONE_IT_MSSQL_HOST", "127.0.0.1"))
    parser.add_argument("--mssql-port", type=int, default=int(os.getenv("DPONE_IT_MSSQL_PORT", "1433")))
    parser.add_argument("--mssql-database", default=os.getenv("DPONE_IT_MSSQL_DATABASE", "master"))
    parser.add_argument("--mssql-user", default=os.getenv("DPONE_IT_MSSQL_USER", "sa"))
    parser.add_argument("--mssql-password", default=os.getenv("DPONE_IT_MSSQL_PASSWORD", ""))
    parser.add_argument("--mssql-driver", default=os.getenv("DPONE_IT_MSSQL_DRIVER", "ODBC Driver 18 for SQL Server"))
    parser.add_argument("--mssql-bcp-path", default=os.getenv("DPONE_IT_MSSQL_BCP_PATH", "bcp"))
    parser.add_argument("--clickhouse-host", default=os.getenv("DPONE_IT_CH_HOST", "127.0.0.1"))
    parser.add_argument("--clickhouse-port", type=int, default=int(os.getenv("DPONE_IT_CH_PORT", "9000")))
    parser.add_argument("--clickhouse-http-port", type=int, default=int(os.getenv("DPONE_IT_CH_HTTP_PORT", "8123")))
    parser.add_argument("--clickhouse-database", default=os.getenv("DPONE_IT_CH_DATABASE", "default"))
    parser.add_argument("--clickhouse-user", default=os.getenv("DPONE_IT_CH_USER", "default"))
    parser.add_argument("--clickhouse-password", default=os.getenv("DPONE_IT_CH_PASSWORD", ""))
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    config = build_config(parse_args(argv))
    result = run_live_certification(config)
    print(result.to_markdown())
    return 0 if result.passed else 1


def _default_run_once(manifest_path: Path, *, run_id: str) -> RunManifestResult:
    return dpone_run(manifest_path, run_id=run_id)


def _mssql_params_from_env(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "host": args.mssql_host,
        "port": args.mssql_port,
        "database": args.mssql_database,
        "username": args.mssql_user,
        "password": args.mssql_password,
        "driver": args.mssql_driver,
        "trust_server_certificate": "yes",
        "encrypt": "yes",
        "bcp_path": args.mssql_bcp_path,
    }


def _clickhouse_params_from_env(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "host": args.clickhouse_host,
        "port": args.clickhouse_port,
        "http_host": args.clickhouse_host,
        "http_port": args.clickhouse_http_port,
        "database": args.clickhouse_database,
        "username": args.clickhouse_user,
        "user": args.clickhouse_user,
        "password": args.clickhouse_password,
        "secure": False,
        "compression": True,
    }


def _json_connection_id(params: dict[str, Any]) -> str:
    return json.dumps(params, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _mssql_connector(config: LiveRunCertificationConfig):
    from dpone.runtime.connectors.mssql import MSSQLConnector

    params = config.mssql_params
    return MSSQLConnector(
        host=str(params["host"]),
        port=int(params.get("port") or 1433),
        database=str(params.get("database") or "master"),
        user=params.get("username"),
        password=params.get("password"),
        driver=str(params.get("driver") or "ODBC Driver 18 for SQL Server"),
        trust_server_certificate=params.get("trust_server_certificate", "yes"),
        encrypt=params.get("encrypt", "yes"),
        bcp_path=str(params.get("bcp_path") or "bcp"),
    )


def _clickhouse_connector(config: LiveRunCertificationConfig):
    from dpone.runtime.connectors.clickhouse import ClickHouseConnector

    params = config.clickhouse_params
    return ClickHouseConnector(
        host=str(params.get("host") or "127.0.0.1"),
        port=int(params.get("port") or 9000),
        database=str(params.get("database") or config.target_database),
        user=str(params.get("username") or params.get("user") or "default"),
        password=str(params.get("password") or ""),
        secure=bool(params.get("secure", False)),
        compression=bool(params.get("compression", True)),
    )


def _prepare_mssql_source(connector: Any, config: LiveRunCertificationConfig) -> None:
    schema = _mssql_ident(config.source_schema)
    table = _mssql_ident(config.source_table)
    connector.execute_query(
        f"IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'{config.source_schema}') EXEC(N'CREATE SCHEMA {schema}')"
    )
    connector.execute_query(f"DROP TABLE IF EXISTS {schema}.{table}")
    connector.execute_query(
        f"""
        CREATE TABLE {schema}.{table} (
            [order_id] int NOT NULL PRIMARY KEY,
            [status] nvarchar(40) NULL,
            [amount] decimal(18, 2) NULL,
            [note] nvarchar(200) NULL,
            [updated_at] datetime2 NULL
        )
        """
    )
    for start in range(1, config.rows + 1, 1000):
        rows_sql = []
        for order_id in range(start, min(start + 1000, config.rows + 1)):
            status = "paid" if order_id % 3 else "new"
            amount = "NULL" if order_id % 11 == 0 else f"{order_id % 1000}.{order_id % 100:02d}"
            note = "NULL" if order_id % 13 == 0 else ("N''" if order_id % 7 == 0 else f"N'note-{order_id}'")
            rows_sql.append(
                f"({order_id}, N'{status}', {amount}, {note}, DATEADD(second, {order_id}, '2026-01-01T00:00:00'))"
            )
        connector.execute_query(
            f"INSERT INTO {schema}.{table} ([order_id], [status], [amount], [note], [updated_at]) VALUES "
            + ", ".join(rows_sql)
        )


def _prepare_clickhouse_target(connector: Any, config: LiveRunCertificationConfig) -> None:
    database = _ch_ident(config.target_database)
    table = _ch_ident(config.target_table)
    connector.execute_query(f"CREATE DATABASE IF NOT EXISTS {database}")
    connector.execute_query(f"DROP TABLE IF EXISTS {database}.{table}")


def _mssql_ident(value: str) -> str:
    return "[" + value.replace("]", "]]") + "]"


def _ch_ident(value: str) -> str:
    return "`" + value.replace("`", "``") + "`"


def _close_quietly(connector: Any) -> None:
    close = getattr(connector, "close", None)
    if callable(close):
        close()


def _checkpoint(index: int, status: str = "committed") -> PartitionCheckpoint:
    """Small test helper kept local to the tool contract tests."""

    return PartitionCheckpoint(
        transfer_partition_id=f"partition-{index}",
        status=PartitionCheckpointStatus(status),
        query_hash="query",
        schema_hash="schema",
        source_table="dbo.orders",
        target_table="analytics.orders",
        partition_bounds={"index": index},
    )


if __name__ == "__main__":
    raise SystemExit(main())
