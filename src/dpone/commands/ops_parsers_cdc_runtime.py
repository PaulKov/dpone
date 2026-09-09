"""CDC runtime and materialization parser registration."""

from __future__ import annotations

import argparse

from dpone.readiness.cdc import CDCBackend

CREDENTIALS_SOURCE_CHOICES = ("env", "vault", "airflow", "params")


def register_cdc_runtime_run_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "cdc-runtime-run",
        help="Run one bounded CDC read, sink apply, and durable offset commit tick",
    )
    parser.add_argument("--output-dir", default=".dpone/cdc-runtime/latest")
    parser.add_argument("--mode", choices=["local", "live"], default="local")
    parser.add_argument("--source", required=True)
    parser.add_argument("--sink", required=True)
    parser.add_argument("--backend", required=True, choices=[item.value for item in CDCBackend])
    parser.add_argument("--pipeline-name", required=True)
    parser.add_argument("--source-schema", required=True)
    parser.add_argument("--source-table", required=True)
    parser.add_argument("--target-dataset", required=True)
    parser.add_argument("--unique-key", action="append", required=True, help="Unique key column; repeatable")
    parser.add_argument("--events-json", help="Path to bounded CDC events JSON for local mode")
    parser.add_argument("--checkpoint-json", help="Path to local durable checkpoint JSON for local mode")
    parser.add_argument("--source-connection-id", help="Source connection id for live mode")
    parser.add_argument("--sink-connection-id", help="Sink connection id for live mode")
    parser.add_argument("--credentials-source", choices=CREDENTIALS_SOURCE_CHOICES, default="env")
    parser.add_argument("--credentials-mount-point", help="Optional credentials mount point for live mode")
    parser.add_argument("--credentials-path", help="Optional credentials path for live mode")
    parser.add_argument("--state-schema", default="etl_state", help="SQL-backed CDC offset state schema for live mode")
    parser.add_argument(
        "--state-table", default="etl_cdc_offset", help="SQL-backed CDC offset state table for live mode"
    )
    parser.add_argument("--capture-instance", help="SQL Server CDC capture instance override")
    parser.add_argument(
        "--change-tracking-key",
        action="append",
        default=[],
        help="SQL Server Change Tracking primary key column; repeatable; defaults to --unique-key",
    )
    parser.add_argument("--max-changes", type=int, default=10000)
    parser.add_argument("--poison-mode", choices=["fail_closed", "quarantine_and_continue"], default="fail_closed")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_cdc_materialize_clickhouse_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "cdc-materialize-clickhouse",
        help="Materialize a ClickHouse append-only CDC log into a current-state serving table",
    )
    parser.add_argument("--output-dir", default=".dpone/cdc-materialization/latest")
    parser.add_argument("--cdc-dataset", required=True, help="ClickHouse CDC log dataset as table or database.table")
    parser.add_argument("--target-dataset", required=True, help="ClickHouse serving target as table or database.table")
    parser.add_argument("--unique-key", action="append", required=True, help="Unique key column; repeatable")
    parser.add_argument("--sink-connection-id", required=True, help="ClickHouse connection id")
    parser.add_argument("--credentials-source", choices=CREDENTIALS_SOURCE_CHOICES, default="env")
    parser.add_argument("--credentials-mount-point", help="Optional credentials mount point")
    parser.add_argument("--credentials-path", help="Optional credentials path")
    parser.add_argument("--delete-mode", choices=["exclude_deleted", "tombstone"], default="exclude_deleted")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_cdc_materialize_clickhouse_typed_parser(
    subparsers: argparse._SubParsersAction,
) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "cdc-materialize-clickhouse-typed",
        help="Materialize a ClickHouse append-only CDC log into a typed current-state serving table",
    )
    parser.add_argument("--output-dir", default=".dpone/cdc-typed-materialization/latest")
    parser.add_argument("--cdc-dataset", required=True, help="ClickHouse CDC log dataset as table or database.table")
    parser.add_argument("--target-dataset", required=True, help="Typed ClickHouse serving target")
    parser.add_argument("--unique-key", action="append", required=True, help="Unique key column; repeatable")
    parser.add_argument(
        "--column",
        action="append",
        required=True,
        help="Projected typed column as name=ClickHouseType; repeatable",
    )
    parser.add_argument("--sink-connection-id", required=True, help="ClickHouse connection id")
    parser.add_argument("--credentials-source", choices=CREDENTIALS_SOURCE_CHOICES, default="env")
    parser.add_argument("--credentials-mount-point", help="Optional credentials mount point")
    parser.add_argument("--credentials-path", help="Optional credentials path")
    parser.add_argument("--delete-mode", choices=["exclude_deleted", "tombstone"], default="exclude_deleted")
    parser.add_argument(
        "--fail-on-parse-errors",
        action="store_true",
        help="Fail closed when typed projection parse failures exceed --max-parse-error-ratio",
    )
    parser.add_argument("--max-parse-error-ratio", type=float, default=0.0)
    parser.add_argument("--quarantine-dataset", help="Optional ClickHouse table for typed parse quarantine rows")
    parser.add_argument("--schema-drift-mode", choices=["off", "warn_additive", "strict"], default="warn_additive")
    parser.add_argument("--quality-sample-limit", type=int, default=100)
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


__all__ = [
    "CREDENTIALS_SOURCE_CHOICES",
    "register_cdc_materialize_clickhouse_parser",
    "register_cdc_materialize_clickhouse_typed_parser",
    "register_cdc_runtime_run_parser",
]
