from __future__ import annotations

import argparse

from dpone.readiness.cdc import CDCBackend

from .ops_parsers_artifacts import CREDENTIALS_SOURCE_CHOICES


def register_cdc_retention_check_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "cdc-retention-check",
        help="Check whether a committed CDC offset is still inside source retention bounds",
    )
    _add_stream_args(parser)
    parser.add_argument("--output-dir", default=".dpone/cdc-retention/latest")
    parser.add_argument("--mode", choices=["local", "live"], default="local")
    parser.add_argument("--committed-offset", help="Last durable CDC offset token")
    parser.add_argument("--min-available-offset", help="Local mode source retention lower bound")
    parser.add_argument("--high-watermark", help="Local mode source current high watermark")
    parser.add_argument("--current-offset", help="Local mode source current offset; defaults to high watermark")
    parser.add_argument("--retention-seconds", type=int, help="Optional local mode retention duration")
    parser.add_argument("--source-connection-id", help="Source connection id for live mode")
    parser.add_argument("--credentials-source", choices=CREDENTIALS_SOURCE_CHOICES, default="env")
    parser.add_argument("--credentials-mount-point", help="Optional credentials mount point")
    parser.add_argument("--credentials-path", help="Optional credentials path")
    parser.add_argument("--capture-instance", help="SQL Server CDC capture instance for mssql_cdc")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_cdc_resync_plan_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "cdc-resync-plan",
        help="Build a bounded CDC resync plan from retention-gap evidence and snapshot rows",
    )
    _add_stream_args(parser)
    parser.add_argument("--output-dir", default=".dpone/cdc-resync-plan/latest")
    parser.add_argument("--retention-report-json", required=True, help="Path to cdc_retention_check.json")
    parser.add_argument("--rows-json", required=True, help="Snapshot rows JSON list or object with rows")
    parser.add_argument("--max-rows", type=int, help="Maximum snapshot rows to include in the plan")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_cdc_resync_execute_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "cdc-resync-execute",
        help="Execute a CDC resync plan through an idempotent sink applier without committing offsets",
    )
    _add_stream_args(parser)
    parser.add_argument("--output-dir", default=".dpone/cdc-resync-execute/latest")
    parser.add_argument("--mode", choices=["local", "live"], default="local")
    parser.add_argument("--resync-plan-json", required=True, help="Path to cdc_resync_actions.json")
    parser.add_argument("--sink-connection-id", help="Sink connection id for live mode")
    parser.add_argument("--credentials-source", choices=CREDENTIALS_SOURCE_CHOICES, default="env")
    parser.add_argument("--credentials-mount-point", help="Optional credentials mount point")
    parser.add_argument("--credentials-path", help="Optional credentials path")
    parser.add_argument("--max-actions", type=int, help="Maximum resync actions to execute")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def _add_stream_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--source", required=True)
    parser.add_argument("--sink", required=True)
    parser.add_argument("--backend", required=True, choices=[item.value for item in CDCBackend])
    parser.add_argument("--pipeline-name", required=True)
    parser.add_argument("--source-schema", required=True)
    parser.add_argument("--source-table", required=True)
    parser.add_argument("--target-dataset", required=True)
    parser.add_argument("--unique-key", action="append", required=True, help="Unique key column; repeatable")
