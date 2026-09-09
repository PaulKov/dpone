from __future__ import annotations

import argparse

from dpone.readiness.cdc import CDCBackend

from .ops_parsers_artifacts import CREDENTIALS_SOURCE_CHOICES


def register_cdc_compare_repair_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "cdc-compare-repair",
        help="Compare a CDC source table with a ClickHouse CDC log current-state projection and write a repair plan",
    )
    _add_stream_args(parser)
    parser.add_argument("--output-dir", default=".dpone/cdc-compare-repair/latest")
    parser.add_argument("--mode", choices=["local", "live"], default="local")
    parser.add_argument("--column", action="append", help="Source payload column to compare; repeatable")
    parser.add_argument("--source-rows-json", help="Local source rows JSON list or object with rows")
    parser.add_argument("--target-rows-json", help="Local target rows JSON list or object with rows")
    parser.add_argument("--source-connection-id", help="Source connection id for live mode")
    parser.add_argument("--sink-connection-id", help="Sink connection id for live mode")
    parser.add_argument("--credentials-source", choices=CREDENTIALS_SOURCE_CHOICES, default="env")
    parser.add_argument("--credentials-mount-point", help="Optional credentials mount point")
    parser.add_argument("--credentials-path", help="Optional credentials path")
    parser.add_argument("--max-rows", type=int, default=10000)
    parser.add_argument("--max-diffs", type=int, default=1000)
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_cdc_repair_execute_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "cdc-repair-execute",
        help="Execute a CDC repair plan through an idempotent sink applier without committing offsets",
    )
    _add_stream_args(parser)
    parser.add_argument("--output-dir", default=".dpone/cdc-repair/latest")
    parser.add_argument("--mode", choices=["local", "live"], default="local")
    parser.add_argument("--repair-plan-json", required=True, help="Path to cdc_repair_plan.json")
    parser.add_argument("--sink-connection-id", help="Sink connection id for live mode")
    parser.add_argument("--credentials-source", choices=CREDENTIALS_SOURCE_CHOICES, default="env")
    parser.add_argument("--credentials-mount-point", help="Optional credentials mount point")
    parser.add_argument("--credentials-path", help="Optional credentials path")
    parser.add_argument("--max-actions", type=int, help="Maximum repair actions to execute")
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
