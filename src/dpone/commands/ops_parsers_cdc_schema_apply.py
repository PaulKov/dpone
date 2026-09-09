from __future__ import annotations

import argparse

from .ops_parsers_artifacts import CREDENTIALS_SOURCE_CHOICES


def register_cdc_schema_apply_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "cdc-schema-apply",
        help="Dry-run or apply governed target DDL for a CDC schema change",
    )
    parser.add_argument("--output-dir", default=".dpone/cdc-schema-apply/latest")
    parser.add_argument("--schema-change-json", required=True, help="Path to CDC schema change JSON")
    parser.add_argument("--sink", required=True, choices=["clickhouse"])
    parser.add_argument("--target-dataset", required=True, help="Target serving dataset as database.table")
    parser.add_argument("--mode", choices=["dry_run", "apply"], default="dry_run")
    parser.add_argument("--sink-connection-id", help="Sink connection id; required for --mode apply")
    parser.add_argument("--credentials-source", choices=CREDENTIALS_SOURCE_CHOICES, default="env")
    parser.add_argument("--credentials-mount-point", help="Optional credentials mount point")
    parser.add_argument("--credentials-path", help="Optional credentials path")
    parser.add_argument("--typed-refresh", action="store_true", help="Refresh typed materialization after applying DDL")
    parser.add_argument("--cdc-dataset", help="ClickHouse CDC log dataset for typed refresh")
    parser.add_argument("--unique-key", action="append", default=[], help="Unique key column for typed refresh")
    parser.add_argument(
        "--column", action="append", default=[], help="Typed column as name=ClickHouseType for typed refresh"
    )
    parser.add_argument("--require-approval", action="store_true")
    parser.add_argument("--fail-on-parse-errors", action="store_true")
    parser.add_argument("--max-parse-error-ratio", type=float, default=0.0)
    parser.add_argument("--quarantine-dataset", help="Optional ClickHouse table for typed parse quarantine rows")
    parser.add_argument("--schema-drift-mode", choices=["off", "warn_additive", "strict"], default="warn_additive")
    parser.add_argument("--quality-sample-limit", type=int, default=100)
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser
