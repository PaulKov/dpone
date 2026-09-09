from __future__ import annotations

import argparse

from dpone.readiness.cdc import CDCBackend

from .ops_parsers_artifacts import CREDENTIALS_SOURCE_CHOICES


def register_cdc_quarantine_inspect_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "cdc-quarantine-inspect",
        help="Summarize CDC poison quarantine records and replay actions",
    )
    parser.add_argument("--output-dir", default=".dpone/cdc-quarantine-inspection/latest")
    parser.add_argument("--quarantine-json", required=True, help="Path to cdc_poison_quarantine.json")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_cdc_replay_execute_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "cdc-replay-execute",
        help="Replay quarantined CDC events through an idempotent sink applier without committing offsets",
    )
    parser.add_argument("--output-dir", default=".dpone/cdc-replay/latest")
    parser.add_argument("--mode", choices=["local", "live"], default="local")
    parser.add_argument("--quarantine-json", required=True, help="Path to cdc_poison_quarantine.json")
    parser.add_argument("--source", required=True)
    parser.add_argument("--sink", required=True)
    parser.add_argument("--backend", required=True, choices=[item.value for item in CDCBackend])
    parser.add_argument("--pipeline-name", required=True)
    parser.add_argument("--source-schema", required=True)
    parser.add_argument("--source-table", required=True)
    parser.add_argument("--target-dataset", required=True)
    parser.add_argument("--unique-key", action="append", required=True, help="Unique key column; repeatable")
    parser.add_argument("--sink-connection-id", help="Sink connection id for live mode")
    parser.add_argument("--credentials-source", choices=CREDENTIALS_SOURCE_CHOICES, default="env")
    parser.add_argument("--credentials-mount-point", help="Optional credentials mount point")
    parser.add_argument("--credentials-path", help="Optional credentials path")
    parser.add_argument("--max-events", type=int, help="Maximum quarantined events to replay")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser
