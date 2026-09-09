"""Route refresh parser registration."""

from __future__ import annotations

import argparse


def register_route_refresh_plan_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "route-refresh-plan",
        help="Plan an idempotent route refresh, backfill, replay, or resync operation",
    )
    parser.add_argument("--output-dir", default=".dpone/route-refresh-plan/latest")
    parser.add_argument("--source", required=True)
    parser.add_argument("--sink", required=True)
    parser.add_argument("--strategy", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument(
        "--reason",
        required=True,
        choices=[
            "initial_backfill",
            "manual_resync",
            "dq_repair",
            "schema_backfill",
            "retention_gap",
            "range_replay",
        ],
    )
    parser.add_argument("--window-kind", required=True, choices=["integer", "timestamp", "partition", "state"])
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--chunk-size", type=int)
    parser.add_argument("--max-chunks", type=int, default=1000)
    parser.add_argument("--partition", default="")
    parser.add_argument("--current-state", default="")
    parser.add_argument("--target-state", default="")
    parser.add_argument("--destructive", action="store_true")
    parser.add_argument("--require-approval", action="store_true")
    parser.add_argument("--approval-granted", action="store_true")
    parser.add_argument("--artifact", action="append", default=[], help="Evidence reference as name=/path/to/file")
    parser.add_argument(
        "--require",
        action="append",
        default=None,
        help="Additional required refresh evidence domain; can be repeated",
    )
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_route_refresh_execute_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "route-refresh-execute",
        help="Dry-run or execute an approved route refresh plan through a configured executor",
    )
    parser.add_argument("--output-dir", default=".dpone/route-refresh-execute/latest")
    parser.add_argument("--route-refresh-plan-json", required=True, help="Path to route_refresh_plan.json")
    parser.add_argument("--runner-id", required=True)
    parser.add_argument("--execute", action="store_true", help="Execute chunks with a configured backend")
    parser.add_argument("--executor", default=None, help="Optional executor backend selector")
    parser.add_argument("--executor-config-json", help="Executor config JSON path for the selected backend")
    parser.add_argument("--source", help="Optional expected source route id")
    parser.add_argument("--sink", help="Optional expected sink route id")
    parser.add_argument("--strategy", help="Optional expected strategy")
    parser.add_argument("--max-chunks", type=int)
    parser.add_argument("--artifact", action="append", default=[], help="Artifact reference as name=/path/to/file")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_route_refresh_capture_snapshots_parser(
    subparsers: argparse._SubParsersAction,
) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "route-refresh-capture-snapshots",
        help="Capture source and sink route refresh snapshots for verification",
    )
    parser.add_argument("--output-dir", default=".dpone/route-refresh-snapshot-capture/latest")
    parser.add_argument("--route-refresh-execution-json", required=True, help="Path to route_refresh_execution.json")
    parser.add_argument("--source-rows-json", help="Credential-free source rows JSON for snapshot capture")
    parser.add_argument("--sink-rows-json", help="Credential-free sink rows JSON for snapshot capture")
    parser.add_argument("--runner-id", required=True)
    parser.add_argument("--executor", default=None, help="Optional backend selector, for example mssql_clickhouse")
    parser.add_argument("--executor-config-json", help="Backend config JSON path for the selected reader backend")
    parser.add_argument("--key", action="append", required=True, help="Key column; repeat for composite keys")
    parser.add_argument("--boundary-column", required=True)
    parser.add_argument("--column", action="append", required=True, help="Compared column; repeat in route order")
    parser.add_argument("--type", action="append", default=[], help="Column type hint as name=type")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_route_refresh_verify_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "route-refresh-verify",
        help="Verify a succeeded route refresh execution through source and sink snapshots",
    )
    parser.add_argument("--output-dir", default=".dpone/route-refresh-verify/latest")
    parser.add_argument("--route-refresh-execution-json", required=True, help="Path to route_refresh_execution.json")
    parser.add_argument("--source-snapshot-json", required=True, help="Path to source route_refresh_snapshot.json")
    parser.add_argument("--sink-snapshot-json", required=True, help="Path to sink route_refresh_snapshot.json")
    parser.add_argument("--runner-id", required=True)
    parser.add_argument("--key", action="append", required=True, help="Key column; repeat for composite keys")
    parser.add_argument("--boundary-column", required=True)
    parser.add_argument("--column", action="append", required=True, help="Compared column; repeat in route order")
    parser.add_argument("--type", action="append", default=[], help="Column type hint as name=type")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


__all__ = [
    "register_route_refresh_capture_snapshots_parser",
    "register_route_refresh_execute_parser",
    "register_route_refresh_plan_parser",
    "register_route_refresh_verify_parser",
]
