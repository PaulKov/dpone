"""Route release-candidate parser registration."""

from __future__ import annotations

import argparse


def register_route_rc_orchestrator_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "route-rc-orchestrator",
        help="Run the route release candidate evidence train and write one orchestration receipt",
    )
    parser.add_argument("--output-dir", default=".dpone/route-rc-orchestrator/latest")
    parser.add_argument("--release", required=True)
    parser.add_argument("--source", required=True)
    parser.add_argument("--sink", required=True)
    parser.add_argument("--strategy", required=True)
    parser.add_argument(
        "--profile",
        choices=["local_live", "real_local", "type_matrix_certification", "native_transfer", "vendor_live"],
        default="real_local",
    )
    parser.add_argument("--row-count", type=int, default=10000)
    parser.add_argument("--artifact", action="append", default=[], help="Evidence reference as name=/path/to/file")
    parser.add_argument(
        "--require",
        action="append",
        default=None,
        help="Additional required release evidence domain; can be repeated",
    )
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_route_rc_execute_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "route-rc-execute",
        help="Dry-run or execute a route release candidate orchestration receipt",
    )
    parser.add_argument("--orchestration-json", required=True, help="Path to route_rc_orchestration.json")
    parser.add_argument("--output-dir", default=".dpone/route-rc-execution/latest")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Run commands from the orchestration receipt; omit for dry-run planning",
    )
    parser.add_argument("--timeout-seconds", type=int, default=1800)
    parser.add_argument("--max-attempts", type=int, default=1)
    parser.add_argument("--redact", action="append", default=[], help="Additional literal secret value to redact")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


__all__ = ["register_route_rc_execute_parser", "register_route_rc_orchestrator_parser"]
