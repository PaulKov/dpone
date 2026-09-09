"""Route run state and ledger parser registration."""

from __future__ import annotations

import argparse


def register_route_run_supervisor_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "route-run-supervisor",
        help="Build one route run lifecycle receipt from route and CDC evidence artifacts",
    )
    parser.add_argument("--output-dir", default=".dpone/route-run/latest")
    parser.add_argument("--source", required=True)
    parser.add_argument("--sink", required=True)
    parser.add_argument("--strategy", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--manifest", help="Manifest path used for the supervised run")
    parser.add_argument(
        "--run-mode",
        choices=["evidence_only", "evidence-only", "route_refresh", "route-refresh"],
        default="evidence_only",
        help="Evidence contract mode for this supervised route run",
    )
    parser.add_argument("--artifact", action="append", default=[], help="Evidence reference as name=/path/to/file")
    parser.add_argument(
        "--require",
        action="append",
        default=None,
        help="Additional required evidence domain; can be repeated",
    )
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_route_execution_ledger_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "route-execution-ledger",
        help="Record an idempotent route execution step and commit protocol evidence",
    )
    parser.add_argument("--output-dir", default=".dpone/route-execution-ledger/latest")
    parser.add_argument("--source", required=True)
    parser.add_argument("--sink", required=True)
    parser.add_argument("--strategy", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument(
        "--stage",
        required=True,
        choices=["planned", "extracting", "loaded_to_staging", "finalized", "quality_checked", "state_committed"],
    )
    parser.add_argument(
        "--status", required=True, choices=["planned", "running", "succeeded", "failed", "skipped", "committed"]
    )
    parser.add_argument("--runner-id", required=True)
    parser.add_argument("--source-boundary", default="")
    parser.add_argument("--sink-boundary", default="")
    parser.add_argument("--idempotency-key")
    parser.add_argument("--lease-ttl-seconds", type=int)
    parser.add_argument("--artifact", action="append", default=[], help="Artifact reference as name=/path/to/file")
    parser.add_argument(
        "--store-backend",
        choices=["local_json", "sqlite"],
        default="local_json",
        help="Ledger persistence backend for steps and leases",
    )
    parser.add_argument(
        "--store-uri", help="Backend-specific store location; for sqlite this is the database file path"
    )
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_route_state_promote_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "route-state-promote",
        help="Promote source state from a verified route execution ledger and commit receipt",
    )
    parser.add_argument("--output-dir", default=".dpone/route-state-promotion/latest")
    parser.add_argument("--source", required=True)
    parser.add_argument("--sink", required=True)
    parser.add_argument("--strategy", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--ledger-json", required=True, help="Path to route_execution_ledger.json")
    parser.add_argument("--proposed-state", required=True)
    parser.add_argument("--source-boundary", required=True)
    parser.add_argument("--sink-boundary", required=True)
    parser.add_argument("--idempotency-key", required=True)
    parser.add_argument("--commit-token", required=True)
    parser.add_argument("--target", required=True)
    parser.add_argument("--fencing-token", default="")
    parser.add_argument("--rows-applied", type=int, default=0)
    parser.add_argument("--events-applied", type=int, default=0)
    parser.add_argument(
        "--state-backend",
        choices=["local_json", "sqlite"],
        default="local_json",
        help="State persistence backend for promoted source state",
    )
    parser.add_argument(
        "--state-uri",
        help="Backend-specific state-store location; for sqlite this is the database file path",
    )
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


__all__ = [
    "register_route_execution_ledger_parser",
    "register_route_run_supervisor_parser",
    "register_route_state_promote_parser",
]
