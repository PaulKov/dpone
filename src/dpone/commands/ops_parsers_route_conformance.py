from __future__ import annotations

import argparse


def register_route_conformance_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "route-conformance",
        help="Run deterministic route conformance checks and release gates",
    )
    commands = parser.add_subparsers(dest="route_conformance_command", required=True)
    _register_run(commands)
    _register_live_run(commands)
    _register_summarize(commands)
    _register_release_gate(commands)
    return parser


def _register_run(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("run", help="Generate exact route conformance evidence")
    parser.add_argument("--output-dir", default=".dpone/route-conformance/latest")
    parser.add_argument("--source", required=True)
    parser.add_argument("--sink", required=True)
    parser.add_argument("--strategy", required=True)
    parser.add_argument("--dataset-profile", default="wide_10k_contract")
    parser.add_argument("--rows", type=int, default=10_000)
    parser.add_argument("--columns", type=int, default=200)
    parser.add_argument("--chunk-size", type=int, default=1_000)
    parser.add_argument("--seed", type=int, default=17)
    parser.add_argument("--include-nested", action="store_true")
    parser.add_argument("--no-schema-evolution", action="store_true")
    parser.add_argument("--require-schema-evolution", action="store_true")
    parser.add_argument("--min-rows", type=int, default=0)
    parser.add_argument("--min-columns", type=int, default=0)
    parser.add_argument("--format", choices=["md", "json"], default="md")


def _register_live_run(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("live-run", help="Run live route conformance through an adapter")
    parser.add_argument("--output-dir", default=".dpone/route-conformance-live/latest")
    parser.add_argument("--source", required=True)
    parser.add_argument("--sink", required=True)
    parser.add_argument("--strategy", required=True)
    parser.add_argument("--adapter", default="in_memory")
    parser.add_argument("--dataset-profile", default="wide_live_contract")
    parser.add_argument("--rows", type=int, default=10_000)
    parser.add_argument("--columns", type=int, default=200)
    parser.add_argument("--chunk-size", type=int, default=1_000)
    parser.add_argument("--seed", type=int, default=17)
    parser.add_argument("--include-nested", action="store_true")
    parser.add_argument("--no-schema-evolution", action="store_true")
    parser.add_argument("--require-schema-evolution", action="store_true")
    parser.add_argument("--min-rows", type=int, default=0)
    parser.add_argument("--min-columns", type=int, default=0)
    parser.add_argument("--drift", choices=["none", "value", "row_count", "contract"], default="none")
    parser.add_argument("--format", choices=["md", "json"], default="md")


def _register_summarize(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("summarize", help="Summarize route conformance artifacts")
    parser.add_argument("--output-dir", default=".dpone/route-conformance-summary/latest")
    parser.add_argument("--artifact", action="append", default=[], help="Artifact reference as name=/path/to/file")
    parser.add_argument("--format", choices=["md", "json"], default="md")


def _register_release_gate(subparsers: argparse._SubParsersAction) -> None:
    parser = subparsers.add_parser("release-gate", help="Fail-closed release gate for route conformance artifacts")
    parser.add_argument("--output-dir", default=".dpone/route-conformance-release-gate/latest")
    parser.add_argument("--release", required=True)
    parser.add_argument("--artifact", action="append", default=[], help="Artifact reference as name=/path/to/file")
    parser.add_argument("--format", choices=["md", "json"], default="md")
