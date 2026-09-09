from __future__ import annotations

import argparse


def register_connection_doctor_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "connection-doctor",
        help="Check route prerequisites without opening source or sink database connections",
    )
    parser.add_argument("--output-dir", default=".dpone/connection-doctor/latest")
    parser.add_argument("--source", required=True)
    parser.add_argument("--sink", required=True)
    parser.add_argument("--strategy", required=True)
    parser.add_argument("--tool", action="append", default=[], help="Required executable name; repeat as needed")
    parser.add_argument(
        "--optional-tool",
        action="append",
        default=[],
        help="Optional executable name that should emit a warning when missing",
    )
    parser.add_argument("--env", action="append", default=[], help="Required environment variable name")
    parser.add_argument("--optional-env", action="append", default=[], help="Optional environment variable name")
    parser.add_argument("--python-import", action="append", default=[], help="Required Python import/module name")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_source_discover_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "source-discover",
        help="Normalize exported source schema JSON into source discovery evidence",
    )
    parser.add_argument("--output-dir", default=".dpone/source-discovery/latest")
    parser.add_argument("--source", required=True)
    parser.add_argument("--schema-json", required=True, help="Path to exported source schema JSON")
    parser.add_argument("--dataset", default="")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_route_bootstrap_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "route-bootstrap",
        help="Generate a route manifest draft and next-command onboarding plan",
    )
    parser.add_argument("--output-dir", default=".dpone/route-bootstrap/latest")
    parser.add_argument("--source", required=True)
    parser.add_argument("--sink", required=True)
    parser.add_argument("--strategy", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--source-discovery-json", help="Path to source_discovery.json")
    parser.add_argument("--manifest-id", help="Manifest process id to use in the generated draft")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_route_doctor_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "route-doctor",
        help="Aggregate route onboarding artifacts into one go/no-go report",
    )
    parser.add_argument("--output-dir", default=".dpone/route-doctor/latest")
    parser.add_argument("--source", required=True)
    parser.add_argument("--sink", required=True)
    parser.add_argument("--strategy", required=True)
    parser.add_argument("--artifact", action="append", default=[], help="Artifact reference as name=/path/to/file")
    parser.add_argument("--require", action="append", default=None, help="Required artifact name; repeat as needed")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser
