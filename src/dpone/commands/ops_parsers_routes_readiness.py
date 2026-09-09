"""Route readiness parser registration."""

from __future__ import annotations

import argparse


def register_route_readiness_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "route-readiness",
        help="Evaluate source -> sink -> strategy readiness from reusable route evidence",
    )
    parser.add_argument("--output-dir", default=".dpone/route-readiness/latest")
    parser.add_argument("--source", required=True)
    parser.add_argument("--sink", required=True)
    parser.add_argument("--strategy", required=True)
    parser.add_argument("--artifact", action="append", default=[], help="Evidence reference as name=/path/to/file")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_route_schema_evolution_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "route-schema-evolution",
        help="Evaluate route-level schema evolution evidence and target DDL apply readiness",
    )
    parser.add_argument("--output-dir", default=".dpone/route-schema-evolution/latest")
    parser.add_argument("--source", required=True)
    parser.add_argument("--sink", required=True)
    parser.add_argument("--strategy", required=True)
    parser.add_argument("--schema-evolution-json", required=True, help="Path to cdc_schema_evolution_evidence.json")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_route_reconciliation_repair_parser(
    subparsers: argparse._SubParsersAction,
) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "route-reconciliation-repair",
        help="Build route-level reconciliation repair evidence from source and target row samples",
    )
    parser.add_argument("--output-dir", default=".dpone/route-reconciliation-repair/latest")
    parser.add_argument("--source", required=True)
    parser.add_argument("--sink", required=True)
    parser.add_argument("--strategy", required=True)
    parser.add_argument("--source-rows-json", required=True, help="JSON list or object with rows for source rows")
    parser.add_argument("--target-rows-json", required=True, help="JSON list or object with rows for target rows")
    parser.add_argument("--key", action="append", required=True, help="Key column; repeat for composite keys")
    parser.add_argument("--compare-column", action="append", default=None, help="Column to compare; repeat as needed")
    parser.add_argument("--delete-column", help="Source delete marker column")
    parser.add_argument("--source-boundary", default="full-snapshot")
    parser.add_argument("--target-boundary", default="full-snapshot")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_route_data_quality_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "route-data-quality",
        help="Build a route data quality scorecard and exception backlog receipt",
    )
    parser.add_argument("--output-dir", default=".dpone/route-data-quality/latest")
    parser.add_argument("--source", required=True)
    parser.add_argument("--sink", required=True)
    parser.add_argument("--strategy", required=True)
    parser.add_argument("--artifact", action="append", default=[], help="Evidence reference as name=/path/to/file")
    parser.add_argument(
        "--require",
        action="append",
        default=None,
        help="Additional required data quality evidence domain; can be repeated",
    )
    parser.add_argument("--min-score", type=float, default=95.0, help="Minimum route DQ score required to pass")
    parser.add_argument("--warning-score", type=float, default=98.0, help="Score below this value emits a warning")
    parser.add_argument(
        "--max-exception-ratio",
        type=float,
        default=0.0,
        help="Maximum allowed failed/quarantined row ratio",
    )
    parser.add_argument("--max-quarantine-rows", type=int, default=0, help="Maximum allowed quarantine rows")
    parser.add_argument(
        "--max-exception-age-hours",
        type=float,
        default=24.0,
        help="Maximum allowed age for open route exceptions",
    )
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


__all__ = [
    "register_route_data_quality_parser",
    "register_route_readiness_parser",
    "register_route_reconciliation_repair_parser",
    "register_route_schema_evolution_parser",
]
