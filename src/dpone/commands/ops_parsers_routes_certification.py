"""Route release certification parser registration."""

from __future__ import annotations

import argparse


def register_route_release_gate_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "route-release-gate",
        help="Aggregate route evidence into one release go/no-go receipt",
    )
    parser.add_argument("--output-dir", default=".dpone/route-release-gate/latest")
    parser.add_argument("--release", required=True)
    parser.add_argument("--source", required=True)
    parser.add_argument("--sink", required=True)
    parser.add_argument("--strategy", required=True)
    parser.add_argument("--artifact", action="append", default=[], help="Evidence reference as name=/path/to/file")
    parser.add_argument(
        "--require",
        action="append",
        default=None,
        help="Additional required evidence domain; can be repeated",
    )
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_route_live_certification_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "route-live-certification",
        help="Build a route-aware live certification harness and evidence bundle",
    )
    parser.add_argument("--output-dir", default=".dpone/route-live-certification/latest")
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
        help="Additional required live evidence domain; can be repeated",
    )
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


def register_route_certify_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "route-certify",
        help="Build a route release certification bundle and promotion gate from immutable evidence",
    )
    parser.add_argument("--output-dir", default=".dpone/route-certify/latest")
    parser.add_argument("--release", required=True)
    parser.add_argument("--source", required=True)
    parser.add_argument("--sink", required=True)
    parser.add_argument("--strategy", required=True)
    parser.add_argument(
        "--profile",
        choices=["oss_ci", "local_live", "vendor_live"],
        default="oss_ci",
        help="Certification profile; vendor_live requires route_live_evidence_bundle",
    )
    parser.add_argument("--artifact", action="append", default=[], help="Evidence reference as name=/path/to/file")
    parser.add_argument(
        "--release-set",
        help="Content-valid release-set used to bind a six-dimensional matrix claim into this bundle",
    )
    parser.add_argument(
        "--require",
        action="append",
        default=None,
        help="Additional route certification evidence domain; can be repeated",
    )
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


__all__ = [
    "register_route_certify_parser",
    "register_route_live_certification_parser",
    "register_route_release_gate_parser",
]
