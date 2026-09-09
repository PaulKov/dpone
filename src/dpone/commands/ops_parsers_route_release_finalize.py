from __future__ import annotations

import argparse


def register_route_release_finalize_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "route-release-finalize",
        help="Finalize route-certified release evidence with discovery, freshness, provenance, and regression gates",
    )
    parser.add_argument("--output-dir", default=".dpone/route-release-finalize/latest")
    parser.add_argument("--release", required=True)
    parser.add_argument("--profile", choices=["oss_ci", "local_live", "vendor_live"], default="oss_ci")
    parser.add_argument("--bundle-root", action="append", default=[], help="Root to scan for route bundles")
    parser.add_argument(
        "--route-bundle",
        action="append",
        default=[],
        help="Explicit route bundle override as route_case_id=/path/to/route_certification_bundle.json",
    )
    parser.add_argument("--route", action="append", default=None, help="Required route case id override")
    parser.add_argument("--history-dir", default=".dpone/route-release-finalize/history")
    parser.add_argument("--baseline-json", help="Previous finalizer, history index, or route release JSON baseline")
    parser.add_argument("--max-age-hours", type=float, default=24.0)
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


__all__ = ["register_route_release_finalize_parser"]
