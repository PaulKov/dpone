from __future__ import annotations

import argparse


def register_route_certify_release_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "route-certify-release",
        help="Build a release-level route certification go/no-go report from route bundles",
    )
    parser.add_argument("--output-dir", default=".dpone/route-certify-release/latest")
    parser.add_argument("--release", required=True)
    parser.add_argument(
        "--profile",
        choices=["oss_ci", "local_live", "vendor_live"],
        default="oss_ci",
        help="Release profile; vendor_live requires vendor_live route bundles",
    )
    parser.add_argument(
        "--route-bundle",
        action="append",
        default=[],
        help="Route bundle as route_case_id=/path/to/route_certification_bundle.json",
    )
    parser.add_argument(
        "--route",
        action="append",
        default=None,
        help="Required route case id override; repeat to build a custom release matrix",
    )
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


__all__ = ["register_route_certify_release_parser"]
