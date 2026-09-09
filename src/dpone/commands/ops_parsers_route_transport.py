from __future__ import annotations

import argparse


def register_route_transport_certification_parser(
    subparsers: argparse._SubParsersAction,
) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "route-transport-certification",
        help="Certify native-transfer transport selection for one source -> sink route",
    )
    parser.add_argument("--manifest", required=True, help="Manifest path used to build the route decision")
    parser.add_argument("--artifact-dir", default=".dpone/certification/native-transfer-route")
    parser.add_argument("--profile", choices=["static", "local_live", "vendor_live"], default="static")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser


__all__ = ["register_route_transport_certification_parser"]
