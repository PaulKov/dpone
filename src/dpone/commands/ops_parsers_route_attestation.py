"""Parser registration for platform route-attestation operations."""

from __future__ import annotations

import argparse


def register_route_attestation_build_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "route-attestation-build",
        help="Build a deployment-bound route authorization blob for external signing",
    )
    parser.add_argument("--route-id", required=True)
    parser.add_argument("--route-certification-bundle", required=True)
    parser.add_argument("--deployment-set", required=True)
    parser.add_argument("--authorization-profile", default="safe_sample_production")
    parser.add_argument("--issued-at", required=True, help="Offset-aware issue time, for example 2026-07-15T10:00:00Z")
    parser.add_argument("--not-before", required=True, help="Offset-aware validity start")
    parser.add_argument("--expires-at", required=True, help="Offset-aware expiry")
    parser.add_argument("--output", required=True, help="Create-only output JSON path")
    parser.add_argument("--format", choices=("json", "md"), default="json")
    return parser


def register_route_attestation_verify_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "route-attestation-verify",
        help="Verify a signed route authorization against one immutable deployment",
    )
    parser.add_argument("--attestation", required=True)
    parser.add_argument("--sigstore-bundle", required=True)
    parser.add_argument("--route-certification-bundle", required=True)
    parser.add_argument("--policy", required=True)
    parser.add_argument("--deployment-set", required=True)
    parser.add_argument(
        "--output-dir",
        default=".dpone/route-attestation-verification",
        help="Create-only verification receipt directory",
    )
    parser.add_argument("--format", choices=("json", "md"), default="json")
    return parser


__all__ = [
    "register_route_attestation_build_parser",
    "register_route_attestation_verify_parser",
]
