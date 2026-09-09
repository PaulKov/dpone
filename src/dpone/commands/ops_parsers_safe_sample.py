"""Safe sample runtime parser registration."""

from __future__ import annotations

import argparse


def register_safe_sample_runtime_run_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "safe-sample-runtime-run",
        help="Run a prepared safe-sample execution plan inside the runtime boundary",
    )
    parser.add_argument("--plan-json", required=True, help="Path to dpone.safe-sample-execution-plan.v1 JSON")
    parser.add_argument(
        "--pipeline-source",
        help="Optional pipeline.yaml used to assemble a route-certified safe-sample copier",
    )
    parser.add_argument("--process-name", help="Optional process name inside --pipeline-source")
    parser.add_argument(
        "--enable-live-copy",
        action="store_true",
        help="Opt in to credential-resolving certified sample copy execution",
    )
    parser.add_argument("--binding-set", help="Runtime binding-set YAML/JSON; required with --enable-live-copy")
    parser.add_argument(
        "--connection-registry",
        help="Runtime connection-registry YAML/JSON; required with --enable-live-copy",
    )
    parser.add_argument(
        "--credential-runtime",
        help="Runtime credential-runtime YAML/JSON; required with --enable-live-copy",
    )
    parser.add_argument("--route-attestation", help="Signed deployment-bound route-attestation JSON")
    parser.add_argument("--route-attestation-bundle", help="Detached Sigstore bundle for --route-attestation")
    parser.add_argument("--route-certification-bundle", help="Immutable route-certification evidence bundle")
    parser.add_argument("--route-attestation-policy", help="Platform-owned exact-identity verification policy")
    parser.add_argument("--output-dir", default=".dpone/safe-sample-runtime/latest")
    parser.add_argument("--cache-root", default=".dpone-cache", help="Local materialized dpone cache root")
    parser.add_argument("--format", choices=["md", "json"], default="json")
    return parser


__all__ = ["register_safe_sample_runtime_run_parser"]
