"""Platform command for offline runtime-artifact attestation preflight."""

from __future__ import annotations

import argparse
import logging

from dpone.commands.airflow_deployment_attestation_cmd import artifact_attestation_group
from dpone.commands.airflow_self_service_output import emit_self_service_result
from dpone.readiness.airflow_artifact_attestation import verify_runtime_artifact_attestation


def register_artifact_attestation_verify_parser(
    subparsers: argparse._SubParsersAction,
) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "verify-attestation",
        help="Verify one release-set with the pinned offline runtime policy",
    )
    parser.add_argument("--subject", required=True, help="Exact release-set.json subject")
    parser.add_argument("--bundle", required=True, help="Portable GitHub attestation bundle")
    parser.add_argument("--trust-policy", required=True, help="Exact runtime trust-policy v2 JSON")
    parser.add_argument(
        "--expected-trust-policy-sha256",
        required=True,
        help="Pinned digest of the exact trust-policy bytes",
    )
    parser.add_argument(
        "--expected-trust-tier",
        required=True,
        choices=["production", "non_production"],
        help="Trust tier pinned by the deployment being verified",
    )
    parser.add_argument("--expected-subject-sha256", help="Optional pinned subject digest")
    parser.add_argument("--format", choices=["text", "json"], default="text")
    return parser


def cmd_airflow_artifact_attestation_verify(
    args: argparse.Namespace,
    *,
    ctx: object,
    logger: logging.Logger,
) -> int:
    del ctx, logger
    result = verify_runtime_artifact_attestation(
        subject_path=args.subject,
        bundle_path=args.bundle,
        trust_policy_path=args.trust_policy,
        expected_trust_policy_sha256=args.expected_trust_policy_sha256,
        expected_trust_tier=args.expected_trust_tier,
        expected_subject_sha256=args.expected_subject_sha256,
    )
    emit_self_service_result(
        result,
        args.format,
        command="airflow_artifact_attestation_verify",
    )
    return result.exit_code if result.exit_code is not None else (0 if result.passed else 4)


__all__ = [
    "artifact_attestation_group",
    "cmd_airflow_artifact_attestation_verify",
    "register_artifact_attestation_verify_parser",
]
