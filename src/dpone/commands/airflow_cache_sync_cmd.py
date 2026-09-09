"""CLI adapter for guarded Airflow deployment cache activation."""

from __future__ import annotations

import argparse
import logging

from dpone.commands.airflow_self_service_output import emit_self_service_result
from dpone.readiness.airflow_promotion_guard import (
    PromotionPreconditionError,
    file_digest_precondition,
)
from dpone.readiness.airflow_self_service_cache_sync import cache_sync_result
from dpone.readiness.airflow_self_service_models import (
    SelfServiceResult,
    dpone_error,
    manual_fix,
)


def register_cache_sync_parser(
    subparsers: argparse._SubParsersAction,
) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "cache-sync",
        help="Verify a pinned release and promote its deployment to current",
    )
    parser.add_argument(
        "--cache-root",
        default=".dpone-cache",
        help="Local dpone cache root; defaults to .dpone-cache",
    )
    parser.add_argument(
        "--deployment-dir",
        required=True,
        help="Deployment with pinned release-set and artifacts",
    )
    parser.add_argument(
        "--environment",
        required=True,
        help="Deployment environment to promote",
    )
    parser.add_argument(
        "--promoted-by",
        required=True,
        help="CI/service identity promoting the deployment",
    )
    parser.add_argument(
        "--allowed-promoter",
        action="append",
        required=True,
        help="Platform-approved CI/service identity; repeat for an exact-match allowlist",
    )
    current_guard = parser.add_mutually_exclusive_group(required=True)
    current_guard.add_argument(
        "--expected-current-deployment-id",
        help="CAS guard for updates; promotion fails if current pointer changed",
    )
    current_guard.add_argument(
        "--expect-current-absent",
        action="store_true",
        help="CAS guard for the first promotion; fails if current already exists",
    )
    parser.add_argument("--source-commit", help="Optional source commit provenance")
    parser.add_argument(
        "--attestation-ref",
        help="Optional promotion attestation reference",
    )
    parser.add_argument(
        "--precommit-guard-path",
        help="Optional bounded desired-state file rechecked inside the promotion lock",
    )
    parser.add_argument(
        "--expected-precommit-guard-sha256",
        help="Canonical SHA-256 of --precommit-guard-path",
    )
    parser.add_argument(
        "--confirm-promote",
        action="store_true",
        help="Required acknowledgement before switching the local current pointer",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output text or dpone cache-sync JSON; defaults to text",
    )
    return parser


def cmd_airflow_cache_sync(
    args: argparse.Namespace,
    *,
    ctx: object,
    logger: logging.Logger,
) -> int:
    del ctx, logger
    guard_path = args.precommit_guard_path
    guard_sha256 = args.expected_precommit_guard_sha256
    if bool(guard_path) != bool(guard_sha256):
        result = _guard_error()
        emit_self_service_result(result, args.format, command="airflow_cache_sync")
        return result.exit_code or 4
    try:
        precondition = (
            file_digest_precondition(guard_path, expected_sha256=guard_sha256) if guard_path and guard_sha256 else None
        )
    except PromotionPreconditionError as exc:
        result = _guard_error(code=exc.code, message=str(exc))
        emit_self_service_result(result, args.format, command="airflow_cache_sync")
        return result.exit_code or 4
    result = cache_sync_result(
        cache_root=args.cache_root,
        deployment_dir=args.deployment_dir,
        environment=args.environment,
        promoted_by=args.promoted_by,
        confirm_promote=args.confirm_promote,
        allowed_promoters=tuple(args.allowed_promoter),
        expected_current_deployment_id=args.expected_current_deployment_id,
        expect_current_absent=args.expect_current_absent,
        source_commit=args.source_commit,
        attestation_ref=args.attestation_ref,
        promotion_precondition=precondition,
    )
    emit_self_service_result(result, args.format, command="airflow_cache_sync")
    if result.exit_code is not None:
        return result.exit_code
    return 0 if result.passed else 1


def _guard_error(
    *,
    code: str = "DPONE_PROMOTION_PRECONDITION_INVALID",
    message: str = "promotion guard path and digest must be supplied together",
) -> SelfServiceResult:
    return SelfServiceResult(
        passed=False,
        errors=(
            dpone_error(
                code,
                message,
                stage="cache_sync",
                fixes=[manual_fix("refresh_desired_deployment_and_retry")],
            ),
        ),
        exit_code=4,
    )


__all__ = ["cmd_airflow_cache_sync", "register_cache_sync_parser"]
