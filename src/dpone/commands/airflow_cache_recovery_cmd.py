"""CLI adapters for explicit Airflow deployment cache recovery."""

from __future__ import annotations

import argparse
import logging

from dpone.commands.airflow_self_service_output import emit_self_service_result
from dpone.readiness.airflow_self_service_composition import build_airflow_self_service_service


def register_cache_recovery_plan_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("cache-recovery-plan", help="Diagnose local Airflow deployment cache recovery")
    parser.add_argument("--cache-root", default=".dpone-cache", help="Local dpone cache root; defaults to .dpone-cache")
    parser.add_argument("--environment", default="dev", help="Deployment environment to inspect; defaults to dev")
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output text or dpone.deployment-cache-recovery-plan.v1 JSON; defaults to text",
    )
    return parser


def cmd_airflow_cache_recovery_plan(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    result = build_airflow_self_service_service().cache_recovery_plan(
        cache_root=args.cache_root,
        environment=args.environment,
    )
    emit_self_service_result(result, args.format, command="airflow_cache_recovery_plan", target=args.cache_root)
    return 0 if result.passed else 1


def register_cache_recovery_apply_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("cache-recovery-apply", help="Repair local Airflow deployment cache current pointer")
    parser.add_argument("--cache-root", default=".dpone-cache", help="Local dpone cache root; defaults to .dpone-cache")
    parser.add_argument("--environment", default="dev", help="Deployment environment to repair; defaults to dev")
    parser.add_argument("--deployment-id", required=True, help="Deployment id to promote during recovery")
    parser.add_argument("--promoted-by", required=True, help="CI/service identity applying the recovery")
    parser.add_argument(
        "--allowed-promoter",
        action="append",
        required=True,
        help="Platform-approved recovery identity; repeat for an exact-match allowlist",
    )
    current_guard = parser.add_mutually_exclusive_group(required=True)
    current_guard.add_argument(
        "--expected-current-deployment-id",
        help="Active deployment id observed in the reviewed recovery plan",
    )
    current_guard.add_argument(
        "--expect-current-absent",
        action="store_true",
        help="Assert that the reviewed recovery plan observed no active current deployment",
    )
    parser.add_argument(
        "--confirm-repair",
        action="store_true",
        help="Required acknowledgement before switching the local current pointer",
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output text or dpone.deployment-cache-recovery-apply.v1 JSON; defaults to text",
    )
    return parser


def cmd_airflow_cache_recovery_apply(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    result = build_airflow_self_service_service().cache_recovery_apply(
        cache_root=args.cache_root,
        environment=args.environment,
        deployment_id=args.deployment_id,
        confirm_repair=args.confirm_repair,
        promoted_by=args.promoted_by,
        expected_current_deployment_id=args.expected_current_deployment_id,
        allowed_promoters=tuple(args.allowed_promoter),
    )
    emit_self_service_result(result, args.format, command="airflow_cache_recovery_apply")
    if result.exit_code is not None:
        return result.exit_code
    return 0 if result.passed else 1


__all__ = [
    "cmd_airflow_cache_recovery_apply",
    "cmd_airflow_cache_recovery_plan",
    "register_cache_recovery_apply_parser",
    "register_cache_recovery_plan_parser",
]
