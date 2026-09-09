"""CLI adapters for plan-first Airflow deployment cache retention."""

from __future__ import annotations

import argparse
import logging

from dpone.commands.airflow_self_service_output import emit_self_service_result
from dpone.readiness.airflow_self_service_composition import build_airflow_self_service_service


def register_cache_retention_plan_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("cache-retention-plan", help="Build a plan-first Airflow deployment cache GC report")
    _add_retention_scope_arguments(parser)
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output text or dpone.deployment-cache-retention-plan.v1 JSON; defaults to text",
    )
    setattr(
        parser,
        "_dpone_io_contract",
        (
            "stdout contains a human report or dpone.deployment-cache-retention-plan.v1 JSON; argparse errors use stderr.",
            "Exit 0 is a complete read-only plan, 1 is an operational blocker, and 2 is invalid deployment identity.",
            "The command never mutates cache state.",
        ),
    )
    return parser


def cmd_airflow_cache_retention_plan(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    result = build_airflow_self_service_service().cache_retention_plan(
        cache_root=args.cache_root,
        environment=args.environment,
        protected_deployment_ids=tuple(args.protect_deployment_id),
        evidence_files=tuple(args.evidence_file),
    )
    emit_self_service_result(result, args.format, command="airflow_cache_retention_plan", target=args.cache_root)
    if result.exit_code is not None:
        return result.exit_code
    return 0 if result.passed else 1


def register_cache_retention_apply_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "cache-retention-apply",
        help="Apply Airflow deployment cache GC for confirmed delete candidates",
    )
    _add_retention_scope_arguments(parser)
    parser.add_argument(
        "--confirm-delete",
        action="store_true",
        help="Required acknowledgement before deleting unreferenced deployment cache entries",
    )
    parser.add_argument("--promoted-by", required=True, help="CI/service identity applying cache retention")
    parser.add_argument(
        "--expected-plan-sha256",
        help=(
            "Exact plan_sha256 returned by the reviewed cache-retention-plan; "
            "required when that fresh plan contains delete candidates"
        ),
    )
    parser.add_argument(
        "--review-id",
        help="Canonical UUIDv4 for this approved destructive attempt; required when the plan deletes deployments",
    )
    parser.add_argument(
        "--loader-ack-file",
        help=(
            "Strict dpone.airflow_loader_ack.v2 for the exact active cache; "
            "required when the fresh plan contains delete candidates"
        ),
    )
    parser.add_argument(
        "--allowed-promoter",
        action="append",
        required=True,
        help="Platform-approved retention identity; repeat for an exact-match allowlist",
    )
    parser.add_argument(
        "--evidence-version",
        choices=["v1", "v2", "v3"],
        default="v1",
        help=(
            "JSON evidence contract: v1 preserves the published compatibility output; "
            "v2 adds reviewed-plan and activation-history evidence; "
            "destructive v3 adds durable committed transaction-receipt evidence, while a no-op "
            "v3 result has empty deletion arrays and intentionally omits receipt fields"
        ),
    )
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output text or the selected retention evidence JSON; defaults to text",
    )
    setattr(
        parser,
        "_dpone_io_contract",
        (
            "stdout contains a human report or the selected v1/v2/v3 retention evidence JSON; argparse errors use stderr.",
            "Exit 0 is a committed destructive receipt or schema-valid no-op, 1 is a fail-closed operational or "
            "evidence blocker, 2 is invalid deployment identity, and 4 is missing or unauthorized deletion approval.",
            "Destructive v3 success requires the reviewed plan, UUIDv4 review ID, exact loader acknowledgement, "
            "and committed durable receipt.",
        ),
    )
    return parser


def cmd_airflow_cache_retention_apply(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    result = build_airflow_self_service_service().cache_retention_apply(
        cache_root=args.cache_root,
        environment=args.environment,
        confirm_delete=args.confirm_delete,
        promoted_by=args.promoted_by,
        allowed_promoters=tuple(args.allowed_promoter),
        expected_plan_sha256=args.expected_plan_sha256,
        review_id=args.review_id,
        loader_ack_file=args.loader_ack_file,
        evidence_version=args.evidence_version,
        protected_deployment_ids=tuple(args.protect_deployment_id),
        evidence_files=tuple(args.evidence_file),
    )
    emit_self_service_result(result, args.format, command="airflow_cache_retention_apply")
    if result.exit_code is not None:
        return result.exit_code
    return 0 if result.passed else 1


def _add_retention_scope_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--cache-root", default=".dpone-cache", help="Local dpone cache root; defaults to .dpone-cache")
    parser.add_argument("--environment", default="dev", help="Deployment environment to inspect; defaults to dev")
    parser.add_argument(
        "--protect-deployment-id",
        action="append",
        default=[],
        help="Deployment id protected by active runs or reproducible evidence; repeatable",
    )
    parser.add_argument(
        "--evidence-file",
        action="append",
        default=[],
        help="Local evidence JSON containing deployment_id references to protect; repeatable",
    )


__all__ = [
    "cmd_airflow_cache_retention_apply",
    "cmd_airflow_cache_retention_plan",
    "register_cache_retention_apply_parser",
    "register_cache_retention_plan_parser",
]
