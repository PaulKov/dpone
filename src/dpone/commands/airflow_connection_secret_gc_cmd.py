"""CLI composition root for Airflow Connection Secret garbage collection."""

from __future__ import annotations

import argparse
import logging

from dpone.commands.airflow_self_service_output import emit_self_service_result
from dpone.readiness.airflow_self_service_secret_gc import (
    connection_secret_gc_apply_command_result,
    connection_secret_gc_plan_command_result,
)


def register_connection_secret_gc_plan_parser(
    subparsers: argparse._SubParsersAction,
) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "connection-secret-gc-plan",
        help="Plan metadata-only cleanup of retained dpone Airflow Connection Secrets",
    )
    _add_common_arguments(parser)
    return parser


def register_connection_secret_gc_apply_parser(
    subparsers: argparse._SubParsersAction,
) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "connection-secret-gc-apply",
        help="Apply a fresh bounded Airflow Connection Secret GC plan",
    )
    _add_common_arguments(parser)
    parser.add_argument("--max-delete-count", type=int, default=100, help="Maximum deletes per invocation; 1..1000")
    parser.add_argument("--actor", required=True, help="Platform audit identity")
    parser.add_argument(
        "--allowed-actor",
        action="append",
        required=True,
        help="Exact allowed platform identity; repeatable",
    )
    parser.add_argument("--confirm-delete", action="store_true", help="Required deletion acknowledgement")
    return parser


def cmd_airflow_connection_secret_gc_plan(
    args: argparse.Namespace,
    *,
    ctx: object,
    logger: logging.Logger,
) -> int:
    del ctx, logger
    result = connection_secret_gc_plan_command_result(
        namespace=args.namespace,
        minimum_age_seconds=args.minimum_age_seconds,
        page_size=args.page_size,
        auth_mode=args.kube_auth,
        kube_context=args.kube_context,
    )
    emit_self_service_result(result, args.format, command="airflow_connection_secret_gc_plan")
    return result.exit_code if result.exit_code is not None else (0 if result.passed else 1)


def cmd_airflow_connection_secret_gc_apply(
    args: argparse.Namespace,
    *,
    ctx: object,
    logger: logging.Logger,
) -> int:
    del ctx, logger
    result = connection_secret_gc_apply_command_result(
        namespace=args.namespace,
        minimum_age_seconds=args.minimum_age_seconds,
        page_size=args.page_size,
        max_delete_count=args.max_delete_count,
        actor=args.actor,
        allowed_actors=tuple(args.allowed_actor),
        confirm_delete=args.confirm_delete,
        auth_mode=args.kube_auth,
        kube_context=args.kube_context,
    )
    emit_self_service_result(result, args.format, command="airflow_connection_secret_gc_apply")
    return result.exit_code if result.exit_code is not None else (0 if result.passed else 1)


def _add_common_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--namespace", required=True, help="One Kubernetes namespace")
    parser.add_argument("--minimum-age-seconds", type=int, default=86_400, help="Age floor; 300..2592000")
    parser.add_argument("--page-size", type=int, default=500, help="Metadata page size; 1..1000")
    parser.add_argument(
        "--kube-auth",
        choices=["auto", "in-cluster", "kubeconfig"],
        default="auto",
        help="Kubernetes authentication source; defaults to auto",
    )
    parser.add_argument("--kube-context", help="Optional kubeconfig context; incompatible with in-cluster")
    parser.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Topology-free text or schema-validated JSON; defaults to text",
    )


__all__ = [
    "cmd_airflow_connection_secret_gc_apply",
    "cmd_airflow_connection_secret_gc_plan",
    "register_connection_secret_gc_apply_parser",
    "register_connection_secret_gc_plan_parser",
]
