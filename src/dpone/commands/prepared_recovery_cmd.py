"""Operator CLI for fail-closed, original-operation ClickHouse recovery."""

from __future__ import annotations

import argparse
import logging

from dpone.app.prepared_recovery_composition import build_prepared_recovery_runtime
from dpone.commands.func_command import CommandGroup, FuncCommand
from dpone.commands.output_json import write_json
from dpone.contracts.clickhouse_cluster_publication import ClusterPublicationError, digest_payload


def prepared_recovery_group() -> CommandGroup:
    """Expose plan/execute as two named steps with the same required identity."""
    return CommandGroup(
        name="clickhouse-prepared-recovery",
        help="Guarded recovery of an original PREPARED cluster publication",
        build_parser=lambda sub: sub.add_parser("clickhouse-prepared-recovery"),
        subcommands=(
            FuncCommand("plan", _register_plan, cmd_prepared_recovery, _requires_app_context=False),
            FuncCommand("execute", _register_execute, cmd_prepared_recovery, _requires_app_context=False),
        ),
        subdest="prepared_recovery_action",
    )


def _register_plan(sub: argparse._SubParsersAction) -> argparse.ArgumentParser:
    return _register_common(sub, "plan")


def _register_execute(sub: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = _register_common(sub, "execute")
    parser.add_argument("--confirmation-digest", required=True)
    return parser


def _register_common(sub: argparse._SubParsersAction, action: str) -> argparse.ArgumentParser:
    parser = sub.add_parser(action)
    for option in (
        "binding-set",
        "connection-registry",
        "connection-ref",
        "cluster",
        "database",
        "target",
        "operation-id",
        "operation-started-at",
    ):
        parser.add_argument(f"--{option}", required=True)
    parser.add_argument("--authority-version", required=True, type=int)
    parser.add_argument("--plan-file", required=True)
    parser.add_argument("--format", choices=("json",), default="json")
    return parser


def cmd_prepared_recovery(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    """Print only redacted proof IDs; never surface connector or SQL errors."""
    del ctx, logger
    try:
        runtime = build_prepared_recovery_runtime(args)
        if args.prepared_recovery_action == "plan":
            plan = runtime.plan(args)
            runtime.save_plan(plan, args.plan_file)
            write_json(plan.to_public_dict())
            return 0
        plan = runtime.load_plan(args, args.plan_file)
        if args.confirmation_digest != plan.plan_digest:
            write_json({"status": "blocked", "code": "DPONE_CLICKHOUSE_CLUSTER_RECOVERY_PLAN_CHANGED"})
            return 2
        receipt = runtime.execute(plan, confirmation_digest=args.confirmation_digest)
        write_json(
            {
                "status": "completed",
                "operation_digest": digest_payload(receipt.authority.operation_id),
                "correlation_id": plan.token,
                "replica_summary": {"expected": plan.replica_count, "published": plan.replica_count},
            }
        )
        return 0
    except ClusterPublicationError as exc:
        write_json({"status": "blocked", "code": exc.code})
        return 1 if "UNKNOWN" in exc.code or "UNRESOLVED" in exc.code else 2
    except Exception:
        write_json({"status": "unknown", "code": "DPONE_CLICKHOUSE_CLUSTER_RECOVERY_OPERATION_FAILED"})
        return 1
