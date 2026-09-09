from __future__ import annotations

import argparse
import logging
from importlib import import_module
from pathlib import Path
from typing import Any

from dpone.commands.base import Command
from dpone.commands.func_command import CommandGroup, FuncCommand
from dpone.commands.output_json import dumps_json
from dpone.commands.output_text import write_text
from dpone.readiness.schema_migration_recovery_rendering import (
    render_recovery_markdown,
    render_recovery_table,
)


def recovery_group() -> Command:
    subcommands = [point_group(), chain_group(), restore_group(), retention_group()]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("recovery", help="Record, verify, and restore migration recovery points")

    return CommandGroup(
        name="recovery",
        help="Record, verify, and restore migration recovery points",
        build_parser=build,
        subcommands=subcommands,
        subdest="schema_migration_recovery_cmd",
    )


def point_group() -> Command:
    subcommands = [
        FuncCommand("record", register_point_record_parser, cmd_point_record),
        FuncCommand("latest", register_point_latest_parser, cmd_point_latest),
        FuncCommand("list", register_point_list_parser, cmd_point_list),
    ]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("point", help="Record and query cataloged restore points")

    return CommandGroup(
        name="point",
        help="Record and query cataloged restore points",
        build_parser=build,
        subcommands=subcommands,
        subdest="schema_migration_recovery_point_cmd",
    )


def chain_group() -> Command:
    subcommands = [FuncCommand("verify", register_chain_verify_parser, cmd_chain_verify)]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("chain", help="Verify full/incremental recovery chains")

    return CommandGroup(
        name="chain",
        help="Verify full/incremental recovery chains",
        build_parser=build,
        subcommands=subcommands,
        subdest="schema_migration_recovery_chain_cmd",
    )


def restore_group() -> Command:
    subcommands = [
        FuncCommand("plan", register_restore_plan_parser, cmd_restore_plan),
        FuncCommand("run", register_restore_run_parser, cmd_restore_run),
        FuncCommand("certify", register_restore_certify_parser, cmd_restore_certify),
        FuncCommand("report", register_restore_report_parser, cmd_restore_report),
    ]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("restore", help="Plan, run, and certify recovery restores")

    return CommandGroup(
        name="restore",
        help="Plan, run, and certify recovery restores",
        build_parser=build,
        subcommands=subcommands,
        subdest="schema_migration_recovery_restore_cmd",
    )


def retention_group() -> Command:
    subcommands = [FuncCommand("plan", register_retention_plan_parser, cmd_retention_plan)]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("retention", help="Plan recovery point retention cleanup")

    return CommandGroup(
        name="retention",
        help="Plan recovery point retention cleanup",
        build_parser=build,
        subcommands=subcommands,
        subdest="schema_migration_recovery_retention_cmd",
    )


def cmd_point_record(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().point_record(
        backup_certificate_path=args.backup_certificate,
        environment=args.environment,
        store_backend=args.store_backend,
        store_uri=args.store_uri,
        mode=args.mode,
    )
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_point_latest(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().point_latest(
        target=args.target,
        environment=args.environment,
        profile=args.profile,
        store_backend=args.store_backend,
        store_uri=args.store_uri,
    )
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_point_list(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().point_list(
        target=args.target,
        environment=args.environment,
        store_backend=args.store_backend,
        store_uri=args.store_uri,
    )
    _emit(payload, args.format, args.output)
    return 0


def cmd_chain_verify(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().chain_verify(
        restore_point_id=args.restore_point_id,
        store_backend=args.store_backend,
        store_uri=args.store_uri,
        require_restore_rehearsal=args.require_restore_rehearsal,
    )
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_restore_plan(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().restore_plan(
        restore_point_id=args.restore_point_id,
        target_connection_path=args.target_connection,
        environment=args.environment,
        store_backend=args.store_backend,
        store_uri=args.store_uri,
    )
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_restore_run(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().restore_run(plan_path=args.plan, execute=args.execute)
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") in {"blocked", "failed"} else 0


def cmd_restore_certify(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().restore_certify(restore_run_path=args.restore_run, profile=args.profile)
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_restore_report(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().restore_report(certificate_path=args.certificate)
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_retention_plan(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().retention_plan(
        target=args.target,
        environment=args.environment,
        store_backend=args.store_backend,
        store_uri=args.store_uri,
    )
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def register_point_record_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("record", help="Record a backup certificate as a recovery point")
    parser.add_argument("--backup-certificate", required=True, help="backup-certificate.json path")
    parser.add_argument("--environment", required=True, help="Environment name")
    parser.add_argument("--mode", choices=["observe", "gate"], default="gate")
    _add_store_args(parser)
    _add_output_args(parser)
    return parser


def register_point_latest_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("latest", help="Read latest usable recovery point")
    parser.add_argument("--target", required=True, help="Target key, e.g. clickhouse.analytics.orders")
    parser.add_argument("--environment", required=True, help="Environment name")
    parser.add_argument("--profile", choices=["advisory", "stage", "prod_strict", "regulated"], default="stage")
    _add_store_args(parser)
    _add_output_args(parser, formats=("text", "json", "md", "table"))
    return parser


def register_point_list_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("list", help="List recovery points")
    parser.add_argument("--target", help="Target key, e.g. clickhouse.analytics.orders")
    parser.add_argument("--environment", help="Environment name")
    _add_store_args(parser)
    _add_output_args(parser, formats=("text", "json", "md", "table"))
    return parser


def register_chain_verify_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("verify", help="Verify recovery chain completeness")
    parser.add_argument("--restore-point-id", required=True, help="Restore point id")
    parser.add_argument("--require-restore-rehearsal", action="store_true")
    _add_store_args(parser)
    _add_output_args(parser)
    return parser


def register_restore_plan_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("plan", help="Build non-prod restore plan for a recovery point")
    parser.add_argument("--restore-point-id", required=True, help="Restore point id")
    parser.add_argument("--target-connection", required=True, help="Target connection JSON/YAML path")
    parser.add_argument("--environment", required=True, help="Sandbox/stage environment name")
    _add_store_args(parser)
    _add_output_args(parser)
    return parser


def register_restore_run_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("run", help="Run recovery restore plan")
    parser.add_argument("--plan", required=True, help="recovery-restore-plan.json path")
    parser.add_argument("--execute", action="store_true", help="Execute target restore operations")
    _add_output_args(parser)
    return parser


def register_restore_certify_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("certify", help="Certify recovery restore run")
    parser.add_argument("--restore-run", required=True, help="recovery-restore-run.json path")
    parser.add_argument("--profile", choices=["advisory", "stage", "prod_strict", "regulated"], default="stage")
    _add_output_args(parser)
    return parser


def register_restore_report_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("report", help="Render recovery restore certificate report")
    parser.add_argument("--certificate", required=True, help="recovery-restore-certificate.json path")
    _add_output_args(parser)
    return parser


def register_retention_plan_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("plan", help="Build recovery retention plan")
    parser.add_argument("--target", required=True, help="Target key, e.g. clickhouse.analytics.orders")
    parser.add_argument("--environment", required=True, help="Environment name")
    _add_store_args(parser)
    _add_output_args(parser, formats=("text", "json", "md", "table"))
    return parser


def _add_store_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--store-backend", choices=["local_json", "sqlite"], default="local_json")
    parser.add_argument("--store-uri", help="Recovery catalog path or SQLite database path")


def _add_output_args(
    parser: argparse.ArgumentParser,
    *,
    formats: tuple[str, ...] = ("text", "json", "md"),
    default: str = "text",
) -> None:
    parser.add_argument("--format", choices=list(formats), default=default)
    parser.add_argument("--output", help="Optional output artifact path")


def _emit(payload: dict[str, Any], output_format: str, output_path: str | None) -> None:
    rendered = _render(payload, output_format)
    if output_path:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(rendered, encoding="utf-8")
    write_text(rendered)


def _render(payload: dict[str, Any], output_format: str) -> str:
    if output_format == "json":
        return dumps_json(payload)
    if output_format == "md":
        return render_recovery_markdown(payload)
    if output_format == "table":
        return render_recovery_table(payload)
    return _render_text(payload)


def _render_text(payload: dict[str, Any]) -> str:
    lines = [str(payload.get("schema_version", "schema_migration_recovery"))]
    for key in ("status", "pack_id", "restore_point_id", "chain_verification_id", "environment", "destination"):
        if payload.get(key):
            lines.append(f"- {key}: {payload.get(key)}")
    for key in ("blockers", "warnings"):
        values = payload.get(key, [])
        if isinstance(values, list) and values:
            lines.append(f"- {key}:")
            lines.extend(f"  - {item}" for item in values)
    return "\n".join(lines) + "\n"


def _facade() -> Any:
    module = import_module("dpone.services.schema_migration_recovery")
    return module.MigrationRecoveryFacade()


__all__ = ["recovery_group"]
