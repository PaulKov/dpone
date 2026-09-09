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
from dpone.readiness.schema_migration_backup_rendering import render_backup_markdown, render_backup_table


def backup_group() -> Command:
    subcommands = [
        FuncCommand("plan", register_plan_parser, cmd_backup_plan),
        FuncCommand("create", register_create_parser, cmd_backup_create),
        restore_group(),
        FuncCommand("certify", register_certify_parser, cmd_backup_certify),
        FuncCommand("report", register_report_parser, cmd_backup_report),
    ]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("backup", help="Plan, create, rehearse, and certify migration backups")

    return CommandGroup(
        name="backup",
        help="Plan, create, rehearse, and certify migration backups",
        build_parser=build,
        subcommands=subcommands,
        subdest="schema_migration_backup_cmd",
    )


def restore_group() -> Command:
    subcommands = [
        FuncCommand("plan", register_restore_plan_parser, cmd_restore_plan),
        FuncCommand("run", register_restore_run_parser, cmd_restore_run),
    ]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("restore", help="Plan and run sandbox restore rehearsal")

    return CommandGroup(
        name="restore",
        help="Plan and run sandbox restore rehearsal",
        build_parser=build,
        subcommands=subcommands,
        subdest="schema_migration_backup_restore_cmd",
    )


def cmd_backup_plan(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().plan(
        pack_path=args.pack,
        manifest_path=args.manifest,
        target_connection_path=args.target_connection,
        environment=args.environment,
    )
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_backup_create(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().create(plan_path=args.plan, approval_path=args.approval, execute=args.execute)
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") in {"blocked", "failed"} else 0


def cmd_restore_plan(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().restore_plan(
        backup_run_path=args.backup_run,
        target_connection_path=args.target_connection,
        environment=args.environment,
    )
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_restore_run(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().restore_run(plan_path=args.plan, execute=args.execute)
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") in {"blocked", "failed"} else 0


def cmd_backup_certify(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().certify(
        backup_run_path=args.backup_run,
        restore_run_path=args.restore_run,
        profile=args.profile,
    )
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_backup_report(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().report(certificate_path=args.certificate)
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def register_plan_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("plan", help="Build a migration backup plan")
    parser.add_argument("--pack", required=True, help="Migration pack JSON/YAML path")
    parser.add_argument("--manifest", required=True, help="Route manifest JSON/YAML path")
    parser.add_argument("--target-connection", required=True, help="Target connection JSON/YAML path")
    parser.add_argument("--environment", required=True, help="Environment name")
    _add_output_args(parser)
    return parser


def register_create_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("create", help="Create the planned target backup")
    parser.add_argument("--plan", required=True, help="backup-plan.json path")
    parser.add_argument("--approval", help="Approval JSON/YAML artifact for protected backups")
    parser.add_argument("--execute", action="store_true", help="Execute target backup operations")
    _add_output_args(parser)
    return parser


def register_restore_plan_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("plan", help="Build a sandbox restore rehearsal plan")
    parser.add_argument("--backup-run", required=True, help="backup-run.json path")
    parser.add_argument("--target-connection", required=True, help="Sandbox target connection JSON/YAML path")
    parser.add_argument("--environment", required=True, help="Sandbox environment name")
    _add_output_args(parser)
    return parser


def register_restore_run_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("run", help="Run a sandbox restore rehearsal")
    parser.add_argument("--plan", required=True, help="restore-plan.json path")
    parser.add_argument("--execute", action="store_true", help="Execute target restore operations")
    _add_output_args(parser)
    return parser


def register_certify_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("certify", help="Create a backup certificate")
    parser.add_argument("--backup-run", required=True, help="backup-run.json path")
    parser.add_argument("--restore-run", help="Optional restore-run.json path")
    parser.add_argument(
        "--profile",
        choices=["advisory", "stage", "prod_strict", "regulated"],
        default="stage",
        help="Backup certification policy profile",
    )
    _add_output_args(parser)
    return parser


def register_report_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("report", help="Render a backup certificate report")
    parser.add_argument("--certificate", required=True, help="backup-certificate.json path")
    _add_output_args(parser)
    return parser


def _add_output_args(
    parser: argparse.ArgumentParser,
    *,
    formats: tuple[str, ...] = ("text", "json", "md", "table"),
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
        return str(payload.get("markdown") or render_backup_markdown(payload))
    if output_format == "table":
        return render_backup_table(payload)
    return _render_text(payload)


def _render_text(payload: dict[str, Any]) -> str:
    lines = [
        str(payload.get("schema_version", "schema_migration_backup")),
        f"- status: {payload.get('status')}",
        f"- pack_id: {payload.get('pack_id')}",
    ]
    for key in ("environment", "backup_destination", "restore_table"):
        if payload.get(key):
            lines.append(f"- {key}: {payload.get(key)}")
    for key in ("blockers", "warnings"):
        values = payload.get(key, [])
        if isinstance(values, list) and values:
            lines.append(f"- {key}:")
            lines.extend(f"  - {item}" for item in values)
    return "\n".join(lines) + "\n"


def _facade() -> Any:
    module = import_module("dpone.services.schema_migration_backup")
    return module.MigrationBackupFacade()


__all__ = ["backup_group"]
