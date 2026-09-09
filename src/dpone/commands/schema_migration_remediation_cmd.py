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
from dpone.readiness.schema_migration_remediation_rendering import (
    render_remediation_markdown,
    render_remediation_table,
)


def remediation_group() -> Command:
    subcommands = [
        FuncCommand("plan", register_plan_parser, cmd_remediation_plan),
        FuncCommand("apply", register_apply_parser, cmd_remediation_apply),
        FuncCommand("certify", register_certify_parser, cmd_remediation_certify),
        FuncCommand("report", register_report_parser, cmd_remediation_report),
    ]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("remediation", help="Plan and execute controlled schema migration remediation")

    return CommandGroup(
        name="remediation",
        help="Plan and execute controlled schema migration remediation",
        build_parser=build,
        subcommands=subcommands,
        subdest="schema_migration_remediation_cmd",
    )


def cmd_remediation_plan(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().plan(
        pack_path=args.pack,
        watch_certificate_path=args.watch_certificate,
        ledger_path=args.ledger,
        manifest_path=args.manifest,
        target_connection_path=args.target_connection,
        environment=args.environment,
        backup_certificate_path=args.backup_certificate,
    )
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_remediation_apply(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().apply(plan_path=args.plan, approval_path=args.approval, execute=args.execute)
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") in {"blocked", "failed"} else 0


def cmd_remediation_certify(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().certify(
        run_path=args.run,
        target_connection_path=args.target_connection,
        profile=args.profile,
    )
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_remediation_report(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().report(certificate_path=args.certificate)
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def register_plan_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("plan", help="Build a controlled remediation plan")
    parser.add_argument("--pack", required=True, help="Migration pack JSON/YAML path")
    parser.add_argument("--watch-certificate", required=True, help="Release watch certificate JSON/YAML path")
    parser.add_argument("--ledger", required=True, help="Migration ledger JSON path")
    parser.add_argument("--manifest", required=True, help="Route manifest JSON/YAML path")
    parser.add_argument("--target-connection", required=True, help="Target connection JSON/YAML path")
    parser.add_argument("--environment", required=True, help="Environment name")
    parser.add_argument("--backup-certificate", help="Optional backup certificate JSON/YAML for restore fallback")
    _add_output_args(parser)
    return parser


def register_apply_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("apply", help="Apply a controlled remediation plan")
    parser.add_argument("--plan", required=True, help="remediation-plan.json path")
    parser.add_argument("--approval", help="Approval JSON/YAML artifact for protected remediation")
    parser.add_argument("--execute", action="store_true", help="Execute target rollback/remediation operations")
    _add_output_args(parser)
    return parser


def register_certify_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("certify", help="Create a remediation certificate")
    parser.add_argument("--run", required=True, help="remediation-run.json path")
    parser.add_argument("--target-connection", help="Target connection JSON/YAML path for future read-back checks")
    parser.add_argument(
        "--profile",
        choices=["advisory", "stage", "prod_strict", "regulated"],
        default="stage",
        help="Remediation certification policy profile",
    )
    _add_output_args(parser)
    return parser


def register_report_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("report", help="Render a remediation certificate report")
    parser.add_argument("--certificate", required=True, help="remediation-certificate.json path")
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
        return str(payload.get("markdown") or render_remediation_markdown(payload))
    if output_format == "table":
        return render_remediation_table(payload)
    return _render_text(payload)


def _render_text(payload: dict[str, Any]) -> str:
    lines = [
        str(payload.get("schema_version", "schema_migration_remediation")),
        f"- status: {payload.get('status')}",
        f"- pack_id: {payload.get('pack_id')}",
    ]
    for key in ("watch_certificate_id", "environment", "capability"):
        if payload.get(key):
            lines.append(f"- {key}: {payload.get(key)}")
    for key in ("blockers", "warnings"):
        values = payload.get(key, [])
        if isinstance(values, list) and values:
            lines.append(f"- {key}:")
            lines.extend(f"  - {item}" for item in values)
    return "\n".join(lines) + "\n"


def _facade() -> Any:
    module = import_module("dpone.services.schema_migration_remediation")
    return module.MigrationRemediationFacade()


__all__ = ["remediation_group"]
