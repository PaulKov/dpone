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
from dpone.readiness.schema_migration_post_apply import render_post_apply_markdown, render_post_apply_table


def post_apply_group() -> Command:
    subcommands = [
        FuncCommand("plan", register_plan_parser, cmd_post_apply_plan),
        FuncCommand("run", register_run_parser, cmd_post_apply_run),
        FuncCommand("certify", register_certify_parser, cmd_post_apply_certify),
        FuncCommand("report", register_report_parser, cmd_post_apply_report),
    ]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("post-apply", help="Verify applied schema migrations in target")

    return CommandGroup(
        name="post-apply",
        help="Verify applied schema migrations in target",
        build_parser=build,
        subcommands=subcommands,
        subdest="schema_migration_post_apply_cmd",
    )


def cmd_post_apply_plan(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().plan(
        pack_path=args.pack,
        bundle_path=args.bundle,
        ledger_path=args.ledger,
        manifest_path=args.manifest,
        target_connection_path=args.target_connection,
        environment=args.environment,
    )
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_post_apply_run(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().run(plan_path=args.plan, execute=args.execute)
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_post_apply_certify(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().certify(run_path=args.run, profile=args.profile)
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_post_apply_report(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().report(certificate_path=args.certificate)
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def register_plan_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("plan", help="Build a post-apply verification plan")
    parser.add_argument("--pack", required=True, help="Migration pack JSON/YAML path")
    parser.add_argument("--bundle", help="Optional migration bundle JSON/YAML path")
    parser.add_argument("--ledger", required=True, help="Migration ledger JSON/YAML path")
    parser.add_argument("--manifest", required=True, help="Route manifest JSON/YAML path")
    parser.add_argument("--target-connection", required=True, help="Target connection JSON/YAML path")
    parser.add_argument("--environment", required=True, help="Applied target environment")
    _add_output_args(parser)
    return parser


def register_run_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("run", help="Run a post-apply verification plan")
    parser.add_argument("--plan", required=True, help="post-apply-plan.json path")
    parser.add_argument("--execute", action="store_true", help="Execute read-only target checks")
    _add_output_args(parser)
    return parser


def register_certify_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("certify", help="Create a post-apply verification certificate")
    parser.add_argument("--run", required=True, help="post-apply-run.json path")
    parser.add_argument(
        "--profile",
        choices=["advisory", "stage", "prod_strict", "regulated"],
        default="stage",
        help="Post-apply certification policy profile",
    )
    _add_output_args(parser)
    return parser


def register_report_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("report", help="Render a post-apply certificate report")
    parser.add_argument("--certificate", required=True, help="post-apply-certificate.json path")
    _add_output_args(parser)
    return parser


def _add_output_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--format", choices=["text", "json", "md", "table"], default="text")
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
        return str(payload.get("markdown") or render_post_apply_markdown(payload))
    if output_format == "table":
        return render_post_apply_table(payload)
    return _render_text(payload)


def _render_text(payload: dict[str, Any]) -> str:
    lines = [
        str(payload.get("schema_version", "schema_migration_post_apply")),
        f"- status: {payload.get('status')}",
        f"- pack_id: {payload.get('pack_id')}",
    ]
    if payload.get("bundle_id"):
        lines.append(f"- bundle_id: {payload.get('bundle_id')}")
    if payload.get("environment"):
        lines.append(f"- environment: {payload.get('environment')}")
    for key in ("blockers", "warnings"):
        values = payload.get(key, [])
        if isinstance(values, list) and values:
            lines.append(f"- {key}:")
            lines.extend(f"  - {item}" for item in values)
    return "\n".join(lines) + "\n"


def _facade() -> Any:
    module = import_module("dpone.services.schema_migration_post_apply")
    return module.PostApplyVerificationFacade()


__all__ = ["post_apply_group"]
