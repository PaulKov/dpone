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


def watch_group() -> Command:
    subcommands = [
        FuncCommand("plan", register_plan_parser, cmd_watch_plan),
        FuncCommand("run", register_run_parser, cmd_watch_run),
        FuncCommand("status", register_status_parser, cmd_watch_status),
        FuncCommand("certify", register_certify_parser, cmd_watch_certify),
        FuncCommand("report", register_report_parser, cmd_watch_report),
    ]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("watch", help="Watch a schema migration after post-apply verification")

    return CommandGroup(
        name="watch",
        help="Watch a schema migration after post-apply verification",
        build_parser=build,
        subcommands=subcommands,
        subdest="schema_migration_watch_cmd",
    )


def cmd_watch_plan(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().plan(
        pack_path=args.pack,
        post_apply_certificate_path=args.post_apply_certificate,
        manifest_path=args.manifest,
        target_connection_path=args.target_connection,
        environment=args.environment,
    )
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_watch_run(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().run(plan_path=args.plan, execute=args.execute)
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_watch_status(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().status(run_path=args.run)
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_watch_certify(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().certify(run_path=args.run, profile=args.profile)
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_watch_report(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().report(certificate_path=args.certificate)
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def register_plan_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("plan", help="Build a release watch plan")
    parser.add_argument("--pack", required=True, help="Migration pack JSON/YAML path")
    parser.add_argument("--post-apply-certificate", required=True, help="Post-apply certificate JSON/YAML path")
    parser.add_argument("--manifest", required=True, help="Route manifest JSON/YAML path")
    parser.add_argument("--target-connection", required=True, help="Target connection JSON/YAML path")
    parser.add_argument("--environment", required=True, help="Watched target environment")
    _add_output_args(parser)
    return parser


def register_run_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("run", help="Run a release watch plan")
    parser.add_argument("--plan", required=True, help="watch-plan.json path")
    parser.add_argument("--execute", action="store_true", help="Execute read-only target watch samples")
    _add_output_args(parser)
    return parser


def register_status_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("status", help="Render current release watch run status")
    parser.add_argument("--run", required=True, help="watch-run.json path")
    _add_output_args(parser, default="table")
    return parser


def register_certify_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("certify", help="Create a release watch certificate")
    parser.add_argument("--run", required=True, help="watch-run.json path")
    parser.add_argument(
        "--profile",
        choices=["advisory", "stage", "prod_strict", "regulated"],
        default="stage",
        help="Watch certification policy profile",
    )
    _add_output_args(parser)
    return parser


def register_report_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("report", help="Render a release watch certificate report")
    parser.add_argument("--certificate", required=True, help="watch-certificate.json path")
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
        return str(payload.get("markdown") or _renderer().render_watch_markdown(payload))
    if output_format == "table":
        return str(_renderer().render_watch_table(payload))
    return _render_text(payload)


def _render_text(payload: dict[str, Any]) -> str:
    lines = [
        str(payload.get("schema_version", "schema_migration_watch")),
        f"- status: {payload.get('status')}",
        f"- pack_id: {payload.get('pack_id')}",
    ]
    if payload.get("post_apply_certificate_id"):
        lines.append(f"- post_apply_certificate_id: {payload.get('post_apply_certificate_id')}")
    if payload.get("environment"):
        lines.append(f"- environment: {payload.get('environment')}")
    for key in ("blockers", "warnings"):
        values = payload.get(key, [])
        if isinstance(values, list) and values:
            lines.append(f"- {key}:")
            lines.extend(f"  - {item}" for item in values)
    return "\n".join(lines) + "\n"


def _facade() -> Any:
    module = import_module("dpone.services.schema_migration_watch")
    return module.MigrationWatchFacade()


def _renderer() -> Any:
    return import_module("dpone.readiness.schema_migration_watch_rendering")


__all__ = ["watch_group"]
