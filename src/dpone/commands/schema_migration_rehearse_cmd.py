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
from dpone.readiness.schema_migration_rehearsal import render_rehearsal_markdown, render_rehearsal_table


def rehearse_group() -> Command:
    subcommands = [
        FuncCommand("plan", register_plan_parser, cmd_rehearsal_plan),
        FuncCommand("run", register_run_parser, cmd_rehearsal_run),
        FuncCommand("certify", register_certify_parser, cmd_rehearsal_certify),
        FuncCommand("report", register_report_parser, cmd_rehearsal_report),
        fixture_group(),
    ]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("rehearse", help="Plan and certify schema migration rehearsal runs")

    return CommandGroup(
        name="rehearse",
        help="Plan and certify schema migration rehearsal runs",
        build_parser=build,
        subcommands=subcommands,
        subdest="schema_migration_rehearsal_cmd",
    )


def cmd_rehearsal_plan(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().plan(
        pack_path=args.pack,
        bundle_path=args.bundle,
        environment=args.environment,
        target_connection_path=args.target_connection,
        output_dir=args.output_dir,
    )
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_rehearsal_run(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().run(plan_path=args.plan, execute=args.execute)
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_rehearsal_certify(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().certify(
        run_path=args.run,
        profile=args.profile,
        fixture_build_path=args.fixture_build,
        before_profile_path=args.before_profile,
        after_profile_path=args.after_profile,
    )
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_rehearsal_report(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().report(certificate_path=args.certificate)
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def register_plan_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("plan", help="Build a schema migration rehearsal plan")
    parser.add_argument("--pack", required=True, help="Migration pack JSON path")
    parser.add_argument("--bundle", help="Optional bundle.json path")
    parser.add_argument("--environment", required=True, help="Sandbox/stage environment name")
    parser.add_argument("--target-connection", required=True, help="Target connection JSON path")
    parser.add_argument("--output-dir", required=True, help="Directory where rehearsal-plan.json is written")
    _add_output_args(parser)
    return parser


def register_run_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("run", help="Run a schema migration rehearsal plan")
    parser.add_argument("--plan", required=True, help="rehearsal-plan.json path")
    parser.add_argument("--execute", action="store_true", help="Execute target DDL/DML; omitted means dry-run")
    _add_output_args(parser)
    return parser


def register_certify_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("certify", help="Create a rehearsal certificate from a rehearsal run")
    parser.add_argument("--run", required=True, help="rehearsal-run.json path")
    parser.add_argument("--fixture-build", help="Optional fixture-build.json evidence")
    parser.add_argument("--before-profile", help="Optional before data-profile.json evidence")
    parser.add_argument("--after-profile", help="Optional after data-profile.json evidence")
    parser.add_argument(
        "--profile",
        choices=["advisory", "stage", "prod_strict", "regulated"],
        default="stage",
        help="Rehearsal certification policy profile",
    )
    _add_output_args(parser)
    return parser


def fixture_group() -> Command:
    subcommands = [
        FuncCommand("plan", register_fixture_plan_parser, cmd_fixture_plan),
        FuncCommand("build", register_fixture_build_parser, cmd_fixture_build),
        FuncCommand("profile", register_fixture_profile_parser, cmd_fixture_profile),
    ]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("fixture", help="Plan, build and profile rehearsal data fixtures")

    return CommandGroup(
        name="fixture",
        help="Plan, build and profile rehearsal data fixtures",
        build_parser=build,
        subcommands=subcommands,
        subdest="schema_migration_rehearsal_fixture_cmd",
    )


def cmd_fixture_plan(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _data_facade().plan(pack_path=args.pack, manifest_path=args.manifest, policy_path=args.policy)
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_fixture_build(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _data_facade().build(
        plan_path=args.plan,
        target_connection_path=args.target_connection,
        execute=args.execute,
    )
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_fixture_profile(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _data_facade().profile(
        fixture_build_path=args.fixture_build,
        target_connection_path=args.target_connection,
        stage=args.stage,
    )
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def register_fixture_plan_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("plan", help="Build a rehearsal data fixture plan")
    parser.add_argument("--pack", required=True, help="Migration pack JSON path")
    parser.add_argument("--manifest", required=True, help="Route manifest YAML/JSON path")
    parser.add_argument("--policy", help="Optional fixture policy YAML/JSON path")
    _add_output_args(parser)
    return parser


def register_fixture_build_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("build", help="Build or seed rehearsal data fixture rows")
    parser.add_argument("--plan", required=True, help="fixture-plan.json path")
    parser.add_argument("--target-connection", required=True, help="Sandbox target connection JSON/YAML path")
    parser.add_argument("--execute", action="store_true", help="Seed fixture rows into the sandbox target")
    _add_output_args(parser)
    return parser


def register_fixture_profile_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("profile", help="Profile rehearsal fixture data")
    parser.add_argument("--fixture-build", required=True, help="fixture-build.json path")
    parser.add_argument("--target-connection", required=True, help="Sandbox target connection JSON/YAML path")
    parser.add_argument("--stage", choices=["before", "after"], required=True, help="Profile stage")
    _add_output_args(parser)
    return parser


def register_report_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("report", help="Render a rehearsal certificate report")
    parser.add_argument("--certificate", required=True, help="rehearsal-certificate.json path")
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
        return str(payload.get("markdown") or render_rehearsal_markdown(payload))
    if output_format == "table":
        return render_rehearsal_table(payload)
    return _render_text(payload)


def _render_text(payload: dict[str, Any]) -> str:
    lines = [
        str(payload.get("schema_version", "schema_migration_rehearsal")),
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
    module = import_module("dpone.services.schema_migration_rehearsal")
    return module.MigrationRehearsalFacade()


def _data_facade() -> Any:
    module = import_module("dpone.services.schema_migration_fixture")
    return module.MigrationRehearsalDataFacade()


__all__ = ["rehearse_group"]
