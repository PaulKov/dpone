from __future__ import annotations

import argparse
import json
import logging
from importlib import import_module
from pathlib import Path
from typing import Any

from dpone.commands.base import Command
from dpone.commands.func_command import CommandGroup, FuncCommand
from dpone.commands.output_json import dumps_json
from dpone.commands.output_text import write_text


def enforcement_group() -> Command:
    subcommands = [
        FuncCommand("plan", _plan_parser, _cmd_plan),
        FuncCommand("apply", _apply_parser, _cmd_apply),
        FuncCommand("certify", _certify_parser, _cmd_certify),
        FuncCommand("report", _report_parser, _cmd_report),
    ]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("enforcement", help="Plan and certify target access enforcement")

    return CommandGroup(
        name="enforcement",
        help="Plan and certify target access enforcement",
        build_parser=build,
        subcommands=subcommands,
        subdest="data_product_access_enforcement_cmd",
    )


def drift_group() -> Command:
    subcommands = [FuncCommand("inspect", _drift_inspect_parser, _cmd_drift_inspect)]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("drift", help="Inspect access enforcement drift")

    return CommandGroup(
        name="drift",
        help="Inspect access enforcement drift",
        build_parser=build,
        subcommands=subcommands,
        subdest="data_product_access_drift_cmd",
    )


def _cmd_plan(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().plan(
        manifest_path=args.manifest,
        classification_path=args.classification,
        entitlement_plan_path=args.entitlement_plan,
        privacy_impact_path=args.privacy_impact,
        access_gate_path=args.access_gate,
        authority_gate_path=args.authority_gate,
        target_connection_path=args.target_connection,
        environment=args.environment,
    )
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def _cmd_apply(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().apply(
        plan_path=args.plan,
        approval_path=args.approval,
        execute=args.execute,
        target_connection_path=args.target_connection,
    )
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "failed" else 0


def _cmd_drift_inspect(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().drift_inspect(plan_path=args.plan, target_connection_path=args.target_connection)
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def _cmd_certify(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().certify(run_path=args.run, drift_report_path=args.drift_report, profile=args.profile)
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def _cmd_report(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().report(certificate_path=args.certificate)
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def _plan_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("plan", help="Build target-bound access enforcement plan")
    parser.add_argument("--manifest", required=True, help="Manifest path")
    parser.add_argument("--classification", required=True, help="Access classification artifact")
    parser.add_argument("--entitlement-plan", required=True, help="Entitlement plan artifact")
    parser.add_argument("--privacy-impact", required=True, help="Privacy impact artifact")
    parser.add_argument("--access-gate", help="Optional access gate artifact")
    parser.add_argument("--authority-gate", help="Optional authority gate artifact")
    parser.add_argument("--target-connection", required=True, help="Target connection JSON/YAML")
    parser.add_argument("--environment", required=True, help="Environment name")
    _add_output_args(parser)
    return parser


def _apply_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("apply", help="Apply access enforcement operations")
    parser.add_argument("--plan", required=True, help="Access enforcement plan artifact")
    parser.add_argument("--approval", help="Optional approval artifact")
    parser.add_argument("--target-connection", help="Target connection JSON/YAML, required with --execute")
    parser.add_argument("--execute", action="store_true", help="Mutate target access state")
    _add_output_args(parser)
    return parser


def _drift_inspect_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("inspect", help="Inspect target access drift")
    parser.add_argument("--plan", required=True, help="Access enforcement plan artifact")
    parser.add_argument("--target-connection", required=True, help="Target connection or access-state artifact")
    _add_output_args(parser)
    return parser


def _certify_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("certify", help="Certify access enforcement run and drift evidence")
    parser.add_argument("--run", required=True, help="Access enforcement run artifact")
    parser.add_argument("--drift-report", required=True, help="Access drift report artifact")
    parser.add_argument("--target-connection", help="Optional target connection JSON/YAML")
    parser.add_argument(
        "--profile",
        choices=["advisory", "stage", "prod_strict", "regulated"],
        default="prod_strict",
        help="Certification profile",
    )
    _add_output_args(parser)
    return parser


def _report_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("report", help="Render access enforcement report")
    parser.add_argument("--certificate", required=True, help="Access enforcement certificate artifact")
    _add_output_args(parser, default="md")
    return parser


def _add_output_args(parser: argparse.ArgumentParser, *, default: str = "text") -> None:
    parser.add_argument("--format", choices=["text", "json", "md", "table"], default=default)
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
        return str(payload.get("markdown") or _render_md(payload))
    if output_format == "table":
        return _render_table(payload)
    return _render_text(payload)


def _render_text(payload: dict[str, Any]) -> str:
    lines = [str(payload.get("schema_version", "dpone.data_product_access_enforcement"))]
    for key in (
        "status",
        "product_id",
        "access_enforcement_plan_id",
        "access_enforcement_run_id",
        "access_drift_report_id",
        "access_enforcement_certificate_id",
        "access_report_id",
    ):
        if payload.get(key) is not None:
            lines.append(f"- {key}: {payload.get(key)}")
    return "\n".join(lines) + "\n"


def _render_md(payload: dict[str, Any]) -> str:
    return "# Data Product Access Enforcement\n\n```json\n" + json.dumps(payload, indent=2) + "\n```\n"


def _render_table(payload: dict[str, Any]) -> str:
    rows = payload.get("operations") or payload.get("blockers") or [payload]
    lines = ["data product access enforcement", "name | status | type", "--- | --- | ---"]
    for item in rows:
        if isinstance(item, dict):
            lines.append(
                f"{item.get('name', '')} | {item.get('status', payload.get('status'))} | {item.get('requirement_type', '')}"
            )
        else:
            lines.append(f"{item} | blocked | blocker")
    return "\n".join(lines) + "\n"


def _facade() -> Any:
    return import_module("dpone.services.data_product_access_enforcement").DataProductAccessEnforcementFacade()


__all__ = ["drift_group", "enforcement_group"]
