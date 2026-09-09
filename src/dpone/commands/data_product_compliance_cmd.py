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


def compliance_group() -> Command:
    subcommands = [controls_group(), audit_group()]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("compliance", help="Map data product evidence to compliance controls")

    return CommandGroup(
        name="compliance",
        help="Map data product evidence to compliance controls",
        build_parser=build,
        subcommands=subcommands,
        subdest="data_product_compliance_cmd",
    )


def controls_group() -> Command:
    subcommands = [
        FuncCommand("plan", _plan_parser, _cmd_plan),
        FuncCommand("evaluate", _evaluate_parser, _cmd_evaluate),
        FuncCommand("gate", _gate_parser, _cmd_gate),
    ]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("controls", help="Plan, evaluate and gate compliance controls")

    return CommandGroup(
        name="controls",
        help="Plan, evaluate and gate compliance controls",
        build_parser=build,
        subcommands=subcommands,
        subdest="data_product_compliance_controls_cmd",
    )


def audit_group() -> Command:
    subcommands = [FuncCommand("package", _package_parser, _cmd_package)]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("audit", help="Render compliance audit evidence packages")

    return CommandGroup(
        name="audit",
        help="Render compliance audit evidence packages",
        build_parser=build,
        subcommands=subcommands,
        subdest="data_product_compliance_audit_cmd",
    )


def _cmd_plan(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().plan(
        manifest_path=args.manifest,
        bundle_path=args.bundle,
        registry_path=args.registry,
        evidence_dir=args.evidence_dir,
    )
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def _cmd_evaluate(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().evaluate(plan_path=args.plan, observed_at=args.observed_at)
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def _cmd_gate(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().gate(evaluation_path=args.evaluation, profile=args.profile)
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def _cmd_package(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().audit_package(gate_path=args.gate)
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def _plan_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("plan", help="Build a compliance control plan")
    parser.add_argument("--manifest", required=True, help="Manifest path")
    parser.add_argument("--bundle", help="Optional schema migration bundle JSON/YAML")
    parser.add_argument("--registry", help="Optional evidence registry JSON/SQLite")
    parser.add_argument("--evidence-dir", help="Optional directory with local evidence artifacts")
    _add_output_args(parser)
    return parser


def _evaluate_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("evaluate", help="Evaluate a compliance control plan")
    parser.add_argument("--plan", required=True, help="Compliance control plan artifact")
    parser.add_argument("--observed-at", help="Optional timestamp for stale-evidence checks")
    _add_output_args(parser)
    return parser


def _gate_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("gate", help="Gate a compliance control evaluation")
    parser.add_argument("--evaluation", required=True, help="Compliance control evaluation artifact")
    parser.add_argument(
        "--profile",
        choices=["advisory", "stage", "prod_strict", "regulated"],
        default="prod_strict",
        help="Compliance gate profile",
    )
    _add_output_args(parser)
    return parser


def _package_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("package", help="Render a compliance audit package")
    parser.add_argument("--gate", required=True, help="Compliance gate artifact")
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
    lines = [str(payload.get("schema_version", "dpone.data_product_compliance"))]
    for key in (
        "status",
        "product_id",
        "compliance_plan_id",
        "compliance_evaluation_id",
        "compliance_gate_id",
        "audit_package_id",
    ):
        if payload.get(key) is not None:
            lines.append(f"- {key}: {payload.get(key)}")
    return "\n".join(lines) + "\n"


def _render_md(payload: dict[str, Any]) -> str:
    return "# Data Product Compliance\n\n```json\n" + json.dumps(payload, ensure_ascii=False, indent=2) + "\n```\n"


def _render_table(payload: dict[str, Any]) -> str:
    rows = payload.get("controls") or [payload]
    lines = ["data product compliance", "control | severity | status", "--- | --- | ---"]
    if isinstance(rows, list):
        for item in rows:
            if isinstance(item, dict):
                name = item.get("control_id") or item.get("schema_version")
                lines.append(f"{name} | {item.get('severity', '')} | {item.get('status', payload.get('status'))}")
    return "\n".join(lines) + "\n"


def _facade() -> Any:
    return import_module("dpone.services.data_product_compliance").DataProductComplianceFacade()


__all__ = ["compliance_group"]
