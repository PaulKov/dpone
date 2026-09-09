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


def assertions_group() -> Command:
    subcommands = [
        FuncCommand("plan", register_plan_parser, cmd_plan),
        FuncCommand("evaluate", register_evaluate_parser, cmd_evaluate),
        FuncCommand("gate", register_gate_parser, cmd_gate),
        FuncCommand("report", register_report_parser, cmd_report),
    ]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("assertions", help="Plan, evaluate and gate data product assertions")

    return CommandGroup(
        name="assertions",
        help="Plan, evaluate and gate data product assertions",
        build_parser=build,
        subcommands=subcommands,
        subdest="data_product_assertions_cmd",
    )


def cmd_plan(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().plan(manifest_path=args.manifest)
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_evaluate(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().evaluate(
        plan_path=args.plan,
        runtime_artifact_path=args.runtime_artifact,
        target_connection_path=args.target_connection,
        target_evidence_path=args.target_evidence,
    )
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_gate(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().gate(evaluation_path=args.evaluation, profile=args.profile)
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_report(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().report(evaluation_path=args.evaluation)
    _emit(payload, args.format, args.output)
    return 0


def register_plan_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("plan", help="Build a data product assertion plan")
    parser.add_argument("--manifest", required=True, help="Manifest path")
    _add_output_args(parser, formats=("text", "json", "md", "table"))
    return parser


def register_evaluate_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("evaluate", help="Evaluate data product assertion evidence")
    parser.add_argument("--plan", required=True, help="Assertion plan artifact")
    parser.add_argument("--runtime-artifact", help="Optional runtime/run evidence JSON/YAML")
    parser.add_argument("--target-connection", help="Optional read-only target connection JSON/YAML")
    parser.add_argument("--target-evidence", help="Optional precomputed read-only target evidence JSON/YAML")
    _add_output_args(parser, formats=("text", "json", "md", "table"))
    return parser


def register_gate_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("gate", help="Gate data product assertion evaluation")
    parser.add_argument("--evaluation", required=True, help="Assertion evaluation artifact")
    parser.add_argument(
        "--profile",
        choices=["advisory", "stage", "prod_strict", "regulated"],
        default="prod_strict",
        help="Assertion gate profile",
    )
    _add_output_args(parser, formats=("text", "json", "md", "table"))
    return parser


def register_report_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("report", help="Render a data product assertion report")
    parser.add_argument("--evaluation", required=True, help="Assertion evaluation artifact")
    _add_output_args(parser, formats=("text", "json", "md", "table"), default="md")
    return parser


def _add_output_args(
    parser: argparse.ArgumentParser,
    *,
    formats: tuple[str, ...],
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
        return str(
            payload.get("markdown")
            or "# Data Product Assertions\n\n```json\n" + json.dumps(payload, indent=2) + "\n```\n"
        )
    if output_format == "table":
        return _render_table(payload)
    return _render_text(payload)


def _render_text(payload: dict[str, Any]) -> str:
    keys = ("status", "product_id", "assertion_plan_id", "assertion_evaluation_id", "assertion_gate_id")
    lines = [str(payload.get("schema_version", "dpone.data_product_assertions"))]
    lines.extend(f"- {key}: {payload[key]}" for key in keys if payload.get(key) is not None)
    for key in ("blockers", "warnings"):
        values = payload.get(key, [])
        if isinstance(values, list) and values:
            lines.append(f"- {key}:")
            lines.extend(f"  - {item}" for item in values)
    return "\n".join(lines) + "\n"


def _render_table(payload: dict[str, Any]) -> str:
    assertions = payload.get("assertions") or [payload]
    lines = ["data product assertions", "assertion | status | severity", "--- | --- | ---"]
    if isinstance(assertions, list):
        for item in assertions:
            if isinstance(item, dict):
                lines.append(
                    f"{item.get('id', payload.get('schema_version'))} | {item.get('status', payload.get('status'))} | {item.get('severity', '')}"
                )
    return "\n".join(lines) + "\n"


def _facade() -> Any:
    return import_module("dpone.services.data_product_assertions").DataProductAssertionFacade()


__all__ = ["assertions_group"]
