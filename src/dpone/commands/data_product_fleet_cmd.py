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


def fleet_group() -> Command:
    subcommands = [
        FuncCommand("evaluate", register_evaluate_parser, cmd_evaluate),
        FuncCommand("gate", register_gate_parser, cmd_gate),
        FuncCommand("report", register_report_parser, cmd_report),
        FuncCommand("export", register_export_parser, cmd_export),
    ]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("fleet", help="Evaluate fleet-level data product reliability")

    return CommandGroup(
        name="fleet",
        help="Evaluate fleet-level data product reliability",
        build_parser=build,
        subcommands=subcommands,
        subdest="data_product_fleet_cmd",
    )


def cmd_evaluate(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().evaluate(
        manifest_patterns=args.manifests,
        registry_path=args.registry,
        observed_at=args.observed_at,
    )
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") in {"blocked", "frozen"} else 0


def cmd_gate(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().gate(evaluation_path=args.evaluation, profile=args.profile)
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") in {"blocked", "frozen"} else 0


def cmd_report(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().report(evaluation_path=args.evaluation)
    _emit(payload, args.format, args.output)
    return 0


def cmd_export(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().export(evaluation_path=args.evaluation, target=args.target)
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def register_evaluate_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("evaluate", help="Evaluate fleet reliability from manifests and registry evidence")
    parser.add_argument("--manifests", action="append", required=True, help="Manifest path or glob pattern")
    parser.add_argument("--registry", help="Optional evidence registry JSON/SQLite")
    parser.add_argument("--observed-at", help="Deterministic evaluation timestamp")
    _add_output_args(parser, formats=("text", "json", "md", "table"))
    return parser


def register_gate_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("gate", help="Gate fleet reliability evaluation")
    parser.add_argument("--evaluation", required=True, help="Fleet evaluation artifact")
    _add_profile_arg(parser, "Fleet gate profile")
    _add_output_args(parser, formats=("text", "json", "md", "table"))
    return parser


def register_report_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("report", help="Render fleet reliability report")
    parser.add_argument("--evaluation", required=True, help="Fleet evaluation artifact")
    _add_output_args(parser, formats=("text", "json", "md", "table"), default="md")
    return parser


def register_export_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("export", help="Render offline reliability export artifact")
    parser.add_argument("--evaluation", required=True, help="Fleet evaluation artifact")
    parser.add_argument(
        "--target",
        required=True,
        choices=["prometheus", "opentelemetry", "openlineage", "datahub", "json"],
        help="Export target format",
    )
    _add_output_args(parser, formats=("text", "json"), default="json")
    return parser


def _add_profile_arg(parser: argparse.ArgumentParser, help_text: str) -> None:
    parser.add_argument(
        "--profile",
        choices=["advisory", "stage", "prod_strict", "regulated"],
        default="prod_strict",
        help=help_text,
    )


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
            or "# Data Product Fleet Reliability\n\n```json\n" + json.dumps(payload, indent=2) + "\n```\n"
        )
    if output_format == "table":
        return _render_table(payload)
    if payload.get("target") == "prometheus" and payload.get("content"):
        return str(payload["content"])
    return _render_text(payload)


def _render_text(payload: dict[str, Any]) -> str:
    keys = (
        "status",
        "fleet_evaluation_id",
        "fleet_gate_id",
        "fleet_report_id",
        "export_id",
        "route_delivery_receipt_id",
    )
    lines = [str(payload.get("schema_version", "dpone.data_product_fleet"))]
    lines.extend(f"- {key}: {payload[key]}" for key in keys if payload.get(key) is not None)
    for key in ("blockers", "warnings"):
        values = payload.get(key, [])
        if isinstance(values, list) and values:
            lines.append(f"- {key}:")
            lines.extend(f"  - {item}" for item in values)
    return "\n".join(lines) + "\n"


def _render_table(payload: dict[str, Any]) -> str:
    products = payload.get("products") or [payload]
    lines = ["data product fleet", "product | owner | status | action", "--- | --- | --- | ---"]
    if isinstance(products, list):
        for item in products:
            if isinstance(item, dict):
                lines.append(
                    f"{item.get('id', payload.get('schema_version'))} | {item.get('owner', '')} | "
                    f"{item.get('status', payload.get('status'))} | {item.get('action', '')}"
                )
    return "\n".join(lines) + "\n"


def _facade() -> Any:
    return import_module("dpone.services.data_product_fleet").DataProductFleetFacade()


__all__ = ["fleet_group"]
