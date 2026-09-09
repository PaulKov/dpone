from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Any

from dpone.commands.base import Command
from dpone.commands.func_command import CommandGroup, FuncCommand
from dpone.commands.output_text import write_text
from dpone.commands.schema_impact_render import render_schema_impact
from dpone.readiness.schema_impact import SchemaImpactFacade


def impact_group() -> Command:
    subcommands = [
        FuncCommand("plan", register_plan_parser, cmd_schema_impact_plan),
        FuncCommand("gate", register_gate_parser, cmd_schema_impact_gate),
    ]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("impact", help="Schema impact and dependency analysis")

    return CommandGroup(
        name="impact",
        help="Schema impact and dependency analysis",
        build_parser=build,
        subcommands=subcommands,
        subdest="schema_impact_cmd",
    )


def cmd_schema_impact_plan(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = SchemaImpactFacade().plan_from_paths(pack_path=args.pack, manifest_path=args.manifest)
    _emit(payload, args.format, args.output)
    return 2 if payload.get("blockers") else 0


def cmd_schema_impact_gate(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = SchemaImpactFacade().gate_from_paths(
        pack_path=args.pack,
        impact_path=args.impact,
        approval_path=args.approval,
    )
    _emit(payload, args.format, args.output)
    return 2 if payload.get("blockers") else 0


def register_plan_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("plan", help="Build a schema impact dependency plan")
    parser.add_argument("--pack", required=True, help="Migration pack JSON path")
    parser.add_argument("--manifest", help="Manifest path with sink.options.schema_impact")
    _add_output_args(parser)
    return parser


def register_gate_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("gate", help="Evaluate schema impact approvals before apply")
    parser.add_argument("--pack", required=True, help="Migration pack JSON path")
    parser.add_argument("--impact", required=True, help="Schema impact plan JSON path")
    parser.add_argument("--approval", help="Approval artifact YAML/JSON path")
    _add_output_args(parser)
    return parser


def _add_output_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--format", choices=["text", "json", "md", "table"], default="text")
    parser.add_argument("--output", help="Optional output artifact path")


def _emit(payload: dict[str, Any], output_format: str, output_path: str | None) -> None:
    rendered = render_schema_impact(payload, output_format)
    if output_path:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(rendered, encoding="utf-8")
    write_text(rendered)


__all__ = ["impact_group"]
