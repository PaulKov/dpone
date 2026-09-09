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


def identity_group() -> Command:
    subcommands = [FuncCommand("plan", register_plan_parser, cmd_schema_identity_plan)]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("identity", help="Schema identity, rename, and alias planning")

    return CommandGroup(
        name="identity",
        help="Schema identity, rename, and alias planning",
        build_parser=build,
        subcommands=subcommands,
        subdest="schema_identity_cmd",
    )


def cmd_schema_identity_plan(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    try:
        payload = _readiness_service().schema_identity_plan(
            manifest_path=args.manifest,
            source_path=args.source,
            actual_path=args.actual,
        )
    except ValueError as exc:
        payload = {
            "schema_version": "dpone.schema_identity_plan.v1",
            "command": "identity_plan",
            "status": "blocked",
            "blockers": [f"schema_identity.configuration_error:{exc}"],
            "warnings": [],
        }
        _emit(payload, args.format, args.output)
        return 2
    _emit(payload, args.format, args.output)
    return 2 if payload.get("blockers") else 0


def register_plan_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("plan", help="Plan schema identity aliases and rename decisions")
    parser.add_argument("--manifest", required=True, help="Manifest path with sink.options.schema_identity")
    parser.add_argument("--source", help="Optional source columns JSON")
    parser.add_argument("--actual", help="Optional actual physical target state JSON")
    parser.add_argument("--format", choices=["text", "json", "md"], default="text")
    parser.add_argument("--output", help="Optional output artifact path")
    return parser


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
        return (
            "# dpone schema identity plan\n\n```json\n" + json.dumps(payload, indent=2, ensure_ascii=False) + "\n```\n"
        )
    return _render_text(payload)


def _render_text(payload: dict[str, Any]) -> str:
    lines = ["dpone schema identity plan", f"- status: {payload.get('status', 'planned')}"]
    for key in ("blockers", "warnings"):
        values = payload.get(key, [])
        if isinstance(values, list) and values:
            lines.append(f"- {key}:")
            lines.extend(f"  - {item}" for item in values)
    decisions = payload.get("identity_decisions", [])
    if isinstance(decisions, list) and decisions:
        lines.append("- identity_decisions:")
        lines.extend(
            f"  - {item.get('action')} {item.get('observed_name')} -> {item.get('canonical_name')}"
            for item in decisions
            if isinstance(item, dict)
        )
    return "\n".join(lines) + "\n"


def _readiness_service() -> Any:
    module = import_module("dpone.services.readiness")
    return module.ReadinessService()


__all__ = ["identity_group"]
