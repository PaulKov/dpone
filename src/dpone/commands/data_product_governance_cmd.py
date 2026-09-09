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


def governance_group() -> Command:
    subcommands = [
        governance_export_group(),
        FuncCommand("publish", _publish_parser, _cmd_publish),
        FuncCommand("verify", _verify_parser, _cmd_verify),
    ]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("governance", help="Export data product governance evidence")

    return CommandGroup(
        name="governance",
        help="Export data product governance evidence",
        build_parser=build,
        subcommands=subcommands,
        subdest="data_product_governance_cmd",
    )


def governance_export_group() -> Command:
    subcommands = [
        FuncCommand("plan", _plan_parser, _cmd_plan),
        FuncCommand("render", _render_parser, _cmd_render),
    ]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("export", help="Plan and render governance export payloads")

    return CommandGroup(
        name="export",
        help="Plan and render governance export payloads",
        build_parser=build,
        subcommands=subcommands,
        subdest="data_product_governance_export_cmd",
    )


def _cmd_plan(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().plan(
        manifest_path=args.manifest,
        registry_path=args.registry,
        evidence_dir=args.evidence_dir,
        targets=_targets(args.targets),
    )
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def _cmd_render(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().render(plan_path=args.plan, target=args.target)
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def _cmd_publish(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().publish(
        payload_path=args.payload,
        provider=args.provider,
        connection_path=args.connection,
        execute=args.execute,
    )
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def _cmd_verify(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().verify(receipt_path=args.receipt)
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def _plan_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("plan", help="Build a governance export plan")
    parser.add_argument("--manifest", required=True, help="Manifest path")
    parser.add_argument("--registry", help="Optional evidence registry JSON/SQLite")
    parser.add_argument("--evidence-dir", help="Directory with local evidence artifacts")
    parser.add_argument("--targets", help="Comma-separated providers")
    _add_output_args(parser)
    return parser


def _render_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("render", help="Render one governance export payload")
    parser.add_argument("--plan", required=True, help="Governance export plan artifact")
    parser.add_argument("--target", required=True, help="Provider target")
    _add_output_args(parser)
    return parser


def _publish_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("publish", help="Publish or dry-run a governance payload")
    parser.add_argument("--payload", required=True, help="Governance payload artifact")
    parser.add_argument("--provider", required=True, help="Provider target")
    parser.add_argument("--connection", help="Optional provider connection JSON/YAML")
    parser.add_argument("--execute", action="store_true", help="Validate an explicit publish execution")
    _add_output_args(parser)
    return parser


def _verify_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("verify", help="Verify a governance publish receipt")
    parser.add_argument("--receipt", required=True, help="Publish receipt artifact")
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
        return str(payload.get("markdown") or _render_md(payload))
    if output_format == "table":
        return _render_table(payload)
    return _render_text(payload)


def _render_text(payload: dict[str, Any]) -> str:
    lines = [str(payload.get("schema_version", "dpone.data_product_governance"))]
    for key in ("status", "provider", "product_id", "governance_export_plan_id", "payload_id", "publish_receipt_id"):
        if payload.get(key) is not None:
            lines.append(f"- {key}: {payload.get(key)}")
    return "\n".join(lines) + "\n"


def _render_md(payload: dict[str, Any]) -> str:
    return "# Data Product Governance\n\n```json\n" + json.dumps(payload, ensure_ascii=False, indent=2) + "\n```\n"


def _render_table(payload: dict[str, Any]) -> str:
    rows = payload.get("export_items") or payload.get("checks") or [payload]
    lines = ["data product governance", "name | status | details", "--- | --- | ---"]
    if isinstance(rows, list):
        for item in rows:
            if isinstance(item, dict):
                name = item.get("provider") or item.get("name") or payload.get("schema_version")
                lines.append(f"{name} | {item.get('status', payload.get('status'))} | {item.get('mode', '')}")
    return "\n".join(lines) + "\n"


def _targets(raw: str | None) -> tuple[str, ...]:
    return tuple(item.strip() for item in str(raw or "").split(",") if item.strip())


def _facade() -> Any:
    return import_module("dpone.services.data_product_governance").DataProductGovernanceFacade()


__all__ = ["governance_group"]
