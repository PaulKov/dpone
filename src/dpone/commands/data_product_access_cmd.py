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


def access_group() -> Command:
    subcommands = [
        FuncCommand("classify", _classify_parser, _cmd_classify),
        entitlements_group(),
        _enforcement_group(),
        _drift_group(),
        FuncCommand("gate", _gate_parser, _cmd_gate),
        FuncCommand("report", _report_parser, _cmd_report),
    ]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("access", help="Classify and gate data product access governance")

    return CommandGroup(
        name="access",
        help="Classify and gate data product access governance",
        build_parser=build,
        subcommands=subcommands,
        subdest="data_product_access_cmd",
    )


def entitlements_group() -> Command:
    subcommands = [FuncCommand("plan", _entitlements_plan_parser, _cmd_entitlements_plan)]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("entitlements", help="Plan subject-column entitlements")

    return CommandGroup(
        name="entitlements",
        help="Plan subject-column entitlements",
        build_parser=build,
        subcommands=subcommands,
        subdest="data_product_access_entitlements_cmd",
    )


def privacy_group() -> Command:
    subcommands = [privacy_impact_group()]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("privacy", help="Assess data product privacy impact")

    return CommandGroup(
        name="privacy",
        help="Assess data product privacy impact",
        build_parser=build,
        subcommands=subcommands,
        subdest="data_product_privacy_cmd",
    )


def privacy_impact_group() -> Command:
    subcommands = [FuncCommand("assess", _privacy_assess_parser, _cmd_privacy_assess)]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("impact", help="Assess sensitive access privacy impact")

    return CommandGroup(
        name="impact",
        help="Assess sensitive access privacy impact",
        build_parser=build,
        subcommands=subcommands,
        subdest="data_product_privacy_impact_cmd",
    )


def _cmd_classify(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().classify(manifest_path=args.manifest, schema_contract_path=args.schema_contract)
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def _cmd_entitlements_plan(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().entitlements_plan(
        manifest_path=args.manifest,
        classification_path=args.classification,
        consumer_matrix_path=args.consumer_matrix,
    )
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def _cmd_privacy_assess(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().privacy_assess(
        manifest_path=args.manifest,
        entitlement_plan_path=args.entitlement_plan,
        authority_gate_path=args.authority_gate,
    )
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def _cmd_gate(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().gate(
        entitlement_plan_path=args.entitlement_plan,
        privacy_impact_path=args.privacy_impact,
        profile=args.profile,
    )
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def _cmd_report(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().report(gate_path=args.gate)
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def _classify_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("classify", help="Build access classification inventory")
    parser.add_argument("--manifest", required=True, help="Manifest path")
    parser.add_argument("--schema-contract", help="Optional schema contract version artifact")
    _add_output_args(parser)
    return parser


def _entitlements_plan_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("plan", help="Build entitlement plan from classification and consumer matrix")
    parser.add_argument("--manifest", required=True, help="Manifest path")
    parser.add_argument("--classification", required=True, help="Access classification artifact")
    parser.add_argument("--consumer-matrix", help="Optional schema contract consumer matrix artifact")
    _add_output_args(parser)
    return parser


def _privacy_assess_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("assess", help="Assess privacy impact from entitlement plan")
    parser.add_argument("--manifest", required=True, help="Manifest path")
    parser.add_argument("--entitlement-plan", required=True, help="Entitlement plan artifact")
    parser.add_argument("--authority-gate", help="Optional authority gate artifact")
    _add_output_args(parser)
    return parser


def _gate_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("gate", help="Gate access and privacy evidence")
    parser.add_argument("--entitlement-plan", required=True, help="Entitlement plan artifact")
    parser.add_argument("--privacy-impact", required=True, help="Privacy impact artifact")
    parser.add_argument(
        "--profile",
        choices=["advisory", "stage", "prod_strict", "regulated"],
        default="prod_strict",
        help="Access governance profile",
    )
    _add_output_args(parser)
    return parser


def _report_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("report", help="Render access governance report")
    parser.add_argument("--gate", required=True, help="Access gate artifact")
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
    lines = [str(payload.get("schema_version", "dpone.data_product_access"))]
    for key in (
        "status",
        "product_id",
        "access_classification_id",
        "entitlement_plan_id",
        "privacy_impact_id",
        "access_gate_id",
        "access_report_id",
    ):
        if payload.get(key) is not None:
            lines.append(f"- {key}: {payload.get(key)}")
    return "\n".join(lines) + "\n"


def _render_md(payload: dict[str, Any]) -> str:
    return (
        "# Data Product Access Governance\n\n```json\n" + json.dumps(payload, ensure_ascii=False, indent=2) + "\n```\n"
    )


def _render_table(payload: dict[str, Any]) -> str:
    rows = payload.get("decisions") or payload.get("columns") or [payload]
    lines = ["data product access", "name | class | status", "--- | --- | ---"]
    if isinstance(rows, list):
        for item in rows:
            if isinstance(item, dict):
                name = item.get("column") or item.get("name") or item.get("schema_version")
                lines.append(f"{name} | {item.get('class', '')} | {item.get('status', payload.get('status'))}")
    return "\n".join(lines) + "\n"


def _facade() -> Any:
    return import_module("dpone.services.data_product_access").DataProductAccessFacade()


def _enforcement_group() -> Command:
    return import_module("dpone.commands.data_product_access_enforcement_cmd").enforcement_group()


def _drift_group() -> Command:
    return import_module("dpone.commands.data_product_access_enforcement_cmd").drift_group()


__all__ = ["access_group", "privacy_group"]
