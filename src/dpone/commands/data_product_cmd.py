from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.commands.base import Command


import argparse
import json
import logging
from importlib import import_module
from pathlib import Path
from typing import Any

from dpone.commands.data_product_access_cmd import access_group, privacy_group
from dpone.commands.data_product_assertions_cmd import assertions_group
from dpone.commands.data_product_audit_cmd import audit_group
from dpone.commands.data_product_authority_cmd import authority_group
from dpone.commands.data_product_compliance_cmd import compliance_group
from dpone.commands.data_product_cost_cmd import cost_group
from dpone.commands.data_product_fleet_cmd import fleet_group
from dpone.commands.data_product_governance_cmd import governance_group
from dpone.commands.data_product_policy_cmd import policy_group
from dpone.commands.data_product_reliability_cmd import (
    budget_group,
    incident_lifecycle_group,
    incident_route_group,
    release_group,
)
from dpone.commands.data_product_remediation_cmd import remediation_group
from dpone.commands.data_product_rollout_cmd import rollout_group
from dpone.commands.data_product_trust_cmd import trust_group
from dpone.commands.func_command import CommandGroup, FuncCommand
from dpone.commands.output_json import dumps_json
from dpone.commands.output_text import write_text


def data_group() -> Command:
    subcommands = [product_group()]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("data", help="Data product operations")

    return CommandGroup(
        name="data",
        help="Data product operations",
        build_parser=build,
        subcommands=subcommands,
        subdest="data_cmd",
    )


def product_group() -> Command:
    subcommands = [
        slo_group(),
        audit_group(),
        assertions_group(),
        policy_group(),
        authority_group(),
        compliance_group(),
        governance_group(),
        access_group(),
        privacy_group(),
        incident_group(),
        release_group(),
        fleet_group(),
        cost_group(),
        rollout_group(),
        trust_group(),
        remediation_group(),
    ]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("product", help="Data product SLO and incident evidence")

    return CommandGroup(
        name="product",
        help="Data product SLO and incident evidence",
        build_parser=build,
        subcommands=subcommands,
        subdest="data_product_cmd",
    )


def slo_group() -> Command:
    subcommands = [
        FuncCommand("plan", register_slo_plan_parser, cmd_slo_plan),
        FuncCommand("evaluate", register_slo_evaluate_parser, cmd_slo_evaluate),
        FuncCommand("gate", register_slo_gate_parser, cmd_slo_gate),
        budget_group(),
    ]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("slo", help="Plan, evaluate and gate data product SLOs")

    return CommandGroup(
        name="slo",
        help="Plan, evaluate and gate data product SLOs",
        build_parser=build,
        subcommands=subcommands,
        subdest="data_product_slo_cmd",
    )


def incident_group() -> Command:
    subcommands = [
        FuncCommand("report", register_incident_report_parser, cmd_incident_report),
        incident_lifecycle_group(),
        incident_route_group(),
    ]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("incident", help="Render data product incident reports from SLO evidence")

    return CommandGroup(
        name="incident",
        help="Render data product incident reports from SLO evidence",
        build_parser=build,
        subcommands=subcommands,
        subdest="data_product_incident_cmd",
    )


def cmd_slo_plan(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().plan(
        manifest_path=args.manifest,
        contract_gate_path=args.contract_gate,
        consumer_gate_path=args.consumer_gate,
        consumer_certification_path=args.consumer_certification,
        watch_certificate_path=args.watch_certificate,
        post_apply_certificate_path=args.post_apply_certificate,
        assertion_gate_path=args.assertion_gate,
    )
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_slo_evaluate(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().evaluate(
        plan_path=args.plan,
        registry_path=args.registry,
        runtime_artifact_path=args.runtime_artifact,
        target_connection_path=args.target_connection,
    )
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_slo_gate(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().gate(evaluation_path=args.evaluation, profile=args.profile)
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_incident_report(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().incident_report(evaluation_path=args.evaluation, slo_gate_path=args.slo_gate)
    _emit(payload, args.format, args.output)
    return 0


def register_slo_plan_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("plan", help="Build a data product SLO plan")
    parser.add_argument("--manifest", required=True, help="Manifest path")
    parser.add_argument("--contract-gate", help="Optional schema contract gate receipt")
    parser.add_argument("--consumer-gate", help="Optional schema contract consumer gate receipt")
    parser.add_argument("--consumer-certification", help="Optional consumer certification receipt")
    parser.add_argument("--assertion-gate", help="Optional data product assertion gate receipt")
    parser.add_argument("--watch-certificate", help="Optional release watch certificate")
    parser.add_argument("--post-apply-certificate", help="Optional post-apply certificate")
    _add_output_args(parser, formats=("text", "json", "md", "table"))
    return parser


def register_slo_evaluate_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("evaluate", help="Evaluate data product SLO evidence")
    parser.add_argument("--plan", required=True, help="SLO plan artifact")
    parser.add_argument("--registry", help="Optional evidence registry JSON/SQLite")
    parser.add_argument("--runtime-artifact", help="Optional runtime/run evidence JSON/YAML")
    parser.add_argument("--target-connection", help="Optional read-only target connection JSON/YAML")
    _add_output_args(parser, formats=("text", "json", "md", "table"))
    return parser


def register_slo_gate_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("gate", help="Gate data product SLO evaluation")
    parser.add_argument("--evaluation", required=True, help="SLO evaluation artifact")
    parser.add_argument(
        "--profile",
        choices=["advisory", "stage", "prod_strict", "regulated"],
        default="prod_strict",
        help="SLO gate profile",
    )
    _add_output_args(parser, formats=("text", "json", "md", "table"))
    return parser


def register_incident_report_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("report", help="Render an incident report from SLO evidence")
    parser.add_argument("--evaluation", required=True, help="SLO evaluation artifact")
    parser.add_argument("--slo-gate", required=True, help="SLO gate artifact")
    _add_output_args(parser, formats=("text", "json", "md", "table"), default="md")
    return parser


def _add_output_args(
    parser: argparse.ArgumentParser,
    *,
    formats: tuple[str, ...] = ("text", "json", "md"),
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
        return str(payload.get("markdown") or _render_md(payload))
    if output_format == "table":
        return _render_table(payload)
    return _render_text(payload)


def _render_text(payload: dict[str, Any]) -> str:
    lines = [str(payload.get("schema_version", "dpone.data_product"))]
    for key in ("status", "product_id", "slo_plan_id", "slo_evaluation_id", "slo_gate_id", "severity"):
        if payload.get(key) is not None:
            lines.append(f"- {key}: {payload.get(key)}")
    for key in ("blockers", "warnings"):
        values = payload.get(key, [])
        if isinstance(values, list) and values:
            lines.append(f"- {key}:")
            lines.extend(f"  - {item}" for item in values)
    return "\n".join(lines) + "\n"


def _render_md(payload: dict[str, Any]) -> str:
    return "# Data Product SLO\n\n```json\n" + json.dumps(payload, ensure_ascii=False, indent=2) + "\n```\n"


def _render_table(payload: dict[str, Any]) -> str:
    rows = payload.get("checks") or [payload]
    lines = ["data product slo", "name | status | details", "--- | --- | ---"]
    if isinstance(rows, list):
        for item in rows:
            if isinstance(item, dict):
                lines.append(
                    f"{item.get('name', payload.get('schema_version'))} | {item.get('status', payload.get('status'))} | "
                    f"{', '.join(str(value) for value in item.get('details', []))}"
                )
    return "\n".join(lines) + "\n"


def _facade() -> Any:
    return import_module("dpone.services.data_product_slo").DataProductSloFacade()


__all__ = ["data_group"]
