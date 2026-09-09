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


def budget_group() -> Command:
    subcommands = [
        FuncCommand("plan", register_budget_plan_parser, cmd_budget_plan),
        FuncCommand("evaluate", register_budget_evaluate_parser, cmd_budget_evaluate),
        FuncCommand("gate", register_budget_gate_parser, cmd_budget_gate),
    ]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("budget", help="Plan, evaluate and gate data product error budgets")

    return CommandGroup(
        name="budget",
        help="Plan, evaluate and gate data product error budgets",
        build_parser=build,
        subcommands=subcommands,
        subdest="data_product_budget_cmd",
    )


def incident_lifecycle_group() -> Command:
    subcommands = [
        FuncCommand("open", register_lifecycle_open_parser, cmd_lifecycle_open),
        FuncCommand("ack", register_lifecycle_ack_parser, cmd_lifecycle_ack),
        FuncCommand("resolve", register_lifecycle_resolve_parser, cmd_lifecycle_resolve),
    ]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("lifecycle", help="Manage data product incident lifecycle artifacts")

    return CommandGroup(
        name="lifecycle",
        help="Manage data product incident lifecycle artifacts",
        build_parser=build,
        subcommands=subcommands,
        subdest="data_product_incident_lifecycle_cmd",
    )


def incident_route_group() -> Command:
    subcommands = [
        FuncCommand("render", register_route_render_parser, cmd_route_render),
        FuncCommand("dry-run", register_route_dry_run_parser, cmd_route_dry_run),
    ]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("route", help="Render incident routing payloads")

    return CommandGroup(
        name="route",
        help="Render incident routing payloads",
        build_parser=build,
        subcommands=subcommands,
        subdest="data_product_incident_route_cmd",
    )


def release_group() -> Command:
    subcommands = [closeout_group()]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("release", help="Gate data product release closeout")

    return CommandGroup(
        name="release",
        help="Gate data product release closeout",
        build_parser=build,
        subcommands=subcommands,
        subdest="data_product_release_cmd",
    )


def closeout_group() -> Command:
    subcommands = [FuncCommand("gate", register_closeout_gate_parser, cmd_closeout_gate)]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("closeout", help="Evaluate data product release closeout")

    return CommandGroup(
        name="closeout",
        help="Evaluate data product release closeout",
        build_parser=build,
        subcommands=subcommands,
        subdest="data_product_closeout_cmd",
    )


def cmd_budget_plan(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().budget_plan(manifest_path=args.manifest, slo_evaluation_path=args.slo_evaluation)
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_budget_evaluate(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().budget_evaluate(
        plan_path=args.plan,
        history_path=args.history,
        registry_path=args.registry,
        observed_at=args.observed_at,
    )
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_budget_gate(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().budget_gate(evaluation_path=args.evaluation, profile=args.profile)
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_lifecycle_open(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().incident_open(
        slo_evaluation_path=args.slo_evaluation,
        slo_gate_path=args.slo_gate,
        budget_gate_path=args.budget_gate,
        existing_incident_path=args.existing_incident,
    )
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") in {"open", "blocked"} else 0


def cmd_lifecycle_ack(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().incident_ack(incident_path=args.incident, actor=args.actor)
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_lifecycle_resolve(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().incident_resolve(incident_path=args.incident, evidence_path=args.evidence)
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_route_render(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().route_render(incident_path=args.incident, provider=args.provider)
    _emit(payload, args.format, args.output)
    return 2 if payload.get("blockers") else 0


def cmd_route_dry_run(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _fleet_facade().route_dry_run(payload_path=args.payload, provider=args.provider)
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_closeout_gate(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().closeout_gate(
        slo_gate_path=args.slo_gate,
        budget_gate_path=args.budget_gate,
        incident_path=args.incident,
        watch_certificate_path=args.watch_certificate,
        post_apply_certificate_path=args.post_apply_certificate,
        policy_gate_path=args.policy_gate,
        profile=args.profile,
    )
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def register_budget_plan_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("plan", help="Build a data product error-budget plan")
    parser.add_argument("--manifest", required=True, help="Manifest path")
    parser.add_argument("--slo-evaluation", help="Optional latest SLO evaluation artifact")
    _add_output_args(parser, formats=("text", "json", "md", "table"))
    return parser


def register_budget_evaluate_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("evaluate", help="Evaluate data product error budget")
    parser.add_argument("--plan", required=True, help="Error-budget plan artifact")
    parser.add_argument("--history", help="Optional SLO evaluation history JSON/YAML")
    parser.add_argument("--registry", help="Optional evidence registry JSON/SQLite")
    parser.add_argument("--observed-at", help="Deterministic evaluation timestamp")
    _add_output_args(parser, formats=("text", "json", "md", "table"))
    return parser


def register_budget_gate_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("gate", help="Gate data product error-budget evaluation")
    parser.add_argument("--evaluation", required=True, help="Error-budget evaluation artifact")
    _add_profile_arg(parser, "Error-budget gate profile")
    _add_output_args(parser, formats=("text", "json", "md", "table"))
    return parser


def register_lifecycle_open_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("open", help="Open or dedupe an incident lifecycle artifact")
    parser.add_argument("--slo-evaluation", required=True, help="SLO evaluation artifact")
    parser.add_argument("--slo-gate", required=True, help="SLO gate artifact")
    parser.add_argument("--budget-gate", required=True, help="Error-budget gate artifact")
    parser.add_argument("--existing-incident", help="Optional existing incident lifecycle artifact")
    _add_output_args(parser, formats=("text", "json", "md", "table"))
    return parser


def register_lifecycle_ack_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("ack", help="Acknowledge an incident lifecycle artifact")
    parser.add_argument("--incident", required=True, help="Incident lifecycle artifact")
    parser.add_argument("--actor", required=True, help="Acknowledging actor")
    _add_output_args(parser, formats=("text", "json", "md", "table"))
    return parser


def register_lifecycle_resolve_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("resolve", help="Resolve an incident lifecycle artifact")
    parser.add_argument("--incident", required=True, help="Incident lifecycle artifact")
    parser.add_argument("--evidence", required=True, help="Resolution evidence artifact")
    _add_output_args(parser, formats=("text", "json", "md", "table"))
    return parser


def register_route_render_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("render", help="Render provider-specific incident route payload")
    parser.add_argument("--incident", required=True, help="Incident lifecycle artifact")
    parser.add_argument("--provider", required=True, choices=["slack", "jira", "pagerduty"], help="Route provider")
    _add_output_args(parser, formats=("text", "json", "md", "table"))
    return parser


def register_route_dry_run_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("dry-run", help="Validate an incident route payload without network writes")
    parser.add_argument("--payload", required=True, help="Incident route payload artifact")
    parser.add_argument(
        "--provider",
        required=True,
        choices=["slack", "jira", "pagerduty", "webhook"],
        help="Route provider",
    )
    _add_output_args(parser, formats=("text", "json", "md", "table"))
    return parser


def register_closeout_gate_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("gate", help="Gate data product release closeout")
    parser.add_argument("--slo-gate", required=True, help="SLO gate artifact")
    parser.add_argument("--budget-gate", required=True, help="Error-budget gate artifact")
    parser.add_argument("--incident", help="Optional incident lifecycle artifact")
    parser.add_argument("--watch-certificate", help="Optional release watch certificate")
    parser.add_argument("--post-apply-certificate", help="Optional post-apply certificate")
    parser.add_argument("--policy-gate", help="Optional data product policy gate artifact")
    _add_profile_arg(parser, "Release closeout profile")
    _add_output_args(parser, formats=("text", "json", "md", "table"))
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
        return "# Data Product Reliability\n\n```json\n" + json.dumps(payload, ensure_ascii=False, indent=2) + "\n```\n"
    if output_format == "table":
        return _render_table(payload)
    return _render_text(payload)


def _render_text(payload: dict[str, Any]) -> str:
    keys = (
        "status",
        "product_id",
        "error_budget_plan_id",
        "error_budget_evaluation_id",
        "error_budget_gate_id",
        "incident_id",
        "route_payload_id",
        "release_closeout_gate_id",
    )
    lines = [str(payload.get("schema_version", "dpone.data_product_reliability"))]
    lines.extend(f"- {key}: {payload[key]}" for key in keys if payload.get(key) is not None)
    for key in ("blockers", "warnings"):
        values = payload.get(key, [])
        if isinstance(values, list) and values:
            lines.append(f"- {key}:")
            lines.extend(f"  - {item}" for item in values)
    return "\n".join(lines) + "\n"


def _render_table(payload: dict[str, Any]) -> str:
    rows = payload.get("windows") or payload.get("events") or [payload]
    lines = ["data product reliability", "name | status | details", "--- | --- | ---"]
    if isinstance(rows, list):
        for item in rows:
            if isinstance(item, dict):
                lines.append(
                    f"{item.get('name', item.get('kind', payload.get('schema_version')))} | "
                    f"{item.get('status', payload.get('status'))} | "
                    f"{', '.join(str(value) for value in item.get('blockers', []))}"
                )
    return "\n".join(lines) + "\n"


def _facade() -> Any:
    return import_module("dpone.services.data_product_reliability").DataProductReliabilityFacade()


def _fleet_facade() -> Any:
    return import_module("dpone.services.data_product_fleet").DataProductFleetFacade()


__all__ = [
    "budget_group",
    "incident_lifecycle_group",
    "incident_route_group",
    "release_group",
]
