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


def policy_group() -> Command:
    subcommands = [
        FuncCommand("evaluate", register_evaluate_parser, cmd_evaluate),
        waiver_group(),
        FuncCommand("gate", register_gate_parser, cmd_gate),
        FuncCommand("report", register_report_parser, cmd_report),
    ]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("policy", help="Evaluate data product policy and waiver evidence")

    return CommandGroup(
        name="policy",
        help="Evaluate data product policy and waiver evidence",
        build_parser=build,
        subcommands=subcommands,
        subdest="data_product_policy_cmd",
    )


def waiver_group() -> Command:
    subcommands = [
        FuncCommand("request", register_waiver_request_parser, cmd_waiver_request),
        FuncCommand("approve", register_waiver_approve_parser, cmd_waiver_approve),
    ]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("waiver", help="Request and approve policy waivers")

    return CommandGroup(
        name="waiver",
        help="Request and approve policy waivers",
        build_parser=build,
        subcommands=subcommands,
        subdest="data_product_policy_waiver_cmd",
    )


def cmd_evaluate(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().evaluate(manifest_path=args.manifest, bundle_path=args.bundle, evidence_dir=args.evidence_dir)
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_waiver_request(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().waiver_request(
        evaluation_path=args.evaluation,
        rule_id=args.rule_id,
        reason=args.reason,
        expires_at=args.expires_at,
        requested_by=args.requested_by,
    )
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_waiver_approve(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().waiver_approve(
        request_path=args.request,
        actor=args.actor,
        approval_path=args.approval,
        authority_check_path=args.authority_check,
        approval_quorum_path=args.approval_quorum,
        approved_at=args.approved_at,
    )
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_gate(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().gate(
        evaluation_path=args.evaluation,
        waiver_paths=tuple(args.waiver or ()),
        authority_gate_path=args.authority_gate,
        profile=args.profile,
        observed_at=args.observed_at,
    )
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def cmd_report(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().report(gate_path=args.gate)
    _emit(payload, args.format, args.output)
    return 0


def register_evaluate_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("evaluate", help="Evaluate data product policy evidence")
    parser.add_argument("--manifest", required=True, help="Manifest path")
    parser.add_argument("--bundle", help="Optional schema migration bundle JSON/YAML")
    parser.add_argument("--evidence-dir", help="Optional directory with local evidence artifacts")
    _add_output_args(parser)
    return parser


def register_waiver_request_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("request", help="Request a bounded policy waiver")
    parser.add_argument("--evaluation", required=True, help="Policy evaluation artifact")
    parser.add_argument("--rule-id", required=True, help="Failed or warning rule id to waive")
    parser.add_argument("--reason", required=True, help="Waiver reason")
    parser.add_argument("--expires-at", required=True, help="Waiver expiry timestamp")
    parser.add_argument("--requested-by", help="Optional requester identity")
    _add_output_args(parser)
    return parser


def register_waiver_approve_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("approve", help="Approve a policy waiver request")
    parser.add_argument("--request", required=True, help="Waiver request artifact")
    parser.add_argument("--actor", required=True, help="Approving actor")
    parser.add_argument("--approval", required=True, help="Approval evidence JSON/YAML")
    parser.add_argument("--authority-check", help="Optional authority check artifact")
    parser.add_argument("--approval-quorum", help="Optional approval quorum artifact")
    parser.add_argument("--approved-at", help="Approval timestamp")
    _add_output_args(parser)
    return parser


def register_gate_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("gate", help="Gate data product policy evaluation")
    parser.add_argument("--evaluation", required=True, help="Policy evaluation artifact")
    parser.add_argument("--waiver", action="append", help="Optional approved waiver artifact")
    parser.add_argument("--authority-gate", help="Optional authority gate artifact")
    parser.add_argument(
        "--profile",
        choices=["advisory", "stage", "prod_strict", "regulated"],
        default="prod_strict",
        help="Policy gate profile",
    )
    parser.add_argument("--observed-at", help="Timestamp used for waiver expiry checks")
    _add_output_args(parser)
    return parser


def register_report_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("report", help="Render a policy gate report")
    parser.add_argument("--gate", required=True, help="Policy gate artifact")
    _add_output_args(parser, default="md")
    return parser


def _add_output_args(
    parser: argparse.ArgumentParser,
    *,
    formats: tuple[str, ...] = ("text", "json", "md", "table"),
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
    lines = [str(payload.get("schema_version", "dpone.data_product_policy"))]
    for key in ("status", "product_id", "policy_evaluation_id", "policy_gate_id", "waiver_id"):
        if payload.get(key) is not None:
            lines.append(f"- {key}: {payload.get(key)}")
    return "\n".join(lines) + "\n"


def _render_md(payload: dict[str, Any]) -> str:
    return "# Data Product Policy\n\n```json\n" + json.dumps(payload, ensure_ascii=False, indent=2) + "\n```\n"


def _render_table(payload: dict[str, Any]) -> str:
    rows = payload.get("rules") or [payload]
    lines = ["data product policy", "rule | status | severity", "--- | --- | ---"]
    if isinstance(rows, list):
        for item in rows:
            if isinstance(item, dict):
                lines.append(
                    f"{item.get('id', payload.get('schema_version'))} | "
                    f"{item.get('status', payload.get('status'))} | {item.get('severity', '')}"
                )
    return "\n".join(lines) + "\n"


def _facade() -> Any:
    return import_module("dpone.services.data_product_policy").DataProductPolicyFacade()


__all__ = ["policy_group"]
