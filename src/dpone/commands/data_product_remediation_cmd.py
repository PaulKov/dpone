"""CLI commands for data product remediation runbooks."""

from __future__ import annotations

import argparse
import logging
from importlib import import_module
from typing import Any

from dpone.cli_render.data_product_artifacts import emit_data_product_artifact
from dpone.commands.base import Command
from dpone.commands.data_product_remediation_execution_cmd import remediation_execution_group
from dpone.commands.func_command import CommandGroup, FuncCommand


def remediation_group() -> Command:
    return _group(
        "remediation",
        "Plan, gate and close out data product remediation runbooks",
        [
            FuncCommand("plan", _plan_parser, _cmd_plan),
            _runbook_group(),
            FuncCommand("gate", _gate_parser, _cmd_gate),
            FuncCommand("closeout", _closeout_parser, _cmd_closeout),
            FuncCommand("report", _report_parser, _cmd_report),
            remediation_execution_group(),
        ],
        "data_product_remediation_cmd",
    )


def _cmd_plan(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().plan(
        manifest_path=args.manifest,
        trust_snapshot_path=args.trust_snapshot,
        trust_gate_path=args.trust_gate,
        bundle_path=args.bundle,
        evidence_dir=args.evidence_dir,
        registry_path=args.registry,
    )
    return _emit_code(payload, args.format, args.output)


def _cmd_runbook(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    return _emit_code(_facade().runbook(plan_path=args.plan), args.format, args.output)


def _cmd_gate(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().gate(plan_path=args.plan, profile=args.profile)
    return _emit_code(payload, args.format, args.output)


def _cmd_closeout(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().closeout(plan_path=args.plan, evidence_dir=args.evidence_dir)
    return _emit_code(payload, args.format, args.output)


def _cmd_report(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().report(gate_path=args.gate, closeout_path=args.closeout)
    return _emit_code(payload, args.format, args.output)


def _runbook_group() -> Command:
    return _group(
        "runbook",
        "Render data product remediation runbooks",
        [FuncCommand("render", _runbook_parser, _cmd_runbook)],
        "data_product_remediation_runbook_cmd",
    )


def _plan_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("plan", help="Build a data product remediation plan")
    parser.add_argument("--manifest", required=True, help="Manifest path")
    parser.add_argument("--trust-snapshot", help="Optional Trust Center snapshot artifact")
    parser.add_argument("--trust-gate", help="Optional Trust Center gate artifact")
    parser.add_argument("--bundle", help="Optional schema migration bundle artifact")
    parser.add_argument("--registry", help="Optional evidence registry JSON/SQLite")
    parser.add_argument("--evidence-dir", help="Optional directory with data product artifacts")
    _add_output_args(parser)
    return parser


def _runbook_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("render", help="Render an operator remediation runbook")
    parser.add_argument("--plan", required=True, help="Remediation plan artifact")
    _add_output_args(parser, default="md")
    return parser


def _gate_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("gate", help="Gate a remediation plan for actionability")
    parser.add_argument("--plan", required=True, help="Remediation plan artifact")
    _add_profile(parser)
    _add_output_args(parser)
    return parser


def _closeout_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("closeout", help="Verify remediation closeout evidence")
    parser.add_argument("--plan", required=True, help="Remediation plan artifact")
    parser.add_argument("--evidence-dir", help="Directory with fresh data product evidence")
    _add_output_args(parser)
    return parser


def _report_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("report", help="Render a remediation report")
    parser.add_argument("--gate", required=True, help="Remediation gate artifact")
    parser.add_argument("--closeout", help="Optional remediation closeout artifact")
    _add_output_args(parser, default="md")
    return parser


def _group(name: str, help_text: str, subcommands: list[Command], subdest: str) -> Command:
    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser(name, help=help_text)

    return CommandGroup(name=name, help=help_text, build_parser=build, subcommands=subcommands, subdest=subdest)


def _add_profile(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--profile", choices=["advisory", "stage", "prod_strict", "regulated"], default="prod_strict")


def _add_output_args(parser: argparse.ArgumentParser, *, default: str = "text") -> None:
    parser.add_argument("--format", choices=["text", "json", "md", "table"], default=default)
    parser.add_argument("--output", help="Optional output artifact path")


def _emit_code(payload: dict[str, Any], output_format: str, output_path: str | None) -> int:
    return emit_data_product_artifact(
        payload,
        output_format,
        output_path,
        default_schema="dpone.data_product_remediation",
        text_keys=(
            "status",
            "product_id",
            "remediation_plan_id",
            "remediation_runbook_id",
            "remediation_gate_id",
            "remediation_closeout_id",
            "remediation_report_id",
        ),
        markdown_title="Data Product Remediation",
        table_title="data product remediation",
    )


def _facade() -> Any:
    return import_module("dpone.services.data_product_remediation").DataProductRemediationFacade()


__all__ = ["remediation_group"]
