"""CLI commands for controlled remediation execution."""

from __future__ import annotations

import argparse
import logging
from importlib import import_module
from typing import Any

from dpone.cli_render.data_product_artifacts import emit_data_product_artifact
from dpone.commands.base import Command
from dpone.commands.func_command import CommandGroup, FuncCommand


def remediation_execution_group() -> Command:
    return _group(
        "execution",
        "Plan, run and certify controlled remediation execution",
        [
            FuncCommand("plan", _plan_parser, _cmd_plan),
            FuncCommand("run", _run_parser, _cmd_run),
            FuncCommand("certify", _certify_parser, _cmd_certify),
            FuncCommand("report", _report_parser, _cmd_report),
        ],
        "data_product_remediation_execution_cmd",
    )


def _cmd_plan(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().plan(
        manifest_path=args.manifest,
        remediation_plan_path=args.remediation_plan,
        remediation_gate_path=args.remediation_gate,
        authority_gate_path=args.authority_gate,
        parameters_path=args.parameters,
    )
    return _emit_code(payload, args.format, args.output)


def _cmd_run(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().run(
        plan_path=args.plan,
        idempotency_key=args.idempotency_key,
        lock_path=args.lock,
        execute=args.execute,
    )
    return _emit_code(payload, args.format, args.output)


def _cmd_certify(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().certify(run_path=args.run, evidence_dir=args.evidence_dir, profile=args.profile)
    return _emit_code(payload, args.format, args.output)


def _cmd_report(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().report(certificate_path=args.certificate)
    return _emit_code(payload, args.format, args.output)


def _plan_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("plan", help="Build a controlled remediation execution plan")
    parser.add_argument("--manifest", required=True, help="Manifest path")
    parser.add_argument("--remediation-plan", required=True, help="Remediation plan artifact")
    parser.add_argument("--remediation-gate", help="Optional remediation gate artifact")
    parser.add_argument("--authority-gate", help="Optional authority gate artifact")
    parser.add_argument("--parameters", help="Optional command placeholder parameters JSON/YAML")
    _add_output_args(parser)
    return parser


def _run_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("run", help="Dry-run or execute a remediation execution plan")
    parser.add_argument("--plan", required=True, help="Remediation execution plan artifact")
    parser.add_argument("--idempotency-key", help="Required for --execute when configured")
    parser.add_argument("--lock", help="Optional acquired lock artifact")
    parser.add_argument("--execute", action="store_true", help="Actually execute prevalidated commands")
    _add_output_args(parser)
    return parser


def _certify_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("certify", help="Certify remediation execution evidence")
    parser.add_argument("--run", required=True, help="Remediation execution run artifact")
    parser.add_argument("--evidence-dir", help="Directory with fresh data product evidence")
    _add_profile(parser)
    _add_output_args(parser)
    return parser


def _report_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("report", help="Render a remediation execution report")
    parser.add_argument("--certificate", required=True, help="Remediation execution certificate artifact")
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
        default_schema="dpone.data_product_remediation_execution",
        text_keys=(
            "status",
            "product_id",
            "remediation_execution_plan_id",
            "remediation_execution_run_id",
            "remediation_execution_certificate_id",
            "remediation_execution_report_id",
        ),
        markdown_title="Data Product Remediation Execution",
        table_title="data product remediation execution",
    )


def _facade() -> Any:
    return import_module("dpone.services.data_product_remediation_execution").DataProductRemediationExecutionFacade()


__all__ = ["remediation_execution_group"]
