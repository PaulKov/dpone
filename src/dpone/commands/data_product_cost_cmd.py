from __future__ import annotations

import argparse
import logging
from importlib import import_module
from typing import Any

from dpone.cli_render.data_product_artifacts import emit_data_product_artifact
from dpone.commands.base import Command
from dpone.commands.func_command import CommandGroup, FuncCommand


def cost_group() -> Command:
    return _group(
        "cost",
        "Plan, evaluate and gate data product cost guardrails",
        [
            FuncCommand("plan", _plan_parser, _cmd_plan),
            FuncCommand("evaluate", _evaluate_parser, _cmd_evaluate),
            FuncCommand("gate", _gate_parser, _cmd_gate),
            FuncCommand("forecast", _forecast_parser, _cmd_forecast),
            FuncCommand("report", _report_parser, _cmd_report),
        ],
        "data_product_cost_cmd",
    )


def _cmd_plan(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().plan(manifest_path=args.manifest, bundle_path=args.bundle)
    return _emit_code(payload, args.format, args.output)


def _cmd_evaluate(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().evaluate(
        plan_path=args.plan,
        registry_path=args.registry,
        runtime_artifact_path=args.runtime_artifact,
        target_connection_path=args.target_connection,
    )
    return _emit_code(payload, args.format, args.output)


def _cmd_gate(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().gate(evaluation_path=args.evaluation, profile=args.profile)
    return _emit_code(payload, args.format, args.output)


def _cmd_forecast(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().forecast(evaluation_path=args.evaluation, history_path=args.history)
    return _emit_code(payload, args.format, args.output)


def _cmd_report(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().report(gate_path=args.gate, forecast_path=args.forecast)
    return _emit_code(payload, args.format, args.output)


def _plan_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("plan", help="Build a data product cost guardrail plan")
    parser.add_argument("--manifest", required=True, help="Manifest path")
    parser.add_argument("--bundle", help="Optional schema migration bundle JSON/YAML")
    _add_output_args(parser)
    return parser


def _evaluate_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("evaluate", help="Evaluate cost and capacity evidence")
    parser.add_argument("--plan", required=True, help="Cost plan artifact")
    parser.add_argument("--registry", help="Optional evidence registry JSON/SQLite")
    parser.add_argument("--runtime-artifact", help="Optional runtime/run evidence JSON/YAML")
    parser.add_argument("--target-connection", help="Optional read-only target connection JSON/YAML")
    _add_output_args(parser)
    return parser


def _gate_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("gate", help="Gate data product cost evaluation")
    parser.add_argument("--evaluation", required=True, help="Cost evaluation artifact")
    _add_profile(parser)
    _add_output_args(parser)
    return parser


def _forecast_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("forecast", help="Render plan-only cost and capacity forecast")
    parser.add_argument("--evaluation", required=True, help="Cost evaluation artifact")
    parser.add_argument("--history", help="Optional historical cost artifacts")
    _add_output_args(parser, default="md")
    return parser


def _report_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("report", help="Render data product cost governance report")
    parser.add_argument("--gate", required=True, help="Cost gate artifact")
    parser.add_argument("--forecast", help="Optional cost forecast artifact")
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
    title = "Data Product Cost Forecast" if payload.get("cost_forecast_id") else "Data Product Cost Governance"
    return emit_data_product_artifact(
        payload,
        output_format,
        output_path,
        default_schema="dpone.data_product_cost",
        text_keys=("status", "product_id", "cost_plan_id", "cost_evaluation_id", "cost_gate_id", "cost_forecast_id"),
        markdown_title=title,
        table_title="data product cost",
    )


def _facade() -> Any:
    return import_module("dpone.services.data_product_cost").DataProductCostFacade()


__all__ = ["cost_group"]
