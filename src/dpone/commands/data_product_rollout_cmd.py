"""CLI commands for data product progressive delivery."""

from __future__ import annotations

import argparse
import logging
from importlib import import_module
from typing import Any

from dpone.cli_render.data_product_artifacts import emit_data_product_artifact
from dpone.commands.base import Command
from dpone.commands.func_command import CommandGroup, FuncCommand


def rollout_group() -> Command:
    return _group(
        "rollout",
        "Plan, validate and promote data product rollout rings",
        [
            FuncCommand("plan", _plan_parser, _cmd_plan),
            FuncCommand("shadow-validate", _shadow_parser, _cmd_shadow),
            _ring_group(),
            FuncCommand("promote", _promote_parser, _cmd_promote),
            FuncCommand("report", _report_parser, _cmd_report),
        ],
        "data_product_rollout_cmd",
    )


def _cmd_plan(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().plan(manifest_path=args.manifest, bundle_path=args.bundle, evidence_dir=args.evidence_dir)
    return _emit_code(payload, args.format, args.output)


def _cmd_shadow(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().shadow_validate(
        plan_path=args.plan,
        runtime_artifact_path=args.runtime_artifact,
        baseline_runtime_artifact_path=args.baseline_runtime_artifact,
    )
    return _emit_code(payload, args.format, args.output)


def _cmd_ring_gate(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().ring_gate(
        plan_path=args.plan,
        ring=args.ring,
        evidence_dir=args.evidence_dir,
        shadow_validation_path=args.shadow_validation,
        promotion_paths=tuple(args.promotion or ()),
    )
    return _emit_code(payload, args.format, args.output)


def _cmd_promote(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().promote(
        ring_gate_path=args.ring_gate,
        target_ring=args.target_ring,
        authority_gate_path=args.authority_gate,
    )
    return _emit_code(payload, args.format, args.output)


def _cmd_report(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().report(promotion_path=args.promotion)
    return _emit_code(payload, args.format, args.output)


def _plan_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("plan", help="Build a data product rollout plan")
    parser.add_argument("--manifest", required=True, help="Manifest path")
    parser.add_argument("--bundle", help="Optional schema migration bundle JSON/YAML")
    parser.add_argument("--evidence-dir", help="Optional directory with data product gate artifacts")
    _add_output_args(parser)
    return parser


def _shadow_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("shadow-validate", help="Compare baseline and candidate runtime evidence")
    parser.add_argument("--plan", required=True, help="Rollout plan artifact")
    parser.add_argument("--runtime-artifact", required=True, help="Candidate runtime/run evidence JSON/YAML")
    parser.add_argument("--baseline-runtime-artifact", help="Baseline runtime/run evidence JSON/YAML")
    _add_output_args(parser)
    return parser


def _ring_group() -> Command:
    return _group(
        "ring",
        "Gate individual rollout rings",
        [FuncCommand("gate", _ring_gate_parser, _cmd_ring_gate)],
        "rollout_ring_cmd",
    )


def _ring_gate_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("gate", help="Gate a data product rollout ring")
    parser.add_argument("--plan", required=True, help="Rollout plan artifact")
    parser.add_argument("--ring", required=True, help="Ring id to evaluate")
    parser.add_argument("--shadow-validation", help="Optional shadow validation artifact")
    parser.add_argument("--promotion", action="append", help="Prior rollout promotion artifact; repeatable")
    parser.add_argument("--evidence-dir", help="Optional directory with data product gate artifacts")
    _add_output_args(parser)
    return parser


def _promote_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("promote", help="Emit a rollout ring promotion receipt")
    parser.add_argument("--ring-gate", required=True, help="Source ring gate artifact")
    parser.add_argument("--target-ring", required=True, help="Target ring id")
    parser.add_argument("--authority-gate", help="Optional authority gate artifact")
    _add_output_args(parser)
    return parser


def _report_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("report", help="Render rollout promotion report")
    parser.add_argument("--promotion", required=True, help="Rollout promotion artifact")
    _add_output_args(parser, default="md")
    return parser


def _group(name: str, help_text: str, subcommands: list[Command], subdest: str) -> Command:
    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser(name, help=help_text)

    return CommandGroup(name=name, help=help_text, build_parser=build, subcommands=subcommands, subdest=subdest)


def _add_output_args(parser: argparse.ArgumentParser, *, default: str = "text") -> None:
    parser.add_argument("--format", choices=["text", "json", "md", "table"], default=default)
    parser.add_argument("--output", help="Optional output artifact path")


def _emit_code(payload: dict[str, Any], output_format: str, output_path: str | None) -> int:
    return emit_data_product_artifact(
        payload,
        output_format,
        output_path,
        default_schema="dpone.data_product_rollout",
        text_keys=(
            "status",
            "product_id",
            "rollout_plan_id",
            "shadow_validation_id",
            "ring_gate_id",
            "rollout_promotion_id",
        ),
        markdown_title="Data Product Rollout",
        table_title="data product rollout",
    )


def _facade() -> Any:
    return import_module("dpone.services.data_product_rollout").DataProductRolloutFacade()


__all__ = ["rollout_group"]
