from __future__ import annotations

import argparse
import logging
from importlib import import_module
from pathlib import Path
from typing import Any

from dpone.commands.base import Command
from dpone.commands.func_command import CommandGroup, FuncCommand
from dpone.commands.output_text import write_text
from dpone.readiness import data_product_audit_retention_rendering as rendering


def audit_group() -> Command:
    subcommands = [archive_group(), retention_group(), legal_hold_group()]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("audit", help="Archive and retain data product audit evidence")

    return CommandGroup(
        name="audit",
        help="Archive and retain data product audit evidence",
        build_parser=build,
        subcommands=subcommands,
        subdest="data_product_audit_cmd",
    )


def archive_group() -> Command:
    subcommands = [
        FuncCommand("plan", _archive_plan_parser, _cmd_archive_plan),
        FuncCommand("run", _archive_run_parser, _cmd_archive_run),
        FuncCommand("verify", _archive_verify_parser, _cmd_archive_verify),
    ]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("archive", help="Plan, run and verify audit evidence archives")

    return CommandGroup(
        name="archive",
        help="Plan, run and verify audit evidence archives",
        build_parser=build,
        subcommands=subcommands,
        subdest="data_product_audit_archive_cmd",
    )


def retention_group() -> Command:
    subcommands = [FuncCommand("plan", _retention_plan_parser, _cmd_retention_plan)]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("retention", help="Plan non-destructive audit evidence retention")

    return CommandGroup(
        name="retention",
        help="Plan non-destructive audit evidence retention",
        build_parser=build,
        subcommands=subcommands,
        subdest="data_product_audit_retention_cmd",
    )


def legal_hold_group() -> Command:
    subcommands = [FuncCommand("apply", _legal_hold_apply_parser, _cmd_legal_hold_apply)]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("legal-hold", help="Apply legal hold evidence to audit archives")

    return CommandGroup(
        name="legal-hold",
        help="Apply legal hold evidence to audit archives",
        build_parser=build,
        subcommands=subcommands,
        subdest="data_product_audit_legal_hold_cmd",
    )


def _cmd_archive_plan(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().archive_plan(
        manifest_path=args.manifest,
        bundle_path=args.bundle,
        registry_path=args.registry,
        evidence_dir=args.evidence_dir,
    )
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def _cmd_archive_run(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().archive_run(plan_path=args.plan, execute=args.execute)
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def _cmd_archive_verify(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().archive_verify(archive_run_path=args.archive_run)
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def _cmd_retention_plan(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().retention_plan(
        manifest_path=args.manifest,
        archive_verification_path=args.archive_verification,
        legal_hold_path=args.legal_hold,
    )
    _emit(payload, args.format, args.output, title="Data Product Audit Retention Plan")
    return 2 if payload.get("status") == "blocked" else 0


def _cmd_legal_hold_apply(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().legal_hold_apply(
        archive_run_path=args.archive_run,
        reason=args.reason,
        authority_gate_path=args.authority_gate,
    )
    _emit(payload, args.format, args.output)
    return 2 if payload.get("status") == "blocked" else 0


def _archive_plan_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("plan", help="Build an audit archive plan")
    parser.add_argument("--manifest", required=True, help="Manifest path")
    parser.add_argument("--bundle", help="Optional schema migration bundle JSON/YAML")
    parser.add_argument("--registry", help="Optional evidence registry JSON/SQLite")
    parser.add_argument("--evidence-dir", help="Optional directory with local evidence artifacts")
    _add_output_args(parser)
    return parser


def _archive_run_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("run", help="Write an audit archive, or emit dry-run receipt")
    parser.add_argument("--plan", required=True, help="Audit archive plan artifact")
    parser.add_argument("--execute", action="store_true", help="Write archive files")
    _add_output_args(parser)
    return parser


def _archive_verify_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("verify", help="Verify an audit archive run")
    parser.add_argument("--archive-run", required=True, help="Audit archive run artifact")
    _add_output_args(parser)
    return parser


def _retention_plan_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("plan", help="Build a non-destructive audit retention plan")
    parser.add_argument("--manifest", required=True, help="Manifest path")
    parser.add_argument("--archive-verification", required=True, help="Archive verification artifact")
    parser.add_argument("--legal-hold", help="Optional legal hold artifact")
    _add_output_args(parser, default="md")
    return parser


def _legal_hold_apply_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("apply", help="Apply legal hold evidence to an audit archive")
    parser.add_argument("--archive-run", required=True, help="Audit archive run artifact")
    parser.add_argument("--reason", required=True, help="Legal hold reason")
    parser.add_argument("--authority-gate", help="Optional authority gate evidence")
    _add_output_args(parser)
    return parser


def _add_output_args(parser: argparse.ArgumentParser, *, default: str = "text") -> None:
    parser.add_argument("--format", choices=["text", "json", "md", "table"], default=default)
    parser.add_argument("--output", help="Optional output artifact path")


def _emit(payload: dict[str, Any], output_format: str, output_path: str | None, *, title: str | None = None) -> None:
    rendered = _render(payload, output_format, title=title)
    if output_path:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(rendered, encoding="utf-8")
    write_text(rendered)


def _render(payload: dict[str, Any], output_format: str, *, title: str | None = None) -> str:
    if output_format == "json":
        return rendering.render_json(payload)
    if output_format == "md":
        return rendering.render_markdown(payload, title=title or "Data Product Audit Evidence")
    if output_format == "table":
        return rendering.render_table(payload)
    return rendering.render_text(payload)


def _facade() -> Any:
    return import_module("dpone.services.data_product_audit").DataProductAuditFacade()


__all__ = ["audit_group"]
