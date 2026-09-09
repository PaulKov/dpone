"""CLI commands for Data Product Trust Center evidence."""

from __future__ import annotations

import argparse
import logging
from importlib import import_module
from typing import Any

from dpone.cli_render.data_product_artifacts import emit_data_product_artifact
from dpone.commands.base import Command
from dpone.commands.func_command import CommandGroup, FuncCommand


def trust_group() -> Command:
    return _group(
        "trust",
        "Index, query and gate data product trust evidence",
        [
            _lake_group(),
            FuncCommand("query", _query_parser, _cmd_query),
            FuncCommand("snapshot", _snapshot_parser, _cmd_snapshot),
            FuncCommand("gate", _gate_parser, _cmd_gate),
            FuncCommand("report", _report_parser, _cmd_report),
            FuncCommand("export", _export_parser, _cmd_export),
        ],
        "data_product_trust_cmd",
    )


def _cmd_index(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().index_lake(
        manifest_patterns=tuple(args.manifests),
        evidence_dir=args.evidence_dir,
        registry_path=args.registry,
        bundle_dir=args.bundle_dir,
    )
    return _emit_code(payload, args.format, args.output)


def _cmd_query(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().query(
        index_path=args.index,
        product_id=args.product_id,
        domain=args.domain,
        artifact_kind=args.artifact_kind,
        status=args.status,
        owner=args.owner,
        max_results=args.max_results,
    )
    return _emit_code(payload, args.format, args.output)


def _cmd_snapshot(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().snapshot(index_path=args.index, product_id=args.product_id, profile=args.profile)
    return _emit_code(payload, args.format, args.output)


def _cmd_gate(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().gate(snapshot_path=args.snapshot, profile=args.profile)
    return _emit_code(payload, args.format, args.output)


def _cmd_report(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().report(snapshot_path=args.snapshot, gate_path=args.gate)
    return _emit_code(payload, args.format, args.output)


def _cmd_export(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _facade().export(snapshot_path=args.snapshot, target=args.target)
    return _emit_code(payload, args.format, args.output)


def _lake_group() -> Command:
    return _group(
        "lake",
        "Build Trust Center evidence lake indexes",
        [FuncCommand("index", _index_parser, _cmd_index)],
        "data_product_trust_lake_cmd",
    )


def _index_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("index", help="Build a Trust Center evidence lake index")
    parser.add_argument("--manifests", required=True, nargs="+", help="Manifest path or glob; repeatable")
    parser.add_argument("--registry", help="Optional evidence registry JSON/SQLite")
    parser.add_argument("--bundle-dir", help="Optional schema migration bundle directory")
    parser.add_argument("--evidence-dir", help="Optional directory with data product artifacts")
    _add_output_args(parser)
    return parser


def _query_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("query", help="Query Trust Center evidence refs")
    parser.add_argument("--index", required=True, help="Evidence lake index artifact")
    parser.add_argument("--product-id", help="Filter by data product id")
    parser.add_argument("--domain", help="Filter by trust evidence domain")
    parser.add_argument("--artifact-kind", help="Filter by artifact kind")
    parser.add_argument("--status", help="Filter by evidence status")
    parser.add_argument("--owner", help="Filter by owner")
    parser.add_argument("--max-results", type=int, default=500, help="Maximum refs to return")
    _add_output_args(parser)
    return parser


def _snapshot_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("snapshot", help="Render a product trust snapshot")
    parser.add_argument("--index", required=True, help="Evidence lake index artifact")
    parser.add_argument("--product-id", required=True, help="Data product id")
    _add_profile(parser)
    _add_output_args(parser)
    return parser


def _gate_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("gate", help="Gate a data product trust snapshot")
    parser.add_argument("--snapshot", required=True, help="Trust snapshot artifact")
    _add_profile(parser)
    _add_output_args(parser)
    return parser


def _report_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("report", help="Render a Trust Center report")
    parser.add_argument("--snapshot", required=True, help="Trust snapshot artifact")
    parser.add_argument("--gate", help="Optional trust gate artifact")
    _add_output_args(parser, default="md")
    return parser


def _export_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("export", help="Render a local Trust Center export payload")
    parser.add_argument("--snapshot", required=True, help="Trust snapshot artifact")
    parser.add_argument("--target", choices=["json", "datahub", "openlineage", "opa"], default="json")
    _add_output_args(parser)
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
        default_schema="dpone.data_product_trust",
        text_keys=(
            "status",
            "product_id",
            "evidence_lake_index_id",
            "trust_query_result_id",
            "trust_snapshot_id",
            "trust_gate_id",
            "trust_report_id",
            "trust_export_id",
        ),
        markdown_title="Data Product Trust Center",
        table_title="data product trust",
    )


def _facade() -> Any:
    return import_module("dpone.services.data_product_trust").DataProductTrustFacade()


__all__ = ["trust_group"]
