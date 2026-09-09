from __future__ import annotations

import argparse
import logging
from typing import cast

from dpone.commands.func_command import CommandGroup, FuncCommand
from dpone.commands.gitops.common import GitOpsOutputContext, write_optional_output
from dpone.commands.output_json import dumps_json, write_json
from dpone.commands.output_text import write_text
from dpone.services.gitops.schema_catalog_service import GitOpsSchemaCatalogService


def schema_group() -> CommandGroup:
    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser(
            "schema",
            help="GitOps schema catalog",
            description="GitOps schema catalog",
        )

    return CommandGroup(
        name="schema",
        help="GitOps schema catalog",
        build_parser=build,
        subcommands=[
            FuncCommand("list", register_list_parser, cmd_gitops_schema_list),
            FuncCommand("show", register_show_parser, cmd_gitops_schema_show),
            FuncCommand("validate", register_validate_parser, cmd_gitops_schema_validate),
        ],
        subdest="gitops_schema_cmd",
    )


def register_list_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("list", help="List registered GitOps JSON Schema contracts")
    parser.add_argument("--prefix", help="Optional contract name or kind prefix")
    _add_output_args(parser)
    return parser


def register_show_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("show", help="Show one registered GitOps JSON Schema contract")
    parser.add_argument("kind", help="Contract name or kind, for example dpone.safe-sample-runtime-run.v1")
    _add_output_args(parser)
    return parser


def register_validate_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("validate", help="Validate a JSON payload against a GitOps schema contract")
    parser.add_argument("--kind", required=True, help="Contract name or kind")
    parser.add_argument("--payload", required=True, help="JSON payload path")
    _add_output_args(parser)
    return parser


def cmd_gitops_schema_list(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    payload = GitOpsSchemaCatalogService().list_contracts(prefix=args.prefix)
    return _emit(payload, args=args, ctx=ctx)


def cmd_gitops_schema_show(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    payload = GitOpsSchemaCatalogService().show_contract(args.kind)
    return _emit(payload, args=args, ctx=ctx)


def cmd_gitops_schema_validate(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    payload = GitOpsSchemaCatalogService().validate_payload(raw_kind=args.kind, payload_path=args.payload)
    return _emit(payload, args=args, ctx=ctx)


def _add_output_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--output", help="Optional repo-relative output artifact path")
    parser.add_argument("--format", choices=("json", "markdown"), default="json")


def _emit(payload: dict[str, object], *, args: argparse.Namespace, ctx: object) -> int:
    rendered = _render(payload, getattr(args, "format", "json"))
    write_optional_output(cast(GitOpsOutputContext, ctx), getattr(args, "output", None), rendered)
    if getattr(args, "format", "json") == "json":
        write_json(payload)
    else:
        write_text(rendered)
    return 0 if payload.get("passed", True) is not False else 1


def _render(payload: dict[str, object], output_format: str) -> str:
    if output_format == "markdown":
        return f"# {payload.get('kind', 'gitops.schema')}\n\n```json\n{dumps_json(payload)}```\n"
    return dumps_json(payload)


__all__ = [
    "cmd_gitops_schema_list",
    "cmd_gitops_schema_show",
    "cmd_gitops_schema_validate",
    "register_list_parser",
    "register_show_parser",
    "register_validate_parser",
    "schema_group",
]
