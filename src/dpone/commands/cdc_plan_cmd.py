from __future__ import annotations

import argparse
import logging

from dpone.commands.output_json import write_json
from dpone.commands.output_text import write_text
from dpone.commands.readiness_facade import build_readiness_service


def cmd_cdc_plan(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = build_readiness_service().cdc_plan(
        backend=args.backend,
        schema=args.schema,
        table=args.table,
        slot_name=args.slot_name,
        publication_name=args.publication_name,
        capture_instance=args.capture_instance,
    )
    if args.format == "json":
        write_json(payload)
    else:
        write_text((str(payload["sql"]) if payload["sql"] else "\n".join(payload["errors"])) + "\n")
    return 0 if payload["valid"] else 2


def _build_parser(subparsers: argparse._SubParsersAction, name: str) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(name, help="Generate CDC setup SQL for supported sources")
    parser.add_argument("--backend", required=True, choices=["postgres_logical", "mssql_cdc", "mssql_change_tracking"])
    parser.add_argument("--schema", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--slot-name")
    parser.add_argument("--publication-name")
    parser.add_argument("--capture-instance")
    parser.add_argument("--format", choices=["text", "json"], default="text")
    return parser


def register_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    return _build_parser(subparsers, "cdc-plan")


def register_plan_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    return _build_parser(subparsers, "plan")
