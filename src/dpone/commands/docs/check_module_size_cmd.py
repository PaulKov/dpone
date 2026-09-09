from __future__ import annotations

import argparse
import logging

from dpone.adapters.project_authoring_lock import project_authoring_lock
from dpone.commands.output_json import write_json
from dpone.commands.output_text import write_text

from ...services.docs.check_module_size_service import CheckModuleSizeService
from ..context import DocsCommandContext

DEFAULT_BASELINE = "docs/module_size_baseline.json"


def _positive_integer(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be a positive integer") from exc
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def _nonempty_baseline(value: str) -> str:
    if not value.strip():
        raise argparse.ArgumentTypeError("must not be empty; use --no-baseline")
    return value


def cmd_docs_check_module_size(args: argparse.Namespace, *, ctx: DocsCommandContext, logger: logging.Logger) -> int:
    svc = CheckModuleSizeService(ctx=ctx, authoring_lock=project_authoring_lock)
    exit_code, payload = svc.run(args)
    if isinstance(payload, dict):
        write_json(payload)
    else:
        write_text(payload)
    return int(exit_code)


def register_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser(
        "check-module-size",
        help="Check Python module LOC/SLOC thresholds for a package, tool, or test directory",
    )
    p.add_argument("--format", choices=["text", "json"], default="text")
    p.add_argument("--package", default="src/dpone", help="Python directory to scan (default: src/dpone)")
    baseline_group = p.add_mutually_exclusive_group()
    baseline_group.add_argument(
        "--baseline",
        default=DEFAULT_BASELINE,
        type=_nonempty_baseline,
        help=f"Closed v2 debt baseline (default: {DEFAULT_BASELINE})",
    )
    baseline_group.add_argument(
        "--no-baseline",
        action="store_true",
        help="Advisory warning scan for an explicit non-ledger package; hard limits still fail",
    )
    p.add_argument(
        "--write-baseline",
        action="store_true",
        help="Write an atomic candidate and exit 2; commit it, then rerun the exact head without this flag",
    )
    p.add_argument(
        "--base-ref",
        help="Exact 40-character lowercase base commit SHA required for baseline governance",
    )
    p.add_argument(
        "--head-ref",
        help="Exact 40-character lowercase checked-out head commit SHA required for baseline governance",
    )
    p.add_argument("--warn-lines", type=_positive_integer, default=450, help="Warning LOC threshold (default: 450)")
    p.add_argument("--max-lines", type=_positive_integer, default=600, help="Hard LOC limit (default: 600)")
    p.add_argument("--warn-sloc", type=_positive_integer, default=350, help="Warning SLOC threshold (default: 350)")
    p.add_argument("--max-sloc", type=_positive_integer, default=400, help="Hard SLOC limit (default: 400)")
    return p
