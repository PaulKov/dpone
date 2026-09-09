"""`dpone docs check-architecture-fitness` command."""

from __future__ import annotations

import argparse
import logging

from dpone.commands.output_json import write_json
from dpone.commands.output_text import write_text

from ...services.docs.check_architecture_fitness_service import CheckArchitectureFitnessService
from ..context import DocsCommandContext


def cmd_docs_check_architecture_fitness(
    args: argparse.Namespace, *, ctx: DocsCommandContext, logger: logging.Logger
) -> int:
    svc = CheckArchitectureFitnessService(ctx=ctx)
    exit_code, payload = svc.run(args)
    if isinstance(payload, dict):
        write_json(payload)
    else:
        write_text(payload)
    return int(exit_code)


def register_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser(
        "check-architecture-fitness",
        help="Check architecture fitness for coupling and class responsibility drift",
    )
    p.add_argument("--format", choices=["text", "json"], default="text")
    p.add_argument("--package", default="src/dpone")
    p.add_argument("--top", type=int, default=20)
    p.add_argument("--target-avg-clustering", type=float, default=0.18)
    p.add_argument(
        "--max-avg-clustering",
        type=float,
        default=None,
        help="Hard graph budget; defaults to docs/benchmarks/quality_budgets.yml.",
    )
    p.add_argument("--max-cross-layer-ratio", type=float, default=0.35)
    p.add_argument("--target-module-ce", type=int, default=40)
    p.add_argument("--max-module-ce", type=int, default=60)
    p.add_argument("--max-class-methods", type=int, default=24)
    p.add_argument("--max-class-loc", type=int, default=450)
    p.add_argument(
        "--fail-on-class-warnings",
        action="store_true",
        help="Treat high-responsibility class findings as errors instead of warnings.",
    )
    return p
