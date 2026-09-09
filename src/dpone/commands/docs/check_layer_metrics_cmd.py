from __future__ import annotations

import argparse
import logging

from dpone.commands.output_json import write_json
from dpone.commands.output_text import write_text

from ...services.docs.check_layer_metrics_service import CheckLayerMetricsService
from ..context import DocsCommandContext

DEFAULT_BASELINE = "docs/layer_metrics_baseline.json"


def cmd_docs_check_layer_metrics(args: argparse.Namespace, *, ctx: DocsCommandContext, logger: logging.Logger) -> int:
    svc = CheckLayerMetricsService(ctx=ctx)
    exit_code, payload = svc.run(args)
    if isinstance(payload, dict):
        write_json(payload)
    else:
        write_text(payload)
    return int(exit_code)


def register_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser(
        "check-layer-metrics",
        help="Check coarse layer/slice architecture metrics against thresholds and baseline (CI-friendly)",
    )
    p.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format: text (default) or json",
    )
    p.add_argument(
        "--package",
        default="src/dpone",
        help="Package directory relative to repo root (default: src/dpone)",
    )
    p.add_argument(
        "--top",
        type=int,
        default=15,
        help="How many top cross flows to retain in the computed snapshot (default: 15)",
    )
    p.add_argument(
        "--exclude-layer",
        action="append",
        default=["dpone.compat"],
        help="Layer to exclude from the coarse architecture view (repeatable, default: dpone.compat)",
    )
    baseline_group = p.add_mutually_exclusive_group()
    baseline_group.add_argument(
        "--baseline",
        default=DEFAULT_BASELINE,
        help=f"Baseline JSON path relative to repo root (default: {DEFAULT_BASELINE})",
    )
    baseline_group.add_argument(
        "--no-baseline",
        action="store_true",
        help="Do not compare against a baseline JSON snapshot",
    )
    p.add_argument(
        "--write-baseline",
        action="store_true",
        help="Write current layer metrics snapshot to --baseline and exit 0",
    )
    p.add_argument(
        "--min-intra-layer-ratio",
        type=float,
        default=0.55,
        help="Fail if current intra-layer ratio is below this threshold (default: 0.55)",
    )
    p.add_argument(
        "--max-cross-layer-ratio",
        type=float,
        default=0.45,
        help="Fail if current cross-layer ratio is above this threshold (default: 0.45)",
    )
    p.add_argument(
        "--max-top-cross-layer-flow",
        type=int,
        default=None,
        help="Optional absolute cap for the largest layer->layer flow",
    )
    p.add_argument(
        "--allowed-ratio-regression",
        type=float,
        default=0.02,
        help="Allowed regression vs baseline for ratio metrics (default: 0.02)",
    )
    p.add_argument(
        "--allowed-flow-regression",
        type=int,
        default=5,
        help="Allowed regression vs baseline for largest cross-layer flow (default: 5)",
    )
    return p
