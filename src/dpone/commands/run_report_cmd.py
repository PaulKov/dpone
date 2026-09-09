from __future__ import annotations

import argparse
import logging

from dpone.commands.output_json import write_json
from dpone.commands.output_text import write_text
from dpone.readiness.managed import ManagedRenderer, RunArtifactWriter


def cmd_run_report(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    artifact = RunArtifactWriter(base_dir=args.out_dir).write(
        run_id=args.run_id,
        pipeline=args.pipeline,
        timeline=[{"stage": "manual", "status": "success", "rows": int(args.rows)}],
        state={"before": None, "after": {}},
        quality={"passed": True},
    )
    payload = artifact.to_dict()
    if args.format == "json":
        write_json(payload)
    elif args.format == "md":
        write_text(ManagedRenderer.render_markdown("dpone run-report", payload))
    else:
        write_text(ManagedRenderer.render_text("dpone run-report", payload))
    return 0


def register_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "run-report",
        help="Generate manual/synthetic run artifacts; does not read `dpone run` output",
    )
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--pipeline", required=True)
    parser.add_argument("--rows", default="0")
    parser.add_argument("--out-dir", default=".dpone/runs")
    parser.add_argument("--format", choices=["text", "json", "md"], default="text")
    return parser
