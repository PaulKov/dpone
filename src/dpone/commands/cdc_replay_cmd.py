from __future__ import annotations

import argparse
import logging

from dpone.commands.output_json import write_json
from dpone.commands.output_text import write_text
from dpone.readiness.cdc import CDCBackend
from dpone.services.cdc_replay import CDCReplayPlanRequest, CDCReplayPlanService


def cmd_cdc_replay_plan(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    plan = CDCReplayPlanService().plan(
        CDCReplayPlanRequest(
            backend=args.backend,
            pipeline_name=args.pipeline_name,
            source_schema=args.schema,
            source_table=args.table,
            stored_offset=args.stored_offset,
            replay_from=args.replay_from,
            replay_to=args.replay_to,
            retention_min=args.retention_min,
            high_watermark=args.high_watermark,
            artifact_uri=args.artifact_uri,
            allow_rewind=args.allow_rewind,
        )
    )
    if args.format == "json":
        write_json(plan.to_dict())
    else:
        write_text(plan.to_markdown())
    return 0 if plan.safe_to_execute else 1


def register_replay_plan_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("replay-plan", help="Plan safe bounded CDC replay and offset advancement")
    parser.add_argument("--backend", required=True, choices=[item.value for item in CDCBackend])
    parser.add_argument("--pipeline-name", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--stored-offset")
    parser.add_argument("--replay-from", required=True)
    parser.add_argument("--replay-to")
    parser.add_argument("--retention-min")
    parser.add_argument("--high-watermark")
    parser.add_argument("--artifact-uri")
    parser.add_argument("--allow-rewind", action="store_true")
    parser.add_argument("--format", choices=["md", "json"], default="md")
    return parser
