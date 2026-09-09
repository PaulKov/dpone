from __future__ import annotations

import argparse
import logging
import sys
from types import ModuleType

from dpone.commands import replay_cmd_support as _default_replay_support
from dpone.commands.output_json import write_json
from dpone.commands.output_text import write_text
from dpone.strategy_intelligence.replay import ReplayExecutionRequest


def cmd_resync(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    result = (
        _replay_support()
        .build_replay_execution_service(args)
        .execute(
            ReplayExecutionRequest(
                action="resync",
                run_id=args.run_id,
                source_type=args.source_type,
                sink_type=args.sink_type,
                strategy_mode=args.strategy,
                partitions=tuple(args.partition or ()),
                yes=bool(args.yes),
            )
        )
    )
    payload = result.to_dict()
    if args.format == "json":
        write_json(payload)
    elif args.format == "md":
        write_text(_render_md(payload))
    else:
        write_text(_render_text(payload))
    return 0


def register_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("resync", help="Plan or execute a safe partition/table resync replay")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--source-type", required=True)
    parser.add_argument("--sink-type", required=True)
    parser.add_argument("--strategy", required=True)
    parser.add_argument("--partition", action="append")
    parser.add_argument("--artifact-dir", default=".dpone/replay")
    parser.add_argument("--yes", action="store_true", help="Mark replay as approved for execution")
    _replay_support().add_live_replay_arguments(parser)
    parser.add_argument("--format", choices=["text", "json", "md"], default="text")
    return parser


def _replay_support() -> ModuleType:
    current = sys.modules.get("dpone.commands.replay_cmd_support")
    return current if isinstance(current, ModuleType) else _default_replay_support


def _render_text(payload: dict) -> str:
    return "\n".join(
        [
            "dpone resync",
            f"- run_id: {payload['run_id']}",
            f"- mode: {payload['mode']}",
            f"- executed: {payload['executed']}",
            f"- artifact_path: {payload['artifact_path']}",
            "",
        ]
    )


def _render_md(payload: dict) -> str:
    return "\n".join(
        [
            "# dpone resync",
            "",
            f"- run_id: `{payload['run_id']}`",
            f"- mode: `{payload['mode']}`",
            f"- executed: `{payload['executed']}`",
            f"- artifact_path: `{payload['artifact_path']}`",
            "",
        ]
    )
