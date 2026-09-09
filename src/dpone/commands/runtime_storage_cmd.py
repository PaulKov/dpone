from __future__ import annotations

import argparse
import logging

from dpone.commands.output_json import write_json
from dpone.commands.output_text import write_text
from dpone.readiness.runtime_storage_gc import StorageGcService


def cmd_runtime_storage_gc(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    report = StorageGcService().collect(
        work_dir=args.work_dir,
        older_than_seconds=args.older_than_seconds,
        dry_run=bool(args.dry_run),
    )
    payload = report.to_dict()
    if args.format == "json":
        write_json(payload)
    else:
        lines = [
            "# dpone runtime storage gc",
            "",
            f"- work_dir: `{payload['work_dir']}`",
            f"- dry_run: `{payload['dry_run']}`",
            f"- candidate_count: `{payload['candidate_count']}`",
            f"- deleted_count: `{payload['deleted_count']}`",
        ]
        write_text("\n".join(lines) + "\n")
    return 0


def register_gc_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("gc", help="Clean old runtime storage work/debug files")
    parser.add_argument("--work-dir", required=True)
    parser.add_argument("--older-than-seconds", type=int, default=0)
    parser.add_argument("--dry-run", dest="dry_run", action="store_true", default=True)
    parser.add_argument("--apply", dest="dry_run", action="store_false")
    parser.add_argument("--format", choices=["json", "md"], default="json")
    return parser


__all__ = ["cmd_runtime_storage_gc", "register_gc_parser"]
