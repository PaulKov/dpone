from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Any

import yaml

from dpone.commands.output_json import write_json
from dpone.commands.output_text import write_text
from dpone.load_profile import LoadProfileAdvisor, ManifestProfileRequestBuilder
from dpone.load_profile.rendering import render_profile_md, render_profile_text


def cmd_profile_advise(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _advice_payload(args)
    if args.format == "json":
        write_json(payload)
    elif args.format == "md":
        write_text(render_profile_md(payload))
    else:
        write_text(render_profile_text(payload))
    return 0


def cmd_profile_wizard(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    payload = _advice_payload(args)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        yaml.safe_dump(payload["recommended_patch"], sort_keys=False, allow_unicode=True), encoding="utf-8"
    )
    result = {"schema_version": "dpone.profile.wizard.v1", "patch_path": str(output), "advice": payload}
    if args.format == "json":
        write_json(result)
    elif args.format == "md":
        write_text(f"# dpone profile wizard\n\n- patch: `{output}`\n\n" + render_profile_md(payload))
    else:
        write_text(f"dpone profile wizard\n- patch_path: {output}\n")
    return 0


def register_advise_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("advise", help="Recommend the fastest safe load profile")
    _add_common_args(parser)
    return parser


def register_wizard_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("wizard", help="Write a manifest patch for the recommended load profile")
    _add_common_args(parser)
    parser.add_argument("--output", required=True, help="YAML patch output path")
    return parser


def _add_common_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("path")
    parser.add_argument("--row-count", type=int)
    parser.add_argument("--column-count", type=int)
    parser.add_argument("--estimated-bytes-per-row", type=int)
    parser.add_argument("--worker-profile", default="balanced")
    parser.add_argument(
        "--goal",
        choices=["balanced", "performance", "safety", "cost"],
        default="balanced",
        help="Optimization intent for recommendations. Use performance to prefer larger safe windows.",
    )
    parser.add_argument("--target-chunk-bytes")
    parser.add_argument("--current-max-chunk-rows", type=int)
    parser.add_argument("--format", choices=["text", "json", "md"], default="text")


def _advice_payload(args: argparse.Namespace) -> dict[str, Any]:
    request = ManifestProfileRequestBuilder().build(
        args.path,
        row_count=args.row_count,
        column_count=args.column_count,
        estimated_bytes_per_row=args.estimated_bytes_per_row,
        worker_profile=args.worker_profile,
        optimization_goal=args.goal,
        target_chunk_bytes=args.target_chunk_bytes,
        current_max_chunk_rows=args.current_max_chunk_rows,
    )
    return LoadProfileAdvisor().advise(request).to_dict()


__all__ = [
    "cmd_profile_advise",
    "cmd_profile_wizard",
    "register_advise_parser",
    "register_wizard_parser",
]
