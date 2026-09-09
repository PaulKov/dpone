from __future__ import annotations

import argparse
import logging
from pathlib import Path

from dpone.commands.hooks_service import HookExecuteService
from dpone.commands.output_json import dumps_json, write_json
from dpone.commands.output_text import write_text


def register_execute_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("execute", help="Execute one manifest hook phase or hook action")
    parser.add_argument("path", help="Path to a YAML manifest")
    parser.add_argument("--selector", help="Process selector inside a batch manifest")
    parser.add_argument("--phase", choices=["pre_hook", "post_hook"], required=True)
    parser.add_argument("--hook-id", help="Optional hook action id")
    parser.add_argument("--dry-run", action="store_true", help="Render selected hook action without executing it")
    parser.add_argument("--registry", action="append", default=[], help="Additional source registry YAML path")
    parser.add_argument("--format", choices=["json", "text"], default="text")
    parser.add_argument("--output", help="Optional report output path")
    return parser


def cmd_hooks_execute(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    report = HookExecuteService().execute(
        path=Path(args.path),
        registry_paths=args.registry,
        selector=args.selector,
        phase=args.phase,
        hook_id=args.hook_id,
        dry_run=bool(args.dry_run),
        repo_root=_repo_root(ctx),
    )
    _emit_report(report, output_format=args.format, output_path=args.output)
    return 0 if report.passed else 1


def _emit_report(report, *, output_format: str, output_path: str | None) -> None:
    rendered = dumps_json(report.to_jsonable()) if output_format == "json" else report.to_text()
    if output_path:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(rendered, encoding="utf-8")
        return
    if output_format == "json":
        write_json(report.to_jsonable())
    else:
        write_text(rendered)


def _repo_root(ctx: object) -> Path | None:
    settings = getattr(ctx, "settings", None)
    value = getattr(settings, "repo_root", None)
    return Path(value) if value else None
