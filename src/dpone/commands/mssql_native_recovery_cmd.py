"""Thin CLI adapters for source-free MSSQL native recovery inspection."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Any

from dpone.adapters.mssql_native_recovery_journal import MssqlNativeRecoveryJournalReader
from dpone.app.mssql_native_recovery_application import MssqlNativeRecoveryApplication
from dpone.commands.output_json import write_json
from dpone.commands.output_text import write_text
from dpone.dag.config_models import ETLProcessConfig


def _render(payload: dict[str, Any], output_format: str) -> None:
    if output_format == "json":
        write_json(payload)
    else:
        if payload["kind"] == "dpone.mssql-native-recovery-index.v1":
            lines = [
                "# MSSQL native recovery",
                "",
                f"- retained_custody_count: `{payload['retained_custody_count']}`",
                f"- invocation_count: `{len(payload['items'])}`",
            ]
            for item in payload["items"]:
                lines.extend(
                    [
                        "",
                        f"## `{item['invocation_id']}`",
                        "",
                        f"- state: `{item['state']}`",
                        f"- diagnostic_code: `{item['diagnostic_code']}`",
                        f"- permitted_actions: `{', '.join(item['permitted_actions'])}`",
                    ]
                )
            write_text("\n".join(lines) + "\n")
        else:
            write_text(
                "# MSSQL native recovery\n\n"
                f"- invocation_id: `{payload['invocation_id']}`\n"
                f"- state: `{payload['state']}`\n"
                f"- diagnostic_code: `{payload['diagnostic_code']}`\n"
                f"- permitted_actions: `{', '.join(payload['permitted_actions'])}`\n"
            )


def _reader(args: argparse.Namespace) -> MssqlNativeRecoveryJournalReader:
    return MssqlNativeRecoveryJournalReader(Path(args.journal_root))


def cmd_inspect(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    try:
        payload = _reader(args).inspect(args.invocation_id)
    except (OSError, ValueError) as error:
        print(str(error), file=sys.stderr)
        return 1
    _render(payload, args.format)
    return 0


def cmd_inspect_all(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    try:
        payload = _reader(args).inspect_all()
    except (OSError, ValueError) as error:
        print(str(error), file=sys.stderr)
        return 1
    _render(payload, args.format)
    return 0


def _mutate(args: argparse.Namespace, *, action: str) -> int:
    if args.yes is not True:
        print("mssql_native.recovery_confirmation_required", file=sys.stderr)
        return 1
    try:
        process = ETLProcessConfig.from_yaml(args.manifest)
        payload = MssqlNativeRecoveryApplication().execute(
            process,
            journal_root=Path(args.journal_root),
            invocation_id=args.invocation_id,
            action=action,
            owner=args.owner,
            confirmed=args.yes,
        )
    except (OSError, RuntimeError, ValueError) as error:
        print(str(error), file=sys.stderr)
        return 1
    _render(payload, args.format)
    return 0


def cmd_reconcile(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    return _mutate(args, action="reconcile")


def cmd_resume(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    return _mutate(args, action="resume")


def cmd_retire(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    return _mutate(args, action="retire")


def _common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--journal-root", required=True, help="SQLite journal or directory containing window-state.sqlite"
    )
    parser.add_argument("--format", choices=("json", "md"), default="json")


def register_inspect_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("inspect", help="Inspect one opaque invocation without target I/O")
    parser.add_argument("invocation_id")
    _common(parser)
    return parser


def register_inspect_all_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("inspect-all", help="List retained native recovery authorities")
    _common(parser)
    return parser


def _register_mutation_parser(
    subparsers: argparse._SubParsersAction, name: str, help_text: str
) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(name, help=help_text)
    parser.add_argument("invocation_id")
    parser.add_argument("--manifest", required=True, help="Manifest path, optionally followed by #selector")
    parser.add_argument("--owner", default=f"dpone-native-recovery-{name}")
    parser.add_argument("--yes", action="store_true", help="Required: confirm the target mutation")
    _common(parser)
    return parser


def register_reconcile_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    return _register_mutation_parser(
        subparsers, "reconcile", "Reobserve an unknown writer or publication outcome without source I/O"
    )


def register_resume_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    return _register_mutation_parser(
        subparsers, "resume", "Resume verified preparation/publication without reopening the source"
    )


def register_retire_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    return _register_mutation_parser(
        subparsers, "retire", "Retire exact owned pre-publication stages after durable proof"
    )


__all__ = [
    "cmd_inspect",
    "cmd_inspect_all",
    "cmd_reconcile",
    "cmd_resume",
    "cmd_retire",
    "register_inspect_all_parser",
    "register_inspect_parser",
    "register_reconcile_parser",
    "register_resume_parser",
    "register_retire_parser",
]
