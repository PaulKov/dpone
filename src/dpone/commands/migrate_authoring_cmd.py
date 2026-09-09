"""CLI facade for explicit, semantically checked authoring migration."""

from __future__ import annotations

import argparse
import logging

from dpone.commands.airflow_self_service_output import emit_authoring_migration_payload
from dpone.manifest.authoring_migration_service import AuthoringMigrationService

_MODES = ("classic", "flow", "folder")


def register_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "authoring",
        help="Plan or apply a semantics-preserving authoring-mode migration",
    )
    parser.add_argument("target", help="Pipeline id, directory, or pipeline.yaml path")
    parser.add_argument("--from", dest="source_mode", choices=_MODES, help="Assert the detected source mode")
    parser.add_argument("--to", dest="target_mode", choices=_MODES, required=True, help="Target authoring mode")
    operation = parser.add_mutually_exclusive_group(required=True)
    operation.add_argument("--plan", action="store_true", help="Show the deterministic migration plan")
    operation.add_argument("--apply", action="store_true", help="Recompute and safely apply the migration")
    parser.add_argument("--format", choices=("text", "json"), default="text")
    return parser


def cmd_migrate_authoring(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    result = AuthoringMigrationService(root=".").migrate(
        target=args.target,
        target_mode=args.target_mode,
        expected_source_mode=args.source_mode,
        apply=bool(args.apply),
    )
    emit_authoring_migration_payload(result.to_jsonable(), args.format)
    return result.exit_code


__all__ = ["cmd_migrate_authoring", "register_parser"]
