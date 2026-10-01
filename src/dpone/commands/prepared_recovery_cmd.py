"""Thin native recovery journey; standalone CLI has no exclusion authority."""

from __future__ import annotations

import argparse
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING

from dpone.commands.func_command import CommandGroup, FuncCommand
from dpone.commands.output_json import write_json

if TYPE_CHECKING:
    import logging


def prepared_recovery_command() -> CommandGroup:
    return CommandGroup(
        name="recover",
        help="Explicit native publication recovery",
        build_parser=lambda sub: sub.add_parser("recover", help="Plan or execute native recovery in a trusted runner"),
        subcommands=[
            FuncCommand(action, partial(_register, action=action), _execute, _requires_app_context=False)
            for action in ("plan", "execute")
        ],
        subdest="prepared_recovery_action",
    )


def _register(sub: argparse._SubParsersAction, *, action: str) -> argparse.ArgumentParser:
    parser = sub.add_parser(action)
    parser.add_argument("--environment", required=True, help="Exact verified runtime environment")
    parser.add_argument("--plan-file", type=Path, required=True, help="Owner-private plan; refuses overwrite")
    if action == "plan":
        for name in ("connection-ref", "sink-connection-ref", "cluster", "database", "target", "operation-id"):
            parser.add_argument(f"--{name}", required=True)
        parser.add_argument("--expected-version", required=True, type=int, help="Exact native PREPARED revision")
    else:
        parser.add_argument("--confirm-digest", required=True, help="Reviewed outer operator plan SHA-256")
    return parser


def _execute(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    from dpone.app.prepared_recovery_application import PreparedRecoveryApplication

    application = PreparedRecoveryApplication()
    if args.prepared_recovery_action == "plan":
        result = application.plan(
            connection_ref=args.connection_ref,
            sink_connection_ref=args.sink_connection_ref,
            environment=args.environment,
            cluster=args.cluster,
            database=args.database,
            target=args.target,
            operation_id=args.operation_id,
            expected_version=args.expected_version,
            path=args.plan_file,
        )
    else:
        result = application.execute(
            path=args.plan_file, environment=args.environment, confirmation_digest=args.confirm_digest
        )
    write_json(result)
    return 0 if result["status"] in {"ready", "completed"} else (2 if result["status"] == "blocked" else 1)
