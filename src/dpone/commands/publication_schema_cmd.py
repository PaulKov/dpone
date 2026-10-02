"""Thin source-free schema commands; SQL and context admission stay in services."""

from __future__ import annotations

import argparse
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING

from dpone.commands.func_command import CommandGroup, FuncCommand
from dpone.commands.output_json import write_json
from dpone.commands.prepared_recovery_cmd import prepared_recovery_command
from dpone.commands.publication_retirement_cmd import publication_retirement_command

if TYPE_CHECKING:
    import logging


def publication_authority_command() -> CommandGroup:
    schema = CommandGroup(
        name="schema",
        help="Explicit publication catalog setup",
        build_parser=lambda sub: sub.add_parser("schema", help="Plan, apply or inspect the publication catalog"),
        subcommands=[
            FuncCommand(action, partial(_register, action=action), _execute, _requires_app_context=False)
            for action in ("plan", "apply", "inspect")
        ],
        subdest="publication_schema_action",
    )
    return CommandGroup(
        name="publication-authority",
        help="Explicit publication authority administration",
        build_parser=lambda sub: sub.add_parser(
            "publication-authority", help="Explicit publication catalog operations"
        ),
        subcommands=[schema, prepared_recovery_command(), publication_retirement_command()],
        subdest="publication_authority_action",
    )


def _register(sub: argparse._SubParsersAction, *, action: str) -> argparse.ArgumentParser:
    parser = sub.add_parser(action)
    parser.add_argument("--environment", required=True, help="Exact environment from the verified runtime context")
    parser.add_argument(
        "--plan-file", required=True, type=Path, help="Private local POSIX plan; plan refuses overwrite"
    )
    if action == "plan":
        parser.add_argument(
            "--connection-ref", required=True, help="Logical MSSQL metadata binding in verified context"
        )
    if action == "apply":
        parser.add_argument("--confirm-digest", required=True, help="Exact SHA-256 from the reviewed plan")
    return parser


def _execute(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    from dpone.app.publication_schema_application import PublicationSchemaApplication

    application = PublicationSchemaApplication()
    if args.publication_schema_action == "plan":
        result = application.plan(connection_ref=args.connection_ref, environment=args.environment, path=args.plan_file)
    elif args.publication_schema_action == "apply":
        result = application.apply(
            path=args.plan_file, environment=args.environment, confirmation_digest=args.confirm_digest
        )
    else:
        result = application.inspect(path=args.plan_file, environment=args.environment)
    write_json(result)
    if result["status"] in {"ready", "completed"}:
        return 0
    return 2 if result["status"] == "blocked" else 1
