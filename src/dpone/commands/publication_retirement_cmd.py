"""Thin guarded-retirement commands; standalone CLI grants no admission."""

from __future__ import annotations

import argparse
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING

from dpone.commands.func_command import CommandGroup, FuncCommand
from dpone.commands.output_json import write_json

if TYPE_CHECKING:
    import logging


def publication_retirement_command() -> CommandGroup:
    return CommandGroup(
        name="retire",
        help="Explicit retirement of a proven unpublished legacy operation",
        build_parser=lambda sub: sub.add_parser("retire", help="Plan, apply or verify in a trusted runner"),
        subcommands=[
            FuncCommand(action, partial(_register, action=action), _execute, _requires_app_context=False)
            for action in ("plan", "apply", "verify")
        ],
        subdest="publication_retirement_action",
    )


def _register(sub: argparse._SubParsersAction, *, action: str) -> argparse.ArgumentParser:
    parser = sub.add_parser(action)
    parser.add_argument("--environment", required=True, help="Exact verified runtime environment")
    parser.add_argument("--plan-file", type=Path, required=True, help="Owner-private plan; refuses overwrite")
    if action == "plan":
        parser.add_argument("--connection-ref", required=True, help="Admitted MSSQL metadata binding")
    else:
        parser.add_argument("--confirm-digest", required=True, help="Reviewed outer operator plan SHA-256")
    return parser


def _execute(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    from dpone.app.publication_retirement_application import PublicationRetirementApplication

    application = PublicationRetirementApplication()
    if args.publication_retirement_action == "plan":
        result = application.plan(connection_ref=args.connection_ref, environment=args.environment, path=args.plan_file)
    else:
        action = application.verify if args.publication_retirement_action == "verify" else application.apply
        result = action(path=args.plan_file, environment=args.environment, confirmation_digest=args.confirm_digest)
    write_json(result)
    return 0 if result["status"] in {"ready", "retired_unpublished"} else (2 if result["status"] == "blocked" else 1)
