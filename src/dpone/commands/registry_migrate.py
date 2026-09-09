"""Top-level migration command registry."""

from __future__ import annotations

import argparse

from dpone.commands.base import Command
from dpone.commands.func_command import CommandGroup, FuncCommand
from dpone.commands.migrate_authoring_cmd import cmd_migrate_authoring, register_parser


def migrate_group() -> Command:
    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("migrate", help="Explicit source and contract migrations")

    return CommandGroup(
        name="migrate",
        help="Explicit source and contract migrations",
        build_parser=build,
        subcommands=[FuncCommand("authoring", register_parser, cmd_migrate_authoring, _requires_app_context=False)],
        subdest="migrate_cmd",
    )


__all__ = ["migrate_group"]
