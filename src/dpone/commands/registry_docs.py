from __future__ import annotations

import argparse

from .base import Command
from .func_command import CommandGroup
from .registry_docs_checks import docs_check_commands
from .registry_docs_updates import docs_update_commands


def docs_group() -> Command:
    sub = [
        *docs_update_commands(),
        *docs_check_commands(),
    ]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("docs", help="Documentation helpers")

    return CommandGroup(
        name="docs",
        help="Docs",
        build_parser=build,
        subcommands=sub,
        subdest="docs_cmd",
    )
