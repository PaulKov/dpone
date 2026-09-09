from __future__ import annotations

import argparse

from dpone.commands import hooks_cmd
from dpone.commands.func_command import CommandGroup, FuncCommand


def hooks_group() -> CommandGroup:
    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("hooks", help="Execute manifest-defined pre_hook/post_hook actions")

    return CommandGroup(
        name="hooks",
        help="Manifest hook execution helpers",
        build_parser=build,
        subcommands=[FuncCommand("execute", hooks_cmd.register_execute_parser, hooks_cmd.cmd_hooks_execute)],
        subdest="hooks_cmd",
    )
