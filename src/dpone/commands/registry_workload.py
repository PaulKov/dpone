from __future__ import annotations

import argparse

from . import workload_discovery_cmd, workload_init_cmd
from .base import Command
from .func_command import CommandGroup, FuncCommand


def workload_group() -> Command:
    sub: list[Command] = [
        FuncCommand("init", workload_init_cmd.register_parser, workload_init_cmd.cmd_workload_init),
        FuncCommand(
            "index",
            workload_discovery_cmd.register_index_parser,
            workload_discovery_cmd.cmd_workload_index,
        ),
        FuncCommand(
            "impact",
            workload_discovery_cmd.register_impact_parser,
            workload_discovery_cmd.cmd_workload_impact,
        ),
        FuncCommand(
            "promote",
            workload_discovery_cmd.register_promote_parser,
            workload_discovery_cmd.cmd_workload_promote,
        ),
    ]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("workload", help="Advanced workload catalog and CI commands")

    return CommandGroup(
        name="workload",
        help="Advanced workload catalog and CI commands",
        build_parser=build,
        subcommands=sub,
        subdest="workload_cmd",
    )


__all__ = ["workload_group"]
