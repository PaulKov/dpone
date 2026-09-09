from __future__ import annotations

import argparse

from .base import Command
from .func_command import CommandGroup, FuncCommand
from .manifest import (
    explain_cmd,
    lint_registry_cmd,
    list_cmd,
    migrate_cmd,
    render_cmd,
    sparse_paths_cmd,
    stats_cmd,
    validate_cmd,
    verify_cmd,
)


def manifest_group() -> Command:
    sub = [
        FuncCommand("list", list_cmd.register_parser, list_cmd.cmd_manifest_list),
        FuncCommand("render", render_cmd.register_parser, render_cmd.cmd_manifest_render),
        FuncCommand("explain", explain_cmd.register_parser, explain_cmd.cmd_manifest_explain),
        FuncCommand("sparse-paths", sparse_paths_cmd.register_parser, sparse_paths_cmd.cmd_manifest_sparse_paths),
        FuncCommand("validate", validate_cmd.register_parser, validate_cmd.cmd_manifest_validate),
        FuncCommand("lint-registry", lint_registry_cmd.register_parser, lint_registry_cmd.cmd_manifest_lint_registry),
        FuncCommand("stats", stats_cmd.register_parser, stats_cmd.cmd_manifest_stats),
        FuncCommand("migrate", migrate_cmd.register_parser, migrate_cmd.cmd_manifest_migrate),
        FuncCommand("verify", verify_cmd.register_parser, verify_cmd.cmd_manifest_verify),
    ]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("manifest", help="Manifest utilities (Variant C aware)")

    return CommandGroup(
        name="manifest",
        help="Manifest utilities",
        build_parser=build,
        subcommands=sub,
        subdest="manifest_cmd",
    )
