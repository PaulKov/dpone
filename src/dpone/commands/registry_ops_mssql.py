"""MSSQL operational command groups."""

from __future__ import annotations

from . import mssql_native_recovery_cmd
from .func_command import CommandGroup, FuncCommand


def mssql_native_recovery_group() -> CommandGroup:
    """Build the source-free native recovery command group."""

    return CommandGroup(
        name="mssql-native-recovery",
        help="Inspect and reconcile durable MSSQL native delivery state",
        build_parser=lambda subparsers: subparsers.add_parser(
            "mssql-native-recovery", help="Inspect and reconcile durable MSSQL native delivery state"
        ),
        subcommands=[
            FuncCommand(
                "inspect",
                mssql_native_recovery_cmd.register_inspect_parser,
                mssql_native_recovery_cmd.cmd_inspect,
                _requires_app_context=False,
            ),
            FuncCommand(
                "inspect-all",
                mssql_native_recovery_cmd.register_inspect_all_parser,
                mssql_native_recovery_cmd.cmd_inspect_all,
                _requires_app_context=False,
            ),
            FuncCommand(
                "reconcile",
                mssql_native_recovery_cmd.register_reconcile_parser,
                mssql_native_recovery_cmd.cmd_reconcile,
                _requires_app_context=False,
            ),
            FuncCommand(
                "resume",
                mssql_native_recovery_cmd.register_resume_parser,
                mssql_native_recovery_cmd.cmd_resume,
                _requires_app_context=False,
            ),
            FuncCommand(
                "retire",
                mssql_native_recovery_cmd.register_retire_parser,
                mssql_native_recovery_cmd.cmd_retire,
                _requires_app_context=False,
            ),
        ],
        subdest="mssql_native_recovery_cmd",
    )


__all__ = ["mssql_native_recovery_group"]
