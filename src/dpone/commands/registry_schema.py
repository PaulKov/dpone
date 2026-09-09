from __future__ import annotations

import argparse
from importlib import import_module

from . import (
    cdc_plan_cmd,
    cdc_replay_cmd,
    schema_identity_cmd,
    schema_impact_cmd,
    schema_migration_cmd,
    schema_plan_cmd,
)
from .base import Command
from .func_command import CommandGroup, FuncCommand


def cdc_plan_top_level() -> Command:
    return FuncCommand("cdc-plan", cdc_plan_cmd.register_parser, cdc_plan_cmd.cmd_cdc_plan)


def schema_group() -> Command:
    sub = [
        FuncCommand("plan", schema_plan_cmd.register_plan_parser, schema_plan_cmd.cmd_schema_plan),
        FuncCommand("explain", schema_plan_cmd.register_explain_parser, schema_plan_cmd.cmd_schema_explain),
        FuncCommand("approve", schema_plan_cmd.register_approve_parser, schema_plan_cmd.cmd_schema_approve),
        FuncCommand("infer", schema_plan_cmd.register_infer_parser, schema_plan_cmd.cmd_schema_infer),
        FuncCommand(
            "physical-plan", schema_plan_cmd.register_physical_plan_parser, schema_plan_cmd.cmd_schema_physical_plan
        ),
        FuncCommand(
            "physical-diff", schema_plan_cmd.register_physical_diff_parser, schema_plan_cmd.cmd_schema_physical_diff
        ),
        FuncCommand("type-matrix", schema_plan_cmd.register_type_matrix_parser, schema_plan_cmd.cmd_schema_type_matrix),
        FuncCommand(
            "expand-contract",
            schema_plan_cmd.register_expand_contract_parser,
            schema_plan_cmd.cmd_schema_expand_contract,
        ),
        _schema_contract_group(),
        schema_identity_cmd.identity_group(),
        schema_impact_cmd.impact_group(),
        schema_migration_cmd.migration_group(),
    ]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("schema", help="Schema evolution utilities")

    return CommandGroup(
        name="schema",
        help="Schema evolution utilities",
        build_parser=build,
        subcommands=sub,
        subdest="schema_cmd",
    )


def _schema_contract_group() -> Command:
    return import_module("dpone.commands.schema_contract_cmd").contract_group()


def cdc_group() -> Command:
    sub = [
        FuncCommand("plan", cdc_plan_cmd.register_plan_parser, cdc_plan_cmd.cmd_cdc_plan),
        FuncCommand("replay-plan", cdc_replay_cmd.register_replay_plan_parser, cdc_replay_cmd.cmd_cdc_replay_plan),
    ]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("cdc", help="CDC utilities")

    return CommandGroup(
        name="cdc",
        help="CDC utilities",
        build_parser=build,
        subcommands=sub,
        subdest="cdc_cmd",
    )
