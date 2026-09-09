from __future__ import annotations

import argparse

from .base import Command
from .func_command import CommandGroup, FuncCommand
from .gitops import affected_cmd, airflow_cmd, bundle_cmd, gitlab_cmd, plan_cmd, schema_cmd, verify_cmd, workloads_cmd


def gitops_group() -> Command:
    sub: list[Command] = [
        FuncCommand("affected", affected_cmd.register_parser, affected_cmd.cmd_gitops_affected),
        airflow_cmd.airflow_group(),
        FuncCommand("bundle", bundle_cmd.register_parser, bundle_cmd.cmd_gitops_bundle),
        gitlab_cmd.gitlab_group(),
        FuncCommand("plan", plan_cmd.register_parser, plan_cmd.cmd_gitops_plan),
        schema_cmd.schema_group(),
        FuncCommand("verify", verify_cmd.register_parser, verify_cmd.cmd_gitops_verify),
        workloads_cmd.workloads_group(),
    ]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("gitops", help="GitOps runner planning and verification")

    return CommandGroup(
        name="gitops",
        help="GitOps utilities",
        build_parser=build,
        subcommands=sub,
        subdest="gitops_cmd",
    )


__all__ = ["gitops_group"]
