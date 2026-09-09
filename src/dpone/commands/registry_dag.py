from __future__ import annotations

import argparse

from .base import Command
from .dag import (
    explain_deps_cmd,
    explain_edge_cmd,
    explain_edge_e2e_cmd,
    explain_node_cmd,
    explain_node_e2e_cmd,
    list_edges_cmd,
    report_cmd,
    subgraph_cmd,
)
from .func_command import CommandGroup, FuncCommand


def dag_group() -> Command:
    sub = [
        FuncCommand("explain-edge", explain_edge_cmd.register_parser, explain_edge_cmd.cmd_dag_explain_edge),
        FuncCommand(
            "explain-edge-e2e", explain_edge_e2e_cmd.register_parser, explain_edge_e2e_cmd.cmd_dag_explain_edge_e2e
        ),
        FuncCommand("list-edges", list_edges_cmd.register_parser, list_edges_cmd.cmd_dag_list_edges),
        FuncCommand("explain-node", explain_node_cmd.register_parser, explain_node_cmd.cmd_dag_explain_node),
        FuncCommand(
            "explain-node-e2e", explain_node_e2e_cmd.register_parser, explain_node_e2e_cmd.cmd_dag_explain_node_e2e
        ),
        FuncCommand("subgraph", subgraph_cmd.register_parser, subgraph_cmd.cmd_dag_subgraph),
        FuncCommand("report", report_cmd.register_parser, report_cmd.cmd_dag_report),
        FuncCommand("explain-deps", explain_deps_cmd.register_parser, explain_deps_cmd.cmd_dag_explain_deps),
    ]

    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("dag", help="DAG utilities (dependencies, reasons, reports)")

    return CommandGroup(
        name="dag",
        help="DAG utilities",
        build_parser=build,
        subcommands=sub,
        subdest="dag_cmd",
    )
