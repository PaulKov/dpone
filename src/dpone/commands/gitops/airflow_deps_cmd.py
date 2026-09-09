from __future__ import annotations

import argparse
import logging

from dpone.commands.output_json import write_json
from dpone.commands.output_text import write_text
from dpone.services.gitops.airflow_asset_deps_service import AirflowAssetDepsService


def cmd_gitops_airflow_deps(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx, logger
    report = AirflowAssetDepsService(repo_root=args.repo_root).build(
        workload_set=args.workload_set,
        env=args.environment,
    )
    if args.format == "json":
        write_json(
            {
                "passed": report.passed,
                "workload_set": report.workload_set,
                "env": report.env,
                "warnings": [warning.to_jsonable() for warning in report.warnings],
                "blockers": [blocker.to_jsonable() for blocker in report.dag_spec_report.blockers],
                "asset_edges": [
                    {
                        "uri": edge.uri,
                        "producer": edge.producer_node_id,
                        "consumer": edge.consumer_node_id,
                        "reason": edge.reason,
                    }
                    for edge in report.asset_graph.edges
                ],
                "markdown": report.markdown,
            }
        )
    else:
        write_text(report.markdown)
    return 0 if report.passed else 1


def register_deps_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser("deps", help="Render reviewable asset dependency graph for a workload-set")
    parser.add_argument("--workload-set", required=True, help="GitOps workload-set root, for example gitops.yaml")
    parser.add_argument("--environment", default="dev", help="GitOps environment name")
    parser.add_argument("--repo-root", default=".", help="Repository root")
    parser.add_argument("--format", choices=("md", "json"), default="md")
    return parser


__all__ = ["cmd_gitops_airflow_deps", "register_deps_parser"]
