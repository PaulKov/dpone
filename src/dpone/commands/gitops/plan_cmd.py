from __future__ import annotations

import argparse
import logging
from typing import cast

from dpone.commands.gitops.common import GitOpsOutputContext, write_optional_output
from dpone.commands.output_json import dumps_json, write_json
from dpone.commands.output_text import write_text
from dpone.gitops.rendering import render_gitops_plan_markdown
from dpone.services.gitops.plan_service import GitOpsPlanContext, GitOpsPlanService


def register_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser("plan", help="Build a GitOps runner contract for one manifest")
    p.add_argument("path", help="Path to a YAML manifest")
    p.add_argument("--workload-root", help="Workload root that bounds sparse path validation")
    p.add_argument("--include-global-overrides", action="store_true", help="Include overrides/global.yaml")
    p.add_argument(
        "--include-env-overrides", action="append", default=[], metavar="ENV", help="Include overrides/<env>.yaml"
    )
    p.add_argument("--include-registry", action="store_true", help="Include the workload registry/ directory")
    p.add_argument("--registry", action="append", default=[], help="Include an explicit registry path")
    p.add_argument("--support-path", action="append", default=[], help="Include an extra support file or directory")
    p.add_argument(
        "--runner",
        choices=["generic", "airflow", "kubernetes_pod_operator", "github_actions", "dagster"],
        default="generic",
        help="Runner family consuming the plan",
    )
    p.add_argument("--run-command", help="Command the runner should execute after checkout")
    p.add_argument("--output", help="Optional repo-relative output artifact path")
    p.add_argument("--format", choices=["json", "markdown"], default="json", help="Output format")
    return p


def cmd_gitops_plan(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    _ = logger
    view = GitOpsPlanService(ctx=cast(GitOpsPlanContext, ctx)).build_view(args)
    if getattr(args, "format", "json") == "markdown":
        rendered = render_gitops_plan_markdown(view.report)
        write_optional_output(cast(GitOpsOutputContext, ctx), getattr(args, "output", None), rendered)
        write_text(rendered)
        return view.exit_code
    payload = view.to_jsonable()
    rendered = dumps_json(payload)
    write_optional_output(cast(GitOpsOutputContext, ctx), getattr(args, "output", None), rendered)
    write_json(payload)
    return view.exit_code


__all__ = ["cmd_gitops_plan", "register_parser"]
