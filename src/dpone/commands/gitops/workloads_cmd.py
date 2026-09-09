from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.services.gitops.views import GitOpsView


import argparse
import logging
from typing import cast

from dpone.commands.func_command import CommandGroup, FuncCommand
from dpone.commands.gitops.common import GitOpsOutputContext, write_optional_output
from dpone.commands.output_json import dumps_json, write_json
from dpone.commands.output_text import write_text
from dpone.services.gitops.workload_catalog_service import GitOpsWorkloadCatalogContext, GitOpsWorkloadCatalogService


def workloads_group() -> CommandGroup:
    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("workloads", help="Resolve GitOps workload catalogs")

    return CommandGroup(
        name="workloads",
        help="GitOps workload catalog utilities",
        build_parser=build,
        subcommands=[
            FuncCommand("list", register_list_parser, cmd_gitops_workloads_list),
            FuncCommand("explain", register_explain_parser, cmd_gitops_workloads_explain),
        ],
        subdest="gitops_workloads_cmd",
    )


def register_list_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser("list", help="List resolved GitOps workloads")
    _add_catalog_args(p)
    p.add_argument("--output", help="Optional repo-relative output path")
    p.add_argument("--format", choices=("json", "markdown"), default="json")
    return p


def register_explain_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser("explain", help="Explain effective config for one workload")
    p.add_argument("workload_id")
    _add_catalog_args(p)
    p.add_argument("--output", help="Optional repo-relative output path")
    p.add_argument("--format", choices=("json", "markdown"), default="json")
    return p


def cmd_gitops_workloads_list(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    _ = logger
    view = _service(ctx).list_view(args)
    return _write_view(view, args=args, ctx=ctx)


def cmd_gitops_workloads_explain(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    _ = logger
    view = _service(ctx).explain_view(args)
    return _write_view(view, args=args, ctx=ctx)


def _add_catalog_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--workload-set", required=True, help="Repo-relative GitOps workload-set YAML")
    parser.add_argument("--env", default="dev", help="Environment scope")


def _service(ctx: object) -> GitOpsWorkloadCatalogService:
    return GitOpsWorkloadCatalogService(ctx=cast(GitOpsWorkloadCatalogContext, ctx))


def _write_view(view: GitOpsView, *, args: argparse.Namespace, ctx: object) -> int:
    payload = view.to_jsonable()
    if getattr(args, "format", "json") == "markdown":
        rendered = "\n".join([f"# {payload['kind']}", "", dumps_json(payload)])
        write_optional_output(cast(GitOpsOutputContext, ctx), getattr(args, "output", None), rendered)
        write_text(rendered)
        return view.exit_code
    rendered = dumps_json(payload)
    write_optional_output(cast(GitOpsOutputContext, ctx), getattr(args, "output", None), rendered)
    write_json(payload)
    return view.exit_code


__all__ = [
    "cmd_gitops_workloads_explain",
    "cmd_gitops_workloads_list",
    "register_explain_parser",
    "register_list_parser",
    "workloads_group",
]
