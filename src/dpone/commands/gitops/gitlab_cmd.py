from __future__ import annotations

import argparse
import logging
from typing import cast

from dpone.commands.func_command import CommandGroup, FuncCommand
from dpone.commands.output_text import write_text
from dpone.services.gitops.gitlab_child_pipeline_service import (
    GitOpsGitLabChildPipelineContext,
    GitOpsGitLabChildPipelineService,
)


def gitlab_group() -> CommandGroup:
    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser("gitlab", help="Render GitLab CI helpers for GitOps workloads")

    return CommandGroup(
        name="gitlab",
        help="GitLab GitOps helpers",
        build_parser=build,
        subcommands=[
            FuncCommand(
                "render-child-pipeline", register_render_child_pipeline_parser, cmd_gitops_gitlab_render_child_pipeline
            )
        ],
        subdest="gitops_gitlab_cmd",
    )


def register_render_child_pipeline_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser("render-child-pipeline", help="Render a dynamic GitLab child pipeline")
    p.add_argument("--workload-set", required=True, help="Repo-relative GitOps workload-set YAML")
    p.add_argument("--changed-files", nargs="+", default=[], help="Repo-relative changed files")
    p.add_argument("--changed-files-file", help="Repo-relative newline-delimited changed-files list")
    p.add_argument("--env", default="dev", help="Environment scope")
    p.add_argument("--output", help="Repo-relative child pipeline YAML path")
    p.add_argument("--format", choices=("yaml",), default="yaml")
    return p


def cmd_gitops_gitlab_render_child_pipeline(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    _ = logger
    rendered = GitOpsGitLabChildPipelineService(ctx=cast(GitOpsGitLabChildPipelineContext, ctx)).render(args)
    write_text(rendered)
    return 0


__all__ = ["cmd_gitops_gitlab_render_child_pipeline", "gitlab_group", "register_render_child_pipeline_parser"]
