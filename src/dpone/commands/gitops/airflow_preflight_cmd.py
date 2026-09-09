from __future__ import annotations

import argparse
import logging
from importlib import import_module
from typing import Any, cast

from dpone.commands.gitops.common import GitOpsOutputContext, write_optional_output
from dpone.commands.output_json import dumps_json, write_json
from dpone.commands.output_text import write_text
from dpone.gitops.airflow_rendering import (
    render_gitops_airflow_artifact_index_markdown,
    render_gitops_airflow_preflight_markdown,
)

_RUNNER_POLICY_CHOICES = ("advisory", "pr", "release")


def register_artifact_index_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser("artifact-index", help="Index generated Airflow runtime artifacts")
    p.add_argument(
        "--artifact-dir",
        default=".dpone/gitops/airflow",
        help="Repo-relative Airflow artifact directory",
    )
    p.add_argument(
        "--output-path",
        default=None,
        help="Repo-relative artifact-index JSON path; defaults to <artifact-dir>/artifact-index.json",
    )
    p.add_argument("--output", help="Optional repo-relative report output path")
    p.add_argument("--format", choices=["json", "markdown"], default="json", help="Output format")
    return p


def register_preflight_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser("preflight", help="Validate an Airflow runtime artifact directory")
    p.add_argument(
        "--artifact-dir",
        default=".dpone/gitops/airflow",
        help="Repo-relative Airflow artifact directory",
    )
    p.add_argument(
        "--artifact-index-path",
        default=None,
        help="Repo-relative artifact-index JSON path used for freshness checks; defaults to <artifact-dir>/artifact-index.json",
    )
    p.add_argument(
        "--runner-policy",
        choices=_RUNNER_POLICY_CHOICES,
        default="advisory",
        help="Airflow preflight policy profile",
    )
    p.add_argument("--output", help="Optional repo-relative report output path")
    p.add_argument("--format", choices=["json", "markdown"], default="json", help="Output format")
    return p


def cmd_gitops_airflow_artifact_index(
    args: argparse.Namespace,
    *,
    ctx: object,
    logger: logging.Logger,
) -> int:
    _ = logger
    view = _artifact_index_service(ctx).build_view(args)
    if getattr(args, "format", "json") == "markdown":
        rendered = render_gitops_airflow_artifact_index_markdown(view.report)
        write_optional_output(cast(GitOpsOutputContext, ctx), getattr(args, "output", None), rendered)
        write_text(rendered)
        return view.exit_code
    payload = view.to_jsonable()
    rendered = dumps_json(payload)
    write_optional_output(cast(GitOpsOutputContext, ctx), getattr(args, "output", None), rendered)
    write_json(payload)
    return view.exit_code


def cmd_gitops_airflow_preflight(
    args: argparse.Namespace,
    *,
    ctx: object,
    logger: logging.Logger,
) -> int:
    _ = logger
    view = _preflight_service(ctx).build_view(args)
    if getattr(args, "format", "json") == "markdown":
        rendered = render_gitops_airflow_preflight_markdown(view.report)
        write_optional_output(cast(GitOpsOutputContext, ctx), getattr(args, "output", None), rendered)
        write_text(rendered)
        return view.exit_code
    payload = view.to_jsonable()
    rendered = dumps_json(payload)
    write_optional_output(cast(GitOpsOutputContext, ctx), getattr(args, "output", None), rendered)
    write_json(payload)
    return view.exit_code


def _artifact_index_service(ctx: object) -> Any:
    module = import_module("dpone.services.gitops.airflow_artifact_index_service")
    service_type = getattr(module, "GitOpsAirflowArtifactIndexService")
    return service_type(ctx=ctx)


def _preflight_service(ctx: object) -> Any:
    module = import_module("dpone.services.gitops.airflow_preflight_service")
    service_type = getattr(module, "GitOpsAirflowPreflightService")
    return service_type(ctx=ctx)


__all__ = [
    "cmd_gitops_airflow_artifact_index",
    "cmd_gitops_airflow_preflight",
    "register_artifact_index_parser",
    "register_preflight_parser",
]
