from __future__ import annotations

import argparse
import logging
from importlib import import_module
from typing import Any, cast

from dpone.commands.gitops.common import GitOpsOutputContext, write_optional_output
from dpone.commands.output_json import dumps_json, write_json
from dpone.commands.output_text import write_text
from dpone.gitops.airflow_cluster_doctor_rendering import render_gitops_airflow_cluster_doctor_markdown

_RUNNER_POLICY_CHOICES = ("advisory", "pr", "release")


def register_cluster_doctor_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser(
        "cluster-doctor",
        help="Plan or run opt-in Kubernetes readiness checks for generated Airflow runtime artifacts",
    )
    p.add_argument("--artifact-dir", default=".dpone/gitops/airflow", help="Repo-relative Airflow artifact directory")
    p.add_argument("--runtime-profile-path", default=None, help="Repo-relative runtime-profile JSON path")
    p.add_argument("--pod-contract-path", default=None, help="Repo-relative pod-contract JSON path")
    p.add_argument("--connection-bridge-plan-path", default=None, help="Repo-relative connection bridge plan JSON path")
    p.add_argument("--mode", choices=("plan", "live"), default="plan", help="Plan kubectl checks or execute them")
    p.add_argument("--runner-policy", choices=_RUNNER_POLICY_CHOICES, default="advisory", help="Cluster doctor policy")
    p.add_argument("--timeout-seconds", type=int, default=120, help="Per-command live cluster doctor timeout")
    p.add_argument("--kubectl", default="kubectl", help="kubectl executable name or wrapper")
    p.add_argument("--external-secret", action="append", default=[], help="ExternalSecret name to check; repeatable")
    p.add_argument(
        "--require-external-secret-ready",
        action="store_true",
        help="Treat ExternalSecret readiness checks as release blockers",
    )
    p.add_argument("--output", help="Optional repo-relative report output path")
    p.add_argument("--format", choices=("json", "markdown"), default="json", help="Output format")
    return p


def cmd_gitops_airflow_cluster_doctor(
    args: argparse.Namespace,
    *,
    ctx: object,
    logger: logging.Logger,
    runner: Any | None = None,
) -> int:
    _ = logger
    view = _cluster_doctor_service(ctx, runner=runner).build_view(args)
    if getattr(args, "format", "json") == "markdown":
        rendered = render_gitops_airflow_cluster_doctor_markdown(view.report)
        write_optional_output(cast(GitOpsOutputContext, ctx), getattr(args, "output", None), rendered)
        write_text(rendered)
        return view.exit_code
    payload = view.to_jsonable()
    rendered = dumps_json(payload)
    write_optional_output(cast(GitOpsOutputContext, ctx), getattr(args, "output", None), rendered)
    write_json(payload)
    return view.exit_code


def _cluster_doctor_service(ctx: object, *, runner: Any | None) -> Any:
    module = import_module("dpone.services.gitops.airflow_cluster_doctor_service")
    service_type = getattr(module, "GitOpsAirflowClusterDoctorService")
    return service_type(ctx=ctx, runner=runner)


__all__ = ["cmd_gitops_airflow_cluster_doctor", "register_cluster_doctor_parser"]
