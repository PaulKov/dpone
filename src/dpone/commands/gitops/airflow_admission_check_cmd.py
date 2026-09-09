from __future__ import annotations

import argparse
import logging
from importlib import import_module
from typing import Any, cast

from dpone.commands.gitops.common import GitOpsOutputContext, write_optional_output
from dpone.commands.output_json import dumps_json, write_json

_RUNNER_POLICY_CHOICES = ("advisory", "pr", "release")


def register_admission_check_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser(
        "admission-check",
        help="Plan or run opt-in Kubernetes server-side dry-run checks for Airflow artifacts",
    )
    p.add_argument("--artifact-dir", default=".dpone/gitops/airflow", help="Repo-relative Airflow artifact directory")
    p.add_argument(
        "--manifest-path",
        default=".dpone/gitops/airflow/airflow-k8s-manifests.yaml",
        help="Repo-relative Kubernetes manifest YAML path",
    )
    p.add_argument(
        "--pod-spec-path",
        default=".dpone/gitops/airflow/pod-spec.yaml",
        help="Repo-relative pod-spec YAML path",
    )
    p.add_argument("--mode", choices=("plan", "live"), default="plan", help="Plan commands or execute kubectl")
    p.add_argument("--runner-policy", choices=_RUNNER_POLICY_CHOICES, default="advisory", help="Admission policy")
    p.add_argument("--timeout-seconds", type=int, default=120, help="Per-command live admission timeout")
    p.add_argument("--kubectl", default="kubectl", help="kubectl executable name or wrapper")
    p.add_argument("--output", help="Optional repo-relative JSON report output path")
    p.add_argument("--format", choices=("json",), default="json", help="Output format")
    return p


def cmd_gitops_airflow_admission_check(
    args: argparse.Namespace,
    *,
    ctx: object,
    logger: logging.Logger,
    runner: Any | None = None,
) -> int:
    _ = logger
    view = _admission_check_service(ctx, runner=runner).build_view(args)
    payload = view.to_jsonable()
    rendered = dumps_json(payload)
    write_optional_output(cast(GitOpsOutputContext, ctx), getattr(args, "output", None), rendered)
    write_json(payload)
    return view.exit_code


def _admission_check_service(ctx: object, *, runner: Any | None) -> Any:
    module = import_module("dpone.services.gitops.airflow_admission_check_service")
    service_type = getattr(module, "GitOpsAirflowAdmissionCheckService")
    return service_type(ctx=ctx, runner=runner)


__all__ = ["cmd_gitops_airflow_admission_check", "register_admission_check_parser"]
