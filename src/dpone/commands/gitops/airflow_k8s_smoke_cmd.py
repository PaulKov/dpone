from __future__ import annotations

import argparse
import logging
from typing import Any, cast

from dpone.commands.gitops.common import GitOpsOutputContext, write_optional_output
from dpone.commands.output_json import dumps_json, write_json
from dpone.commands.output_text import write_text
from dpone.gitops.airflow_rendering import render_gitops_airflow_k8s_smoke_markdown
from dpone.services.gitops.airflow_k8s_smoke_service import (
    GitOpsAirflowK8sSmokeContext,
    GitOpsAirflowK8sSmokeService,
)

_RUNNER_POLICY_CHOICES = ("advisory", "pr", "release")


def register_k8s_smoke_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser("k8s-smoke", help="Plan or run an opt-in Airflow Kubernetes smoke contract")
    p.add_argument(
        "--run-spec-path",
        default=".dpone/gitops/airflow/run-spec.json",
        help="Repo-relative Airflow run-spec JSON path",
    )
    p.add_argument(
        "--runtime-profile-path",
        default=".dpone/gitops/airflow/runtime-profile.json",
        help="Repo-relative Airflow runtime profile JSON path",
    )
    p.add_argument(
        "--pod-contract-path",
        default=".dpone/gitops/airflow/pod-contract.json",
        help="Repo-relative Airflow pod contract JSON path",
    )
    p.add_argument(
        "--image-contract-path",
        default=".dpone/gitops/airflow/image-contract.json",
        help="Repo-relative custom dpone image contract JSON path",
    )
    p.add_argument(
        "--xcom-summary-path",
        default=".dpone/gitops/airflow/xcom-summary.json",
        help="Repo-relative final XCom summary JSON path",
    )
    p.add_argument("--mode", choices=["plan", "live"], default="plan", help="Plan commands or execute them")
    p.add_argument(
        "--runner-kind",
        choices=["kubernetes_pod_operator", "kubernetes_executor"],
        default="kubernetes_pod_operator",
        help="Airflow Kubernetes runner kind being certified",
    )
    p.add_argument(
        "--runner-policy",
        choices=_RUNNER_POLICY_CHOICES,
        default="advisory",
        help="Airflow Kubernetes smoke policy",
    )
    p.add_argument("--smoke-name", default="dpone-smoke", help="Stable Kubernetes smoke pod name prefix")
    p.add_argument("--timeout-seconds", type=int, default=300, help="Per-command live smoke timeout")
    p.add_argument("--kubectl", default="kubectl", help="kubectl executable name or wrapper")
    p.add_argument("--airflow-cmd", default="airflow", help="Airflow CLI executable name or wrapper")
    p.add_argument("--output", help="Optional repo-relative report output path")
    p.add_argument("--format", choices=["json", "markdown"], default="json", help="Output format")
    return p


def cmd_gitops_airflow_k8s_smoke(
    args: argparse.Namespace,
    *,
    ctx: object,
    logger: logging.Logger,
    runner: Any | None = None,
) -> int:
    _ = logger
    view = GitOpsAirflowK8sSmokeService(
        ctx=cast(GitOpsAirflowK8sSmokeContext, ctx),
        runner=runner,
    ).build_view(args)
    if getattr(args, "format", "json") == "markdown":
        rendered = render_gitops_airflow_k8s_smoke_markdown(view.report)
        write_optional_output(cast(GitOpsOutputContext, ctx), getattr(args, "output", None), rendered)
        write_text(rendered)
        return view.exit_code
    payload = view.to_jsonable()
    rendered = dumps_json(payload)
    write_optional_output(cast(GitOpsOutputContext, ctx), getattr(args, "output", None), rendered)
    write_json(payload)
    return view.exit_code


__all__ = ["cmd_gitops_airflow_k8s_smoke", "register_k8s_smoke_parser"]
