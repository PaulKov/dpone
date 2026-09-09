from __future__ import annotations

import argparse
import logging
from importlib import import_module
from typing import Any, cast

from dpone.commands.gitops.common import GitOpsOutputContext, write_optional_output
from dpone.commands.output_json import dumps_json, write_json
from dpone.commands.output_text import write_text
from dpone.gitops.airflow_rendering import render_gitops_airflow_evidence_bundle_markdown

_RUNNER_POLICY_CHOICES = ("advisory", "pr", "release")


def register_evidence_bundle_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser(
        "evidence-bundle",
        help="Collect one correlated Airflow attempt evidence bundle",
    )
    p.add_argument("--bundle-path", default=".dpone/gitops/bundle/bundle.json", help="Repo-relative bundle JSON path")
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
        "--runtime-evidence-path",
        default=".dpone/gitops/airflow/runtime-evidence.json",
        help="Repo-relative runtime evidence JSON path",
    )
    p.add_argument(
        "--xcom-summary-path",
        default=".dpone/gitops/airflow/xcom-summary.json",
        help="Repo-relative final XCom summary JSON path",
    )
    p.add_argument(
        "--k8s-smoke-path",
        default=".dpone/gitops/airflow/airflow-k8s-smoke.json",
        help="Repo-relative optional Airflow Kubernetes smoke JSON path",
    )
    p.add_argument(
        "--pod-launch-evidence-path",
        default=".dpone/gitops/airflow/airflow-pod-launch-evidence.json",
        help="Repo-relative optional pod-watch evidence JSON path",
    )
    p.add_argument("--dag-id", required=True, help="Airflow DAG id for this task attempt")
    p.add_argument("--task-id", required=True, help="Airflow task id for this task attempt")
    p.add_argument("--run-id", required=True, help="Airflow run id for this task attempt")
    p.add_argument("--try-number", type=int, required=True, help="Airflow try number")
    p.add_argument("--map-index", type=int, default=-1, help="Airflow mapped-task index, or -1 when unmapped")
    p.add_argument("--pod-name", help="Observed Kubernetes pod name override")
    p.add_argument("--pod-uid", help="Observed Kubernetes pod UID override")
    p.add_argument(
        "--runner-policy",
        choices=_RUNNER_POLICY_CHOICES,
        default="advisory",
        help="Airflow evidence bundle policy",
    )
    p.add_argument("--require-k8s-smoke", action="store_true", help="Block when k8s-smoke evidence is missing")
    p.add_argument(
        "--require-pod-launch-evidence",
        action="store_true",
        help="Block when pod-watch evidence is missing",
    )
    p.add_argument("--output", help="Optional repo-relative report output path")
    p.add_argument("--format", choices=["json", "markdown"], default="json", help="Output format")
    return p


def cmd_gitops_airflow_evidence_bundle(
    args: argparse.Namespace,
    *,
    ctx: object,
    logger: logging.Logger,
) -> int:
    _ = logger
    view = _build_service(ctx).build_view(args)
    if getattr(args, "format", "json") == "markdown":
        rendered = render_gitops_airflow_evidence_bundle_markdown(view.report)
        write_optional_output(cast(GitOpsOutputContext, ctx), getattr(args, "output", None), rendered)
        write_text(rendered)
        return view.exit_code
    payload = view.to_jsonable()
    rendered = dumps_json(payload)
    write_optional_output(cast(GitOpsOutputContext, ctx), getattr(args, "output", None), rendered)
    write_json(payload)
    return view.exit_code


def _build_service(ctx: object) -> Any:
    module = import_module("dpone.services.gitops.airflow_evidence_bundle_service")
    service_type = getattr(module, "GitOpsAirflowEvidenceBundleService")
    return service_type(ctx=ctx)


__all__ = ["cmd_gitops_airflow_evidence_bundle", "register_evidence_bundle_parser"]
