from __future__ import annotations

import argparse
import logging
from typing import Any, cast

from dpone.commands.gitops.common import GitOpsOutputContext, write_optional_output
from dpone.commands.output_json import dumps_json, write_json
from dpone.commands.output_text import write_text
from dpone.gitops.airflow_rendering import render_gitops_airflow_pod_launch_evidence_markdown
from dpone.services.gitops.airflow_pod_launch_evidence_service import (
    GitOpsAirflowPodLaunchEvidenceContext,
    GitOpsAirflowPodLaunchEvidenceService,
)

_RUNNER_POLICY_CHOICES = ("advisory", "pr", "release")
_EXPECTED_PHASE_CHOICES = ("Running", "Succeeded", "Failed", "Any")


def register_pod_watch_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser(
        "pod-watch",
        help="Plan or collect Airflow Kubernetes pod launch evidence",
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
        "--runtime-evidence-path",
        default=".dpone/gitops/airflow/runtime-evidence.json",
        help="Repo-relative runtime evidence JSON path",
    )
    p.add_argument(
        "--xcom-summary-path",
        default=".dpone/gitops/airflow/xcom-summary.json",
        help="Repo-relative final XCom summary JSON path",
    )
    p.add_argument("--pod-name", help="Kubernetes pod name; inferred from the pod contract when omitted")
    p.add_argument("--mode", choices=["plan", "live"], default="plan", help="Plan watch commands or execute them")
    p.add_argument(
        "--runner-policy",
        choices=_RUNNER_POLICY_CHOICES,
        default="advisory",
        help="Airflow pod launch evidence policy",
    )
    p.add_argument(
        "--expected-phase",
        choices=_EXPECTED_PHASE_CHOICES,
        default="Succeeded",
        help="Expected observed Kubernetes pod phase",
    )
    p.add_argument("--timeout-seconds", type=int, default=300, help="Per-command live watch timeout")
    p.add_argument("--log-tail-lines", type=int, default=200, help="Base container log tail line count")
    p.add_argument("--kubectl", default="kubectl", help="kubectl executable name or wrapper")
    p.add_argument("--output", help="Optional repo-relative report output path")
    p.add_argument("--format", choices=["json", "markdown"], default="json", help="Output format")
    return p


def cmd_gitops_airflow_pod_watch(
    args: argparse.Namespace,
    *,
    ctx: object,
    logger: logging.Logger,
    runner: Any | None = None,
) -> int:
    _ = logger
    view = GitOpsAirflowPodLaunchEvidenceService(
        ctx=cast(GitOpsAirflowPodLaunchEvidenceContext, ctx),
        runner=runner,
    ).build_view(args)
    if getattr(args, "format", "json") == "markdown":
        rendered = render_gitops_airflow_pod_launch_evidence_markdown(view.report)
        write_optional_output(cast(GitOpsOutputContext, ctx), getattr(args, "output", None), rendered)
        write_text(rendered)
        return view.exit_code
    payload = view.to_jsonable()
    rendered = dumps_json(payload)
    write_optional_output(cast(GitOpsOutputContext, ctx), getattr(args, "output", None), rendered)
    write_json(payload)
    return view.exit_code


__all__ = ["cmd_gitops_airflow_pod_watch", "register_pod_watch_parser"]
