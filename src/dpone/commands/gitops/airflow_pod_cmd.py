from __future__ import annotations

import argparse
import logging
from typing import cast

from dpone.commands.gitops.common import GitOpsOutputContext, write_optional_output
from dpone.commands.output_json import dumps_json, write_json
from dpone.commands.output_text import write_text
from dpone.gitops.airflow_outcome_gate import airflow_outcome_mode_names
from dpone.gitops.airflow_rendering import (
    render_gitops_airflow_pod_contract_markdown,
    render_gitops_airflow_pod_doctor_markdown,
)
from dpone.services.gitops.airflow_pod_contract_service import (
    GitOpsAirflowPodContractContext,
    GitOpsAirflowPodContractService,
)
from dpone.services.gitops.airflow_pod_doctor_service import (
    GitOpsAirflowPodDoctorContext,
    GitOpsAirflowPodDoctorService,
)

_RUNNER_POLICY_CHOICES = ("advisory", "pr", "release")
_OUTCOME_MODE_CHOICES = airflow_outcome_mode_names()


def register_pod_contract_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser(
        "pod-contract",
        help="Build Airflow pod spec and KubernetesPodOperator kwargs from runtime contracts",
    )
    p.add_argument("bundle_path", help="Repo-relative gitops.bundle JSON path")
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
        "--output-path",
        default=".dpone/gitops/airflow/pod-contract.json",
        help="Repo-relative pod contract JSON path",
    )
    p.add_argument(
        "--pod-spec-path",
        default=".dpone/gitops/airflow/pod-spec.yaml",
        help="Repo-relative Kubernetes pod spec YAML path",
    )
    p.add_argument(
        "--kpo-kwargs-path",
        default=".dpone/gitops/airflow/kpo-kwargs.json",
        help="Repo-relative KubernetesPodOperator kwargs JSON path",
    )
    p.add_argument(
        "--image-pull-secret", action="append", default=[], help="Kubernetes imagePullSecret name; repeatable"
    )
    p.add_argument("--volume", action="append", default=[], help="Volume NAME=REPO_PATH; repeatable")
    p.add_argument(
        "--volume-mount",
        action="append",
        default=[],
        help="Container mount NAME=/absolute/path[:ro|rw]; repeatable",
    )
    p.add_argument("--env-from-configmap", action="append", default=[], help="ConfigMap envFrom name; repeatable")
    p.add_argument("--env-secret", action="append", default=[], help="Secret env NAME=secret:key; repeatable")
    p.add_argument("--node-selector", action="append", default=[], help="Node selector KEY=value; repeatable")
    p.add_argument("--toleration", action="append", default=[], help="Toleration KEY=value:Effect; repeatable")
    p.add_argument("--label", action="append", default=[], help="Kubernetes label KEY=value; repeatable")
    p.add_argument("--annotation", action="append", default=[], help="Kubernetes annotation KEY=value; repeatable")
    p.add_argument("--on-finish-action", default="delete_pod", help="KubernetesPodOperator on_finish_action")
    p.add_argument("--get-logs", action=argparse.BooleanOptionalAction, default=True, help="Enable KPO log streaming")
    p.add_argument("--deferrable", action=argparse.BooleanOptionalAction, default=False, help="Set KPO deferrable mode")
    p.add_argument(
        "--outcome-mode",
        choices=_OUTCOME_MODE_CHOICES,
        default=None,
        help="Pod exit strategy for final XCom outcomes",
    )
    p.add_argument("--output", help="Optional repo-relative report output path")
    p.add_argument("--format", choices=["json", "markdown"], default="json", help="Output format")
    return p


def register_pod_doctor_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser("pod-doctor", help="Validate generated Airflow pod contract artifacts offline")
    p.add_argument(
        "--artifact-dir",
        default=".dpone/gitops/airflow",
        help="Repo-relative Airflow artifact directory with pod-contract.json, pod-spec.yaml, and kpo-kwargs.json",
    )
    p.add_argument(
        "--pod-contract-path",
        default=None,
        help="Repo-relative pod contract JSON path",
    )
    p.add_argument(
        "--pod-spec-path",
        default=None,
        help="Repo-relative Kubernetes pod spec YAML path",
    )
    p.add_argument(
        "--kpo-kwargs-path",
        default=None,
        help="Repo-relative KubernetesPodOperator kwargs JSON path",
    )
    p.add_argument(
        "--runner-policy",
        choices=_RUNNER_POLICY_CHOICES,
        default="advisory",
        help="Airflow pod contract policy profile",
    )
    p.add_argument("--output", help="Optional repo-relative report output path")
    p.add_argument("--format", choices=["json", "markdown"], default="json", help="Output format")
    return p


def cmd_gitops_airflow_pod_contract(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    _ = logger
    view = GitOpsAirflowPodContractService(ctx=cast(GitOpsAirflowPodContractContext, ctx)).build_view(args)
    if getattr(args, "format", "json") == "markdown":
        rendered = render_gitops_airflow_pod_contract_markdown(view.report)
        write_optional_output(cast(GitOpsOutputContext, ctx), getattr(args, "output", None), rendered)
        write_text(rendered)
        return view.exit_code
    payload = view.to_jsonable()
    rendered = dumps_json(payload)
    write_optional_output(cast(GitOpsOutputContext, ctx), getattr(args, "output", None), rendered)
    write_json(payload)
    return view.exit_code


def cmd_gitops_airflow_pod_doctor(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    _ = logger
    view = GitOpsAirflowPodDoctorService(ctx=cast(GitOpsAirflowPodDoctorContext, ctx)).build_view(args)
    if getattr(args, "format", "json") == "markdown":
        rendered = render_gitops_airflow_pod_doctor_markdown(view.report)
        write_optional_output(cast(GitOpsOutputContext, ctx), getattr(args, "output", None), rendered)
        write_text(rendered)
        return view.exit_code
    payload = view.to_jsonable()
    rendered = dumps_json(payload)
    write_optional_output(cast(GitOpsOutputContext, ctx), getattr(args, "output", None), rendered)
    write_json(payload)
    return view.exit_code


__all__ = [
    "cmd_gitops_airflow_pod_contract",
    "cmd_gitops_airflow_pod_doctor",
    "register_pod_contract_parser",
    "register_pod_doctor_parser",
]
