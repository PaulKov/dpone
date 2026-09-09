from __future__ import annotations

import argparse
import logging
from typing import cast

from dpone.commands.gitops.common import GitOpsOutputContext, write_optional_output
from dpone.commands.output_json import dumps_json, write_json
from dpone.commands.output_text import write_text
from dpone.gitops.airflow_outcome_gate import airflow_outcome_mode_names
from dpone.gitops.airflow_rendering import render_gitops_airflow_runtime_profile_markdown
from dpone.services.gitops.airflow_runtime_profile_service import (
    GitOpsAirflowRuntimeProfileContext,
    GitOpsAirflowRuntimeProfileService,
)

_OUTCOME_MODE_CHOICES = airflow_outcome_mode_names()
_RUNNER_POLICY_CHOICES = ("advisory", "pr", "release")


def register_runtime_profile_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser(
        "runtime-profile",
        help="Build Airflow runtime profile, XCom summary, and DAG factory artifacts",
    )
    p.add_argument("bundle_path", help="Repo-relative gitops.bundle JSON path")
    p.add_argument(
        "--run-spec-path",
        default=".dpone/gitops/airflow/run-spec.json",
        help="Repo-relative Airflow run-spec JSON path",
    )
    p.add_argument(
        "--runtime-evidence-path",
        default=".dpone/gitops/airflow/runtime-evidence.json",
        help="Repo-relative runtime evidence JSON path written by the runner",
    )
    p.add_argument(
        "--output-path",
        default=".dpone/gitops/airflow/runtime-profile.json",
        help="Repo-relative runtime profile JSON artifact path",
    )
    p.add_argument(
        "--xcom-summary-path",
        default=".dpone/gitops/airflow/xcom-summary.json",
        help="Repo-relative planned XCom summary JSON artifact path",
    )
    p.add_argument(
        "--dag-factory-path",
        default=".dpone/gitops/airflow/airflow_dag_factory.py",
        help="Repo-relative generated Airflow DAG factory path",
    )
    p.add_argument(
        "--outcome-gate-path",
        default=".dpone/gitops/airflow/outcome_gate.py",
        help="Repo-relative generated Airflow outcome gate helper path",
    )
    p.add_argument("--image", required=True, help="Custom dpone runner image")
    p.add_argument("--image-digest", help="Immutable custom dpone image digest")
    p.add_argument("--namespace", default="default", help="Kubernetes namespace for runner pods")
    p.add_argument("--service-account", default="default", help="Kubernetes service account for runner pods")
    p.add_argument("--cpu-request", default="100m", help="Container CPU request")
    p.add_argument("--memory-request", default="256Mi", help="Container memory request")
    p.add_argument("--cpu-limit", default="1", help="Container CPU limit")
    p.add_argument("--memory-limit", default="1Gi", help="Container memory limit")
    p.add_argument("--artifact-sink-kind", default="local", help="Runtime artifact sink kind")
    p.add_argument("--artifact-sink-path", default=".dpone/gitops/airflow", help="Repo-relative artifact sink path")
    p.add_argument("--env", action="append", default=[], help="Runner environment variable NAME=value; repeatable")
    p.add_argument("--label", action="append", default=[], help="Kubernetes label KEY=value; repeatable")
    p.add_argument("--annotation", action="append", default=[], help="Kubernetes annotation KEY=value; repeatable")
    p.add_argument("--git-sync-repo", help="Git repository URL for sparse git-sync initContainers")
    p.add_argument("--git-sync-ref", help="Git ref, branch, tag, or SHA for git-sync; defaults to HEAD")
    p.add_argument("--git-sync-image", help="git-sync v4 image used by the sparse checkout initContainer")
    p.add_argument("--git-sync-depth", type=int, default=1, help="git-sync shallow clone depth; 0 syncs full history")
    p.add_argument(
        "--git-sync-filter",
        choices=("blob:none", "tree:0"),
        help="git-sync partial clone filter; requires git-sync v4.7.0+ for release profiles",
    )
    p.add_argument(
        "--git-sync-auth-mode",
        choices=("image", "ssh_secret", "https_secret"),
        default="image",
        help="git-sync auth source; secret modes serialize only secret names and keys",
    )
    p.add_argument("--git-sync-ssh-secret", help="Kubernetes Secret name with SSH key and known_hosts data")
    p.add_argument("--git-sync-ssh-key", default="ssh", help="Secret key containing the SSH private key")
    p.add_argument(
        "--git-sync-ssh-known-hosts-key",
        default="known_hosts",
        help="Secret key containing SSH known_hosts data",
    )
    p.add_argument("--git-sync-https-secret", help="Kubernetes Secret name with HTTPS username/password data")
    p.add_argument("--git-sync-https-username-key", default="username", help="Secret key containing HTTPS username")
    p.add_argument(
        "--git-sync-https-password-key", default="password", help="Secret key containing HTTPS password/token"
    )
    p.add_argument(
        "--airflow-connection-bridge",
        choices=("k8s_secret", "env", "disabled"),
        default="k8s_secret",
        help="Bridge connection_type=airflow ids into runtime-only pods",
    )
    p.add_argument(
        "--airflow-connection-secret",
        default="dpone-airflow-connections",
        help="Kubernetes Secret containing AIRFLOW_CONN_* URI values",
    )
    p.add_argument(
        "--airflow-runtime-mode",
        choices=("runtime_only", "airflow_image"),
        default="runtime_only",
        help="Runner image contract mode for Airflow connection resolution",
    )
    p.add_argument(
        "--runner-policy",
        choices=_RUNNER_POLICY_CHOICES,
        default="advisory",
        help="Airflow runtime profile policy",
    )
    p.add_argument(
        "--outcome-mode",
        choices=_OUTCOME_MODE_CHOICES,
        default="strict_fail",
        help="Pod exit strategy for final XCom outcomes",
    )
    p.add_argument("--output", help="Optional repo-relative report output path")
    p.add_argument("--format", choices=["json", "markdown"], default="json", help="Output format")
    return p


def cmd_gitops_airflow_runtime_profile(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    _ = logger
    view = GitOpsAirflowRuntimeProfileService(ctx=cast(GitOpsAirflowRuntimeProfileContext, ctx)).build_view(args)
    if getattr(args, "format", "json") == "markdown":
        rendered = render_gitops_airflow_runtime_profile_markdown(view.report)
        write_optional_output(cast(GitOpsOutputContext, ctx), getattr(args, "output", None), rendered)
        write_text(rendered)
        return view.exit_code
    payload = view.to_jsonable()
    rendered = dumps_json(payload)
    write_optional_output(cast(GitOpsOutputContext, ctx), getattr(args, "output", None), rendered)
    write_json(payload)
    return view.exit_code


__all__ = ["cmd_gitops_airflow_runtime_profile", "register_runtime_profile_parser"]
