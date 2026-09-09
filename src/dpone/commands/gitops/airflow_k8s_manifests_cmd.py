from __future__ import annotations

import argparse
import logging
from importlib import import_module
from typing import Any, cast

from dpone.commands.gitops.common import GitOpsOutputContext, write_optional_output
from dpone.commands.output_json import dumps_json, write_json


def register_k8s_manifests_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser(
        "k8s-manifests",
        help="Render deployable Kubernetes manifests for generated Airflow runtime artifacts",
    )
    p.add_argument("--artifact-dir", default=".dpone/gitops/airflow", help="Repo-relative Airflow artifact directory")
    p.add_argument("--runtime-profile-path", default=None, help="Repo-relative runtime-profile JSON path")
    p.add_argument("--pod-contract-path", default=None, help="Repo-relative pod-contract JSON path")
    p.add_argument("--connection-bridge-plan-path", default=None, help="Repo-relative connection bridge plan JSON path")
    p.add_argument(
        "--manifest-output",
        default=".dpone/gitops/airflow/airflow-k8s-manifests.yaml",
        help="Repo-relative Kubernetes manifest YAML output path",
    )
    p.add_argument(
        "--include-network-policy",
        action="store_true",
        help="Include an opt-in allow-all-egress NetworkPolicy skeleton",
    )
    p.add_argument(
        "--gitops-controller",
        choices=("plain", "argocd", "flux"),
        default="plain",
        help="Add controller-aware metadata for Argo CD or Flux GitOps consumption",
    )
    p.add_argument("--output", help="Optional repo-relative JSON report output path")
    p.add_argument("--format", choices=("json",), default="json", help="Output format")
    return p


def cmd_gitops_airflow_k8s_manifests(
    args: argparse.Namespace,
    *,
    ctx: object,
    logger: logging.Logger,
) -> int:
    _ = logger
    view = _k8s_manifests_service(ctx).build_view(args)
    payload = view.to_jsonable()
    rendered = dumps_json(payload)
    write_optional_output(cast(GitOpsOutputContext, ctx), getattr(args, "output", None), rendered)
    write_json(payload)
    return view.exit_code


def _k8s_manifests_service(ctx: object) -> Any:
    module = import_module("dpone.services.gitops.airflow_k8s_manifests_service")
    service_type = getattr(module, "GitOpsAirflowK8sManifestsService")
    return service_type(ctx=ctx)


__all__ = ["cmd_gitops_airflow_k8s_manifests", "register_k8s_manifests_parser"]
