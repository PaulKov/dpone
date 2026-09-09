from __future__ import annotations

import argparse
import logging
from importlib import import_module
from typing import Any, cast

from dpone.commands.gitops.common import GitOpsOutputContext, write_optional_output
from dpone.commands.output_json import dumps_json, write_json
from dpone.commands.output_text import write_text
from dpone.gitops.airflow_connection_bridge_plan_rendering import (
    render_gitops_airflow_connection_bridge_plan_markdown,
)


def register_connection_bridge_plan_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser(
        "connection-bridge-plan",
        help="Build Secret, ExternalSecret, and env skeletons for Airflow connection bridge contracts",
    )
    p.add_argument("--artifact-dir", default=".dpone/gitops/airflow", help="Repo-relative Airflow artifact directory")
    p.add_argument(
        "--output-path",
        default=None,
        help="Repo-relative plan JSON path; defaults to <artifact-dir>/connection-bridge-plan.json",
    )
    p.add_argument("--runtime-profile-path", default=None, help="Repo-relative runtime-profile JSON path")
    p.add_argument("--pod-contract-path", default=None, help="Repo-relative pod-contract JSON path")
    p.add_argument("--secret-manifest-path", default=None, help="Repo-relative Kubernetes Secret skeleton path")
    p.add_argument("--external-secret-path", default=None, help="Repo-relative ExternalSecret skeleton path")
    p.add_argument("--env-example-path", default=None, help="Repo-relative AIRFLOW_CONN_* env example path")
    p.add_argument("--external-secret-store", default="airflow-connections", help="ExternalSecret SecretStore name")
    p.add_argument(
        "--external-secret-store-kind",
        choices=("SecretStore", "ClusterSecretStore"),
        default="SecretStore",
        help="ExternalSecret SecretStore reference kind",
    )
    p.add_argument(
        "--external-secret-remote-prefix",
        default="airflow/connections",
        help="ExternalSecret remote key prefix for AIRFLOW_CONN_* values",
    )
    p.add_argument("--output", help="Optional repo-relative report output path")
    p.add_argument("--format", choices=["json", "markdown"], default="json", help="Output format")
    return p


def cmd_gitops_airflow_connection_bridge_plan(
    args: argparse.Namespace,
    *,
    ctx: object,
    logger: logging.Logger,
) -> int:
    _ = logger
    view = _connection_bridge_plan_service(ctx).build_view(args)
    if getattr(args, "format", "json") == "markdown":
        rendered = render_gitops_airflow_connection_bridge_plan_markdown(view.report)
        write_optional_output(cast(GitOpsOutputContext, ctx), getattr(args, "output", None), rendered)
        write_text(rendered)
        return view.exit_code
    payload = view.to_jsonable()
    rendered = dumps_json(payload)
    write_optional_output(cast(GitOpsOutputContext, ctx), getattr(args, "output", None), rendered)
    write_json(payload)
    return view.exit_code


def _connection_bridge_plan_service(ctx: object) -> Any:
    module = import_module("dpone.services.gitops.airflow_connection_bridge_plan_service")
    service_type = getattr(module, "GitOpsAirflowConnectionBridgePlanService")
    return service_type(ctx=ctx)


__all__ = [
    "cmd_gitops_airflow_connection_bridge_plan",
    "register_connection_bridge_plan_parser",
]
