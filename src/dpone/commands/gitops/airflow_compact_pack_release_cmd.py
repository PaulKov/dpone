"""CLI for promoting compact packs into a v2-ready release-set."""

from __future__ import annotations

import argparse
import logging
from importlib import import_module
from typing import Any, cast

from dpone.commands.gitops.common import GitOpsOutputContext, write_optional_output
from dpone.commands.output_json import dumps_json, write_json
from dpone.commands.output_text import write_text
from dpone.contracts.dbt_compact_release import COMPACT_OUTPUT_INVALID

# registry.example.test/dockerhub/alpine:3.20 digest (2026-07-21)
_DEFAULT_XCOM_SIDECAR_IMAGE = (
    "registry.example.test/dockerhub/alpine@sha256:d9e853e87e55526f6b2917df91a2115c36dd7c696a35be12163d44e6e2a4b6bc"
)


def register_release_materialize_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "release-materialize",
        help=(
            "Promote compact reconcile packs into an immutable release-set "
            "for v2 init_fetch RuntimeConnectionContext delivery"
        ),
    )
    parser.add_argument(
        "--pack-root",
        default=".dpone/gitops/airflow",
        help="Repo-relative legacy reconcile output or complete workspace wire-v2 compile output",
    )
    parser.add_argument(
        "--cache-root",
        default=".dpone-cache",
        help="Local cache root for releases/",
    )
    parser.add_argument(
        "--xcom-sidecar-image",
        default=_DEFAULT_XCOM_SIDECAR_IMAGE,
        help="Digest-pinned OCI ref required by strict init_fetch",
    )
    parser.add_argument(
        "--dag-id",
        action="append",
        default=[],
        help="Repeatable DAG filter. Default: all DAGs. Native workspace input requires the complete set",
    )
    parser.add_argument("--output", help="Optional repo-relative console output mirror path")
    parser.add_argument("--format", choices=("json", "markdown"), default="json", help="Output format")
    return parser


def cmd_gitops_airflow_release_materialize(
    args: argparse.Namespace,
    *,
    ctx: object,
    logger: logging.Logger,
) -> int:
    _ = logger
    view = _service(ctx).materialize_view(args)
    output = (
        None
        if any(str(item).startswith(COMPACT_OUTPUT_INVALID) for item in view.report.blockers)
        else getattr(args, "output", None)
    )
    if getattr(args, "format", "json") == "markdown":
        payload = dumps_json(view.to_jsonable())
        rendered = "# Gitops Airflow Compact Pack Release\n\n```json\n" + payload + "\n```\n"
        write_optional_output(cast(GitOpsOutputContext, ctx), output, rendered)
        write_text(rendered)
        return view.exit_code
    payload = view.to_jsonable()
    rendered = dumps_json(payload)
    write_optional_output(cast(GitOpsOutputContext, ctx), output, rendered)
    write_json(payload)
    return view.exit_code


def _service(ctx: object) -> Any:
    module = import_module("dpone.services.gitops.airflow_compact_pack_release_service")
    return getattr(module, "GitOpsAirflowCompactPackReleaseService")(ctx=ctx)


__all__ = [
    "cmd_gitops_airflow_release_materialize",
    "register_release_materialize_parser",
]
