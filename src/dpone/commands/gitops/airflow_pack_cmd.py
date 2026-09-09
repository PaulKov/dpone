from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.services.gitops.views import GitOpsView


import argparse
import logging
from importlib import import_module
from typing import Any, cast

from dpone.commands.gitops.common import GitOpsOutputContext, write_optional_output
from dpone.commands.output_json import dumps_json, write_json
from dpone.commands.output_text import write_text
from dpone.gitops.airflow_pack_rendering import render_gitops_airflow_pack_markdown

_RUNNER_POLICY_CHOICES = ("advisory", "pr", "release")


def register_pack_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser(
        "pack",
        help="Build a compact workload Airflow pack or plan legacy runtime artifacts",
    )
    p.add_argument("--workload", help="GitOps workload id for compact pack mode")
    p.add_argument("--workload-set", help="Repo-relative GitOps workload-set YAML for compact pack mode")
    p.add_argument("--env", default="dev", help="Environment scope for compact pack mode")
    p.add_argument("--artifact-dir", default=".dpone/gitops/airflow", help="Repo-relative Airflow artifact directory")
    p.add_argument("--bundle-path", default=".dpone/gitops/bundle/bundle.json", help="Repo-relative bundle JSON path")
    p.add_argument("--image", help="Custom dpone runtime image used by Airflow workers")
    p.add_argument("--image-digest", help="Optional immutable image digest for planned runtime commands")
    p.add_argument("--mode", choices=("plan", "verify"), default="plan", help="Plan steps or verify local artifacts")
    p.add_argument("--runner-policy", choices=_RUNNER_POLICY_CHOICES, default="advisory", help="Pack policy profile")
    p.add_argument(
        "--include-live-gates",
        action="store_true",
        help="Include credentialed live Kubernetes/Airflow gate commands in the report",
    )
    p.add_argument(
        "--output-path",
        default=None,
        help="Repo-relative pack JSON path; defaults to <artifact-dir>/airflow-runtime-pack.json",
    )
    p.add_argument("--output", help="Optional repo-relative console output mirror path")
    p.add_argument("--format", choices=("json", "markdown"), default="json", help="Output format")
    return p


def register_reconcile_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser(
        "reconcile",
        help="Build compact Airflow packs and DAG specs for affected or all workloads",
    )
    p.add_argument("--workload-set", required=True, help="Repo-relative GitOps workload-set YAML")
    p.add_argument(
        "--all-workloads",
        action="store_true",
        help="Build every workload in the catalog; conflicts with changed-file selection",
    )
    p.add_argument("--changed-files", nargs="+", default=[], help="Repo-relative changed files")
    p.add_argument("--changed-files-file", help="Repo-relative newline-delimited changed-files list")
    p.add_argument("--env", default="dev", help="Environment scope")
    p.add_argument("--output-dir", default=".dpone/gitops", help="Repo-relative GitOps output root")
    p.add_argument("--output", help="Optional repo-relative console output mirror path")
    p.add_argument("--format", choices=("json", "markdown"), default="json", help="Output format")
    return p


def cmd_gitops_airflow_pack(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    _ = logger
    if getattr(args, "workload", None):
        view = _compact_pack_service(ctx).pack_view(args)
        return _write_view(view, args=args, ctx=ctx)
    view = _pack_service(ctx).build_view(args)
    return _write_view(view, args=args, ctx=ctx)


def cmd_gitops_airflow_reconcile(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    _ = logger
    view = _compact_pack_service(ctx).reconcile_view(args)
    return _write_view(view, args=args, ctx=ctx)


def _write_view(view: GitOpsView, *, args: argparse.Namespace, ctx: object) -> int:
    if getattr(args, "format", "json") == "markdown":
        rendered = _render_markdown(view)
        _write_optional_output(view, args=args, ctx=ctx, rendered=rendered)
        write_text(rendered)
        return view.exit_code
    payload = view.to_jsonable()
    rendered = dumps_json(payload)
    _write_optional_output(view, args=args, ctx=ctx, rendered=rendered)
    write_json(payload)
    return view.exit_code


def _write_optional_output(view: GitOpsView, *, args: argparse.Namespace, ctx: object, rendered: str) -> None:
    raw_output = getattr(args, "output", None)
    if _output_mirror_is_blocked(view, raw_output):
        return
    write_optional_output(
        cast(GitOpsOutputContext, ctx),
        raw_output,
        rendered,
        suppress_unsafe=view.exit_code != 0,
    )


def _output_mirror_is_blocked(view: GitOpsView, raw_output: object) -> bool:
    if not raw_output:
        return False
    blockers = getattr(view.report, "blockers", ())
    return any(
        getattr(blocker, "code", None) == "invalid_path" and getattr(blocker, "path", None) == str(raw_output)
        for blocker in blockers
    )


def _render_markdown(view: GitOpsView) -> str:
    if _is_legacy_airflow_pack(view.report):
        return render_gitops_airflow_pack_markdown(view.report)
    payload = dumps_json(view.to_jsonable())
    title = str(view.meta.kind).replace(".", " ").replace("_", " ").title()
    return f"# {title}\n\n```json\n{payload}\n```\n"


def _is_legacy_airflow_pack(report: object) -> bool:
    return all(
        hasattr(report, attr)
        for attr in ("artifacts", "artifact_dir", "mode", "next_actions", "output_path", "runner_policy")
    )


def _pack_service(ctx: object) -> Any:
    module = import_module("dpone.services.gitops.airflow_pack_service")
    service_type = getattr(module, "GitOpsAirflowPackService")
    return service_type(ctx=ctx)


def _compact_pack_service(ctx: object) -> Any:
    service_module = import_module("dpone.services.gitops.airflow_compact_pack_service")
    writer_module = import_module("dpone.gitops.airflow_dag_spec_artifacts")
    service_type = getattr(service_module, "GitOpsAirflowCompactPackService")
    writer_type = getattr(writer_module, "AirflowDagSpecArtifactWriter")
    repo_root = getattr(getattr(ctx, "settings"), "repo_root")
    return service_type(ctx=ctx, dag_spec_writer=writer_type(repo_root=repo_root))


def register_publish_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "publish",
        help="Build and publish compact Airflow packs to object storage",
    )
    parser.add_argument("--workload-set", required=True)
    parser.add_argument("--env", default="dev")
    parser.add_argument("--workload", action="append", default=[])
    parser.add_argument("--git-sha", required=True)
    parser.add_argument("--uri-prefix")
    parser.add_argument("--latest-index-uri")
    parser.add_argument(
        "--pack-dir",
        help="Repo-relative directory with pre-built packs (<pack-dir>/<workload_id>/airflow-pack.json)",
    )
    parser.add_argument("--local-root-dir", help="Local filesystem root for dry-run publish without S3 credentials")
    parser.add_argument("--connection-type", default="airflow", choices=("airflow", "env", "vault", "params"))
    parser.add_argument("--connection-id")
    parser.add_argument("--max-pack-bytes", type=int, default=10 * 1024**2)
    parser.add_argument("--max-index-bytes", type=int, default=25 * 1024**2)
    parser.add_argument("--output")
    parser.add_argument("--format", choices=("json",), default="json")
    return parser


def cmd_gitops_airflow_publish(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del logger
    module = import_module("dpone.services.gitops.airflow_pack_publish_service")
    return int(module.run_publish(args, ctx=ctx))


__all__ = [
    "cmd_gitops_airflow_pack",
    "cmd_gitops_airflow_publish",
    "cmd_gitops_airflow_reconcile",
    "register_pack_parser",
    "register_publish_parser",
    "register_reconcile_parser",
]
