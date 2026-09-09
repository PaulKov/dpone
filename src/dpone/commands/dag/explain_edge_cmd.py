from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.services.dag.views.common import PathSegmentView


import argparse
import logging
from pathlib import Path
from typing import cast

from dpone.cli_render.dag.edge import render_explain_edge_text
from dpone.commands.output_json import write_json
from dpone.dag.edge_explain import explain_direct_edge, locate_node
from dpone.dag.errors import DagConfigurationError
from dpone.gitops.airflow_dag_spec_explain import (
    explain_dag_spec_direct_edge,
    resolve_dag_spec_context,
    to_dag_edge_explanation,
)
from dpone.services.dag.load_context import load_dag_context
from dpone.services.dag.path_view import reasons_signature
from dpone.services.dag.views import ExplainEdgeView, build_explain_edge_view
from dpone.services.dag.views.common import DagViewMeta, build_path_segments
from dpone.services.dag.views.edge import EdgeStepView


def register_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser(
        "explain-edge",
        help="Explain why a DAG edge A -> B exists (mirrors TaskGroupBuilder semantics)",
    )
    p.add_argument(
        "root",
        help=(
            "Root manifest (same entrypoint you pass to DAG builder). "
            "With --dag-spec this is the GitOps workload-set path (for example "
            "dpone_workloads/gitops/gitops.yaml)."
        ),
    )
    p.add_argument("--from", dest="upstream", required=True, help="Upstream task: task_id or file.yaml#selector")
    p.add_argument("--to", dest="downstream", required=True, help="Downstream task: task_id or file.yaml#selector")
    p.add_argument(
        "--dag-spec",
        action="store_true",
        help="Explain a GitOps dag-spec edge using declared/curated/inferred layer provenance.",
    )
    p.add_argument(
        "--dag-id",
        help="DAG id from the domain catalog dags: block (required with --dag-spec).",
    )
    p.add_argument(
        "--env",
        default="dev",
        help="GitOps environment label for dag-spec resolution (default: dev).",
    )
    p.add_argument(
        "--base-path",
        help="Manifest directory (defaults to MANIFEST_DIR). Used for resolving relative root and refs.",
    )
    p.add_argument(
        "--path",
        action="store_true",
        help="If there is no direct edge, try to find and print a shortest path A ~> B.",
    )
    p.add_argument(
        "--explain-path",
        action="store_true",
        help="With --path: also explain every edge along the found path.",
    )
    p.add_argument(
        "--path-view",
        choices=["edges", "grouped"],
        default="edges",
        help="How to present path explanations when --explain-path is enabled.",
    )
    p.add_argument(
        "--path-output",
        choices=["compact", "full"],
        default="compact",
        help="Level of detail for each edge explanation along the path.",
    )
    p.add_argument(
        "--max-triggers",
        type=int,
        default=10,
        help="Max trigger tasks to print for group-to-group reasons.",
    )
    p.add_argument("--format", choices=["text", "json"], default="text", help="Output format")
    p.add_argument(
        "--registry",
        action="append",
        default=[],
        help=(
            "Path to a sources registry YAML (can be repeated). "
            "Registry provides default vars like host/type for naming/metadata conventions."
        ),
    )
    return p


def cmd_dag_explain_edge(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    """Explain why a direct DAG edge exists (TaskGroupBuilder parity)."""

    if bool(getattr(args, "dag_spec", False)):
        return _cmd_dag_explain_dag_spec_edge(args, ctx=ctx)

    dag = load_dag_context(args, ctx=ctx)

    try:
        up_node = locate_node(dag.nodes, token=str(args.upstream), root_file=dag.root_path, base_path=dag.base_path)
        dn_node = locate_node(dag.nodes, token=str(args.downstream), root_file=dag.root_path, base_path=dag.base_path)
    except ValueError as exc:
        raise DagConfigurationError(str(exc)) from exc

    exp = explain_direct_edge(
        dag.edge_ctx,
        upstream_name=up_node.name,
        downstream_name=dn_node.name,
        max_triggers=int(getattr(args, "max_triggers", 10)),
        include_transitive_path=bool(getattr(args, "path", False) or getattr(args, "explain_path", False)),
    )

    edge_steps = None
    if exp.path and bool(getattr(args, "explain_path", False)):
        edge_steps = []
        path = list(exp.path)
        for i in range(len(path) - 1):
            a = path[i]
            b = path[i + 1]
            e2 = explain_direct_edge(
                dag.edge_ctx,
                upstream_name=a,
                downstream_name=b,
                max_triggers=int(getattr(args, "max_triggers", 10)),
                include_transitive_path=False,
            )
            edge_steps.append((a, b, e2))

    view = build_explain_edge_view(
        dag=dag,
        upstream_name=up_node.name,
        downstream_name=dn_node.name,
        result=exp,
        path_edges=edge_steps,
        path_view=str(getattr(args, "path_view", "edges") or "edges"),
        path_output=str(getattr(args, "path_output", "compact") or "compact"),
    )

    if args.format == "json":
        write_json(view.to_jsonable())
        return view.exit_code()

    text = render_explain_edge_text(view)
    print(text, end="")

    return view.exit_code()


class _LoaderContext:
    @property
    def settings(self) -> object:
        raise NotImplementedError


def _cmd_dag_explain_dag_spec_edge(args: argparse.Namespace, *, ctx: object) -> int:
    dag_id = str(getattr(args, "dag_id", "") or "").strip()
    if not dag_id:
        raise DagConfigurationError("--dag-id is required with --dag-spec")

    loader_ctx = cast(_LoaderContext, ctx)
    repo_root = Path(getattr(loader_ctx.settings, "repo_root"))
    workload_set = str(getattr(args, "root", "") or "").strip()
    if not workload_set:
        raise DagConfigurationError("workload-set path is required as the root positional argument")

    resolved, blockers = resolve_dag_spec_context(
        repo_root=repo_root,
        workload_set=workload_set,
        dag_id=dag_id,
        env=str(getattr(args, "env", "dev") or "dev"),
    )
    if resolved is None:
        message = blockers[0].message if blockers else f"Unable to resolve dag-spec {dag_id!r}"
        raise DagConfigurationError(message)

    try:
        explanation = explain_dag_spec_direct_edge(
            context=resolved,
            upstream_id=str(args.upstream),
            downstream_id=str(args.downstream),
            include_path=bool(getattr(args, "path", False) or getattr(args, "explain_path", False)),
        )
    except ValueError as exc:
        raise DagConfigurationError(str(exc)) from exc

    legacy = to_dag_edge_explanation(explanation)
    step_views: tuple[EdgeStepView, ...] = ()
    if explanation.path and bool(getattr(args, "explain_path", False)):
        steps: list[EdgeStepView] = []
        path = list(explanation.path)
        for index in range(len(path) - 1):
            upstream = path[index]
            downstream = path[index + 1]
            step = explain_dag_spec_direct_edge(
                context=resolved,
                upstream_id=upstream,
                downstream_id=downstream,
                include_path=False,
            )
            steps.append(EdgeStepView(upstream, downstream, to_dag_edge_explanation(step)))
        step_views = tuple(steps)

    path_view = str(getattr(args, "path_view", "edges") or "edges")
    path_output = str(getattr(args, "path_output", "compact") or "compact")
    segments: tuple[PathSegmentView, ...] = ()
    if path_view == "grouped" and step_views:
        segments = build_path_segments(
            step_views,
            key_fn=lambda step: reasons_signature(step.result.reasons),
            signature_fn=lambda key: repr(key),
            nodes_fn=lambda seg: [seg[0].upstream_name] + [item.downstream_name for item in seg],
            primary_reason_fn=lambda seg: seg[0].result.reasons[0].to_jsonable() if seg[0].result.reasons else None,
        )

    view = ExplainEdgeView(
        meta=DagViewMeta(
            kind="dag.explain_edge_dag_spec",
            root=workload_set,
            base_path=str(repo_root),
            task_count=len(resolved.membership.nodes),
            options={
                "dag_id": dag_id,
                "env": str(getattr(args, "env", "dev") or "dev"),
                "path_view": path_view,
                "path_output": path_output,
            },
        ),
        upstream_name=str(args.upstream),
        downstream_name=str(args.downstream),
        result=legacy,
        path_edges=step_views,
        path_segments=segments,
    )

    if args.format == "json":
        payload = explanation.to_jsonable()
        payload.update(
            {
                "upstream_name": view.upstream_name,
                "downstream_name": view.downstream_name,
                "result": legacy.to_jsonable(),
            }
        )
        write_json(payload)
        return view.exit_code()

    text = render_explain_edge_text(view)
    print(text, end="")
    return view.exit_code()
