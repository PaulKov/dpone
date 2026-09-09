from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.dag.edge_end_to_end_explain import EdgeEndToEndResult


import argparse
import logging

from dpone.cli_render.dag.edge_e2e import render_edge_e2e_text
from dpone.commands.output_json import write_json
from dpone.dag.deps_end_to_end_explain import ProcessExplainCache
from dpone.dag.edge_end_to_end_explain import explain_edge_end_to_end
from dpone.dag.edge_explain import locate_node
from dpone.dag.errors import DagConfigurationError
from dpone.services.dag.load_context import load_dag_context
from dpone.services.dag.views import build_explain_edge_e2e_view


def register_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser(
        "explain-edge-e2e",
        help="Explain a specific edge A -> B end-to-end (YAML -> normalized -> edge)",
    )
    p.add_argument(
        "root",
        help=("Root manifest (same entrypoint you pass to DAG builder). If no extension, .yaml is assumed."),
    )
    p.add_argument("--from", dest="upstream", required=True, help="Upstream task: task_id or file.yaml#selector")
    p.add_argument("--to", dest="downstream", required=True, help="Downstream task: task_id or file.yaml#selector")
    p.add_argument(
        "--base-path",
        help="Manifest directory (defaults to MANIFEST_DIR). Used for resolving relative root and refs.",
    )
    p.add_argument(
        "--max-triggers",
        type=int,
        default=10,
        help="Max trigger tasks to print for group-to-group attributions.",
    )
    p.add_argument(
        "--path",
        action="store_true",
        help="If there is no direct edge, try to find and print a shortest path A ~> B.",
    )
    p.add_argument(
        "--explain-path",
        action="store_true",
        help=("With --path (or when used alone): explain every edge along the found shortest path end-to-end."),
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
        help="Level of detail for each end-to-end edge explanation along the path.",
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


def cmd_dag_explain_edge_e2e(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    dag = load_dag_context(args, ctx=ctx)

    try:
        up_node = locate_node(dag.nodes, token=str(args.upstream), root_file=dag.root_path, base_path=dag.base_path)
        dn_node = locate_node(dag.nodes, token=str(args.downstream), root_file=dag.root_path, base_path=dag.base_path)
    except ValueError as exc:
        raise DagConfigurationError(str(exc)) from exc

    cache = ProcessExplainCache(registry_paths=dag.registry_paths)

    res = explain_edge_end_to_end(
        upstream_node=up_node,
        downstream_node=dn_node,
        ctx=dag.edge_ctx,
        registry_paths=dag.registry_paths,
        cache=cache,
        max_triggers=int(getattr(args, "max_triggers", 10) or 10),
        include_transitive_path=bool(getattr(args, "path", False) or getattr(args, "explain_path", False)),
    )

    path_edge_results: list[EdgeEndToEndResult] | None = None
    if (not res.direct_edge) and res.path and bool(getattr(args, "explain_path", False)):
        path_edge_results = []
        path = list(res.path)
        for i in range(len(path) - 1):
            a = path[i]
            b = path[i + 1]
            an = dag.edge_ctx.nodes_by_name.get(a)
            bn = dag.edge_ctx.nodes_by_name.get(b)
            if not an or not bn:
                continue
            path_edge_results.append(
                explain_edge_end_to_end(
                    upstream_node=an,
                    downstream_node=bn,
                    ctx=dag.edge_ctx,
                    registry_paths=dag.registry_paths,
                    cache=cache,
                    max_triggers=int(getattr(args, "max_triggers", 10) or 10),
                    include_transitive_path=False,
                )
            )

    view = build_explain_edge_e2e_view(
        dag=dag,
        upstream_name=up_node.name,
        downstream_name=dn_node.name,
        result=res,
        path_edges=path_edge_results,
        path_view=str(getattr(args, "path_view", "edges") or "edges"),
        path_output=str(getattr(args, "path_output", "compact") or "compact"),
    )

    if args.format == "json":
        write_json(view.to_jsonable())
        return view.exit_code()

    text = render_edge_e2e_text(view)
    print(text, end="")

    return view.exit_code()
