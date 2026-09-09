from __future__ import annotations

import argparse
import logging

from dpone.cli_render.dag.node_e2e import render_explain_node_e2e_text
from dpone.commands.output_json import write_json
from dpone.dag.deps_end_to_end_explain import ProcessExplainCache
from dpone.dag.edge_explain import locate_node
from dpone.dag.errors import DagConfigurationError
from dpone.dag.node_end_to_end_explain import explain_node_end_to_end
from dpone.services.dag.load_context import load_dag_context
from dpone.services.dag.views import build_explain_node_e2e_view


def register_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser(
        "explain-node-e2e",
        help="Explain incoming/outgoing edges for a node end-to-end (YAML -> normalized -> edges)",
    )
    p.add_argument(
        "root",
        help=("Root manifest (same entrypoint you pass to DAG builder). If no extension, .yaml is assumed."),
    )
    p.add_argument("task", help="Task token (task_id, #selector, file.yaml#selector)")
    p.add_argument(
        "--focus-peer",
        dest="focus_peer",
        help="Limit end-to-end details to edges between the node and this peer (token).",
    )
    p.add_argument(
        "--direction",
        choices=["in", "out", "both"],
        default="both",
        help="Which edges to show.",
    )
    p.add_argument(
        "--output",
        choices=["compact", "full"],
        default="compact",
        help="Compact table or full edge-by-edge explanations.",
    )
    p.add_argument("--max-in", type=int, default=20, help="Max incoming edges")
    p.add_argument("--max-out", type=int, default=20, help="Max outgoing edges")
    p.add_argument(
        "--max-triggers",
        type=int,
        default=10,
        help="Max trigger tasks to print for group-to-group reasons.",
    )
    p.add_argument(
        "--base-path",
        help="Manifest directory (defaults to MANIFEST_DIR). Used for resolving relative root and refs.",
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


def cmd_dag_explain_node_e2e(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    dag = load_dag_context(args, ctx=ctx)

    try:
        node = locate_node(dag.nodes, token=str(args.task), root_file=dag.root_path, base_path=dag.base_path)
    except ValueError as exc:
        raise DagConfigurationError(str(exc)) from exc

    focus_peer_name = None
    if getattr(args, "focus_peer", None):
        try:
            peer = locate_node(dag.nodes, token=str(args.focus_peer), root_file=dag.root_path, base_path=dag.base_path)
        except ValueError as exc:
            raise DagConfigurationError(f"focus-peer: {exc}") from exc
        focus_peer_name = peer.name

    cache = ProcessExplainCache(registry_paths=dag.registry_paths)
    exp = explain_node_end_to_end(
        dag.edge_ctx,
        node_name=node.name,
        focus_peer_name=focus_peer_name,
        direction=str(getattr(args, "direction", "both") or "both"),
        max_in=int(getattr(args, "max_in", 20) or 20),
        max_out=int(getattr(args, "max_out", 20) or 20),
        max_triggers=int(getattr(args, "max_triggers", 10) or 10),
        registry_paths=dag.registry_paths,
        cache=cache,
    )

    view = build_explain_node_e2e_view(
        dag=dag,
        result=exp,
        direction=str(getattr(args, "direction", "both") or "both"),
        output=str(getattr(args, "output", "compact") or "compact"),
        focus_peer_raw=getattr(args, "focus_peer", None),
        focus_peer_resolved=focus_peer_name,
    )

    if args.format == "json":
        write_json(view.to_jsonable())
        return 0

    text = render_explain_node_e2e_text(view, edge_ctx=dag.edge_ctx)
    print(text, end="")

    return 0
