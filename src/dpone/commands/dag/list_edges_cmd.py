from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Any

from dpone.commands.output_json import write_json
from dpone.dag.edge_explain import explain_direct_edge, locate_node
from dpone.dag.errors import DagConfigurationError
from dpone.output_table import render_table
from dpone.services.dag.load_context import load_dag_context


def register_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser(
        "list-edges",
        help="List DAG edges for a given root manifest (Variant C aware)",
    )
    p.add_argument(
        "root",
        help=("Root manifest (same entrypoint you pass to DAG builder). If no extension, .yaml is assumed."),
    )
    p.add_argument(
        "--base-path",
        help="Manifest directory (defaults to MANIFEST_DIR). Used for resolving relative root and refs.",
    )
    p.add_argument(
        "--from",
        dest="upstream",
        help="Filter: upstream task token (task_id, #selector, file.yaml#selector)",
    )
    p.add_argument(
        "--to",
        dest="downstream",
        help="Filter: downstream task token (task_id, #selector, file.yaml#selector)",
    )
    p.add_argument(
        "--task",
        action="append",
        default=[],
        help="Filter: include only edges touching this task token (can be repeated)",
    )
    p.add_argument(
        "--group",
        action="append",
        default=[],
        help="Filter: include only edges touching this task_group (can be repeated)",
    )
    p.add_argument(
        "--file",
        action="append",
        default=[],
        help="Filter: include only edges touching a YAML file (absolute or relative; can be repeated)",
    )
    p.add_argument(
        "--selector",
        action="append",
        default=[],
        help="Filter: include only edges touching this selector (can be repeated)",
    )
    p.add_argument(
        "--with-refs",
        action="store_true",
        help="Include node refs (file.yaml#selector) for copy/paste into explain-edge",
    )
    p.add_argument(
        "--with-files",
        action="store_true",
        help="Include node config_path filenames",
    )
    p.add_argument(
        "--with-groups",
        action="store_true",
        help="Include upstream/downstream task_group columns",
    )
    p.add_argument(
        "--with-reasons",
        action="store_true",
        help="Include reason kinds for each printed edge (slower for large graphs)",
    )
    p.add_argument(
        "--max",
        dest="max_edges",
        type=int,
        default=200,
        help="Max number of edges to print (after filtering)",
    )
    p.add_argument(
        "--max-triggers",
        type=int,
        default=5,
        help="Max trigger tasks to attribute for group-to-group reasons (when --with-reasons)",
    )
    p.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format",
    )
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


def cmd_dag_list_edges(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    """Variant C aware DAG edge lister."""

    dag = load_dag_context(args, ctx=ctx)

    # Resolve optional node filters
    upstream_node = None
    downstream_node = None
    if getattr(args, "upstream", None):
        try:
            upstream_node = locate_node(
                dag.nodes, token=str(args.upstream), root_file=dag.root_path, base_path=dag.base_path
            )
        except ValueError as exc:
            raise DagConfigurationError(str(exc)) from exc
    if getattr(args, "downstream", None):
        try:
            downstream_node = locate_node(
                dag.nodes, token=str(args.downstream), root_file=dag.root_path, base_path=dag.base_path
            )
        except ValueError as exc:
            raise DagConfigurationError(str(exc)) from exc

    touch_nodes: set[str] = set()
    for tok in args.task or []:
        if not str(tok).strip():
            continue
        try:
            n = locate_node(dag.nodes, token=str(tok), root_file=dag.root_path, base_path=dag.base_path)
        except ValueError as exc:
            raise DagConfigurationError(str(exc)) from exc
        touch_nodes.add(n.name)

    group_filters = {str(g).strip() for g in (args.group or []) if str(g).strip()}

    file_filters: set[Path] = set()
    for fp in args.file or []:
        s = str(fp).strip()
        if not s:
            continue
        p = Path(s)
        if not p.is_absolute():
            # same heuristic as locate_node
            if "/" in s or s.startswith("."):
                p = dag.root_path.parent / p
            else:
                p = dag.base_path / p
        file_filters.add(p.resolve(strict=False))

    selector_filters = {str(s).strip() for s in (args.selector or []) if str(s).strip()}

    # Collect edges
    edges: list[tuple[str, str]] = []
    for up, downs in dag.edge_ctx.adjacency.items():
        for downstream_name in downs:
            edges.append((up, downstream_name))

    edges.sort(key=lambda x: (x[0], x[1]))

    def _edge_touches(nm: str) -> bool:
        return nm in touch_nodes

    def _matches_edge(up: str, dn: str) -> bool:
        if upstream_node and up != upstream_node.name:
            return False
        if downstream_node and dn != downstream_node.name:
            return False
        if touch_nodes and not (_edge_touches(up) or _edge_touches(dn)):
            return False
        if group_filters:
            u = dag.edge_ctx.nodes_by_name.get(up)
            d = dag.edge_ctx.nodes_by_name.get(dn)
            ug = str(getattr(u, "task_group", "") or "")
            dg = str(getattr(d, "task_group", "") or "")
            if ug not in group_filters and dg not in group_filters:
                return False
        if selector_filters:
            u = dag.edge_ctx.nodes_by_name.get(up)
            d = dag.edge_ctx.nodes_by_name.get(dn)
            us = str(getattr(u, "selector", "") or "")
            ds = str(getattr(d, "selector", "") or "")
            if us not in selector_filters and ds not in selector_filters:
                return False
        if file_filters:
            u = dag.edge_ctx.nodes_by_name.get(up)
            d = dag.edge_ctx.nodes_by_name.get(dn)
            uf = Path(getattr(u, "config_path", "")).resolve(strict=False) if u else None
            df = Path(getattr(d, "config_path", "")).resolve(strict=False) if d else None
            if (uf not in file_filters) and (df not in file_filters):
                return False
        return True

    filtered = [(u, d) for (u, d) in edges if _matches_edge(u, d)]

    max_edges = int(getattr(args, "max_edges", 200) or 200)
    truncated = False
    if max_edges > 0 and len(filtered) > max_edges:
        filtered = filtered[:max_edges]
        truncated = True

    if args.format == "json":
        out_edges: list[dict[str, Any]] = []
        for u, d in filtered:
            un = dag.edge_ctx.nodes_by_name.get(u)
            downstream_node_match = dag.edge_ctx.nodes_by_name.get(d)
            rec: dict[str, Any] = {
                "upstream": u,
                "downstream": d,
            }
            if getattr(args, "with_groups", False):
                rec["upstream_group"] = getattr(un, "task_group", None)
                rec["downstream_group"] = getattr(downstream_node_match, "task_group", None)
            if getattr(args, "with_files", False):
                rec["upstream_file"] = str(getattr(un, "config_path", "")) if un else None
                rec["downstream_file"] = (
                    str(getattr(downstream_node_match, "config_path", "")) if downstream_node_match else None
                )
            if getattr(args, "with_refs", False):
                rec["upstream_ref"] = getattr(un, "ref", None)
                rec["downstream_ref"] = getattr(downstream_node_match, "ref", None)
            if getattr(args, "with_reasons", False):
                exp = explain_direct_edge(
                    dag.edge_ctx,
                    upstream_name=u,
                    downstream_name=d,
                    max_triggers=int(getattr(args, "max_triggers", 5)),
                    include_transitive_path=False,
                )
                rec["reason_kinds"] = sorted({r.kind for r in exp.reasons})
            out_edges.append(rec)

        write_json(
            {
                "root": str(dag.root_path),
                "base_path": str(dag.base_path),
                "task_count": len(dag.nodes),
                "edge_count": len(edges),
                "filtered_count": len(filtered),
                "truncated": truncated,
                "edges": out_edges,
            }
        )
        return 0

    # text output
    print(f"DAG root: {dag.root_path}")
    print(f"Base dir: {dag.base_path}")
    print(f"Tasks:    {len(dag.nodes)}")
    print(f"Edges:    {len(edges)}")
    if truncated:
        print(f"WARN: output truncated to first {max_edges} edges (use filters or --max)")

    cols = ["upstream", "downstream"]
    if getattr(args, "with_groups", False):
        cols += ["up_group", "down_group"]
    if getattr(args, "with_files", False):
        cols += ["up_file", "down_file"]
    if getattr(args, "with_refs", False):
        cols += ["up_ref", "down_ref"]
    if getattr(args, "with_reasons", False):
        cols += ["reason_kinds"]
    rows: list[list[Any]] = []

    for u, d in filtered:
        un = dag.edge_ctx.nodes_by_name.get(u)
        downstream_node_match = dag.edge_ctx.nodes_by_name.get(d)
        row: list[Any] = [u, d]
        if getattr(args, "with_groups", False):
            row += [
                getattr(un, "task_group", None) if un else None,
                getattr(downstream_node_match, "task_group", None) if downstream_node_match else None,
            ]
        if getattr(args, "with_files", False):
            row += [
                Path(getattr(un, "config_path", "")).name if un else None,
                Path(getattr(downstream_node_match, "config_path", "")).name if downstream_node_match else None,
            ]
        if getattr(args, "with_refs", False):
            row += [
                getattr(un, "ref", None) if un else None,
                getattr(downstream_node_match, "ref", None) if downstream_node_match else None,
            ]
        if getattr(args, "with_reasons", False):
            exp = explain_direct_edge(
                dag.edge_ctx,
                upstream_name=u,
                downstream_name=d,
                max_triggers=int(getattr(args, "max_triggers", 5)),
                include_transitive_path=False,
            )
            kinds = sorted({r.kind for r in exp.reasons})
            row += [",".join(kinds) if kinds else "-"]
        rows.append(row)

    print("\n" + render_table(cols, rows))
    return 0
