from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Any

from dpone.commands.output_json import write_json
from dpone.dag.edge_explain import locate_node
from dpone.dag.errors import DagConfigurationError
from dpone.dag.subgraph import extract_shortest_path_subgraph
from dpone.output_table import render_table
from dpone.output_yaml import dumps_yaml
from dpone.services.dag.load_context import load_dag_context


def register_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    p = subparsers.add_parser(
        "subgraph",
        help="Extract a minimal subgraph between two tasks (shortest path)",
    )
    p.add_argument(
        "root",
        help=("Root manifest (same entrypoint you pass to DAG builder). If no extension, .yaml is assumed."),
    )
    p.add_argument(
        "--from",
        dest="upstream",
        required=True,
        help="Upstream task: task_id, selector, #selector, file.yaml#selector",
    )
    p.add_argument(
        "--to",
        dest="downstream",
        required=True,
        help="Downstream task: task_id, selector, #selector, file.yaml#selector",
    )
    p.add_argument(
        "--all-shortest",
        action="store_true",
        help="Use the union of all shortest paths (can include more nodes/edges than a single path).",
    )
    p.add_argument(
        "--max-edges",
        type=int,
        default=500,
        help="Safety limit: max edges to include in subgraph.",
    )
    p.add_argument(
        "--max-triggers",
        type=int,
        default=10,
        help="Max trigger tasks to print for group-to-group reasons (when reasons are enabled).",
    )
    p.add_argument("--with-reasons", action="store_true", help="Attach reason kinds to edges.")
    p.add_argument(
        "--with-evidence",
        action="store_true",
        help="Include full reason evidence blocks (text/json).",
    )
    p.add_argument(
        "--base-path",
        help="Manifest directory (defaults to MANIFEST_DIR). Used for resolving relative root and refs.",
    )
    p.add_argument(
        "--format",
        choices=["text", "json", "dot", "mermaid"],
        default="text",
        help="Output format: text/json for inspection, dot/mermaid for visualization.",
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


def cmd_dag_subgraph(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    dag = load_dag_context(args, ctx=ctx)

    try:
        up_node = locate_node(dag.nodes, token=str(args.upstream), root_file=dag.root_path, base_path=dag.base_path)
        dn_node = locate_node(dag.nodes, token=str(args.downstream), root_file=dag.root_path, base_path=dag.base_path)
    except ValueError as exc:
        raise DagConfigurationError(str(exc)) from exc

    with_reasons = bool(getattr(args, "with_reasons", False) or getattr(args, "with_evidence", False))

    sg = extract_shortest_path_subgraph(
        dag.edge_ctx,
        source=up_node.name,
        target=dn_node.name,
        all_shortest=bool(getattr(args, "all_shortest", False)),
        with_reasons=with_reasons,
        with_evidence=bool(getattr(args, "with_evidence", False)),
        max_triggers=int(getattr(args, "max_triggers", 10) or 10),
        max_edges=int(getattr(args, "max_edges", 500) or 500),
    )

    if args.format == "json":
        nodes_by_name = {n.name: n for n in dag.nodes}
        out = sg.to_jsonable()
        out["root"] = str(dag.root_path)
        out["base_path"] = str(dag.base_path)
        out["from"] = {"name": up_node.name, "ref": up_node.ref}
        out["to"] = {"name": dn_node.name, "ref": dn_node.ref}
        out["node_meta"] = {
            nm: {
                "ref": nodes_by_name[nm].ref,
                "task_group": nodes_by_name[nm].task_group,
                "file": str(Path(nodes_by_name[nm].config_path).name),
            }
            for nm in out.get("nodes", [])
            if nm in nodes_by_name
        }
        write_json(out)
        return 0 if out.get("nodes") else 1

    if args.format in ("dot", "mermaid"):
        if not sg.nodes:
            print("# No path found")
            return 1

        if args.format == "dot":
            print("digraph dpone {")
            print("  rankdir=LR;")
            for nm in sg.nodes:
                print(f'  "{nm}";')
            for e in sg.edges:
                label = ",".join(e.reason_kinds()) if (with_reasons and e.reason_kinds()) else ""
                if label:
                    print(f'  "{e.upstream}" -> "{e.downstream}" [label="{label}"]; ')
                else:
                    print(f'  "{e.upstream}" -> "{e.downstream}";')
            print("}")
            return 0

        # mermaid
        id_map: dict[str, str] = {nm: f"N{i + 1}" for i, nm in enumerate(sg.nodes)}
        print("graph TD")
        for nm in sg.nodes:
            nid = id_map[nm]
            print(f'  {nid}["{nm}"]')
        for e in sg.edges:
            a = id_map[e.upstream]
            b = id_map[e.downstream]
            label = ",".join(e.reason_kinds()) if (with_reasons and e.reason_kinds()) else ""
            if label:
                print(f"  {a} -->|{label}| {b}")
            else:
                print(f"  {a} --> {b}")
        return 0

    # text output
    print(f"DAG root: {dag.root_path}")
    print(f"Base dir: {dag.base_path}")
    print(f"Tasks:    {len(dag.nodes)}")
    print(f"From:     {up_node.name}")
    print(f"To:       {dn_node.name}")
    if sg.shortest_distance is not None:
        print(f"Distance: {sg.shortest_distance}")
    if sg.warnings:
        for w in sg.warnings:
            print(f"WARN: {w}")

    if not sg.nodes:
        print("\nNo path found.")
        return 1

    node_headers = ["node", "task_group", "file", "ref"]
    node_rows: list[list[Any]] = []
    for nm in sg.nodes:
        pn = dag.edge_ctx.nodes_by_name.get(nm)
        node_rows.append(
            [
                nm,
                getattr(pn, "task_group", None) or "-",
                Path(getattr(pn, "config_path", "")).name if pn else "-",
                getattr(pn, "ref", None) or "-",
            ]
        )
    print("\nNodes:")
    print(render_table(node_headers, node_rows))

    cols = ["upstream", "downstream"]
    if with_reasons:
        cols.append("reason_kinds")
    edge_rows: list[list[Any]] = []
    for e in sg.edges:
        row: list[Any] = [e.upstream, e.downstream]
        if with_reasons:
            row.append(",".join(e.reason_kinds()) if e.reason_kinds() else "-")
        edge_rows.append(row)
    print("\nEdges:")
    print(render_table(cols, edge_rows))

    if getattr(args, "with_evidence", False):
        for e in sg.edges:
            if not e.reasons:
                continue
            print("\n" + "-" * 80)
            print(f"{e.upstream} -> {e.downstream}")
            for rr in e.reasons:
                print(f"\n[{rr.kind}] {rr.description}")
                if rr.evidence:
                    print(dumps_yaml(rr.evidence, sort_keys=True).rstrip())

    return 0
