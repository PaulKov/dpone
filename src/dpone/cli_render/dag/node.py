from __future__ import annotations

from pathlib import Path
from typing import Any

from dpone.dag.edge_explain import DagEdgeContext, EdgeReason
from dpone.output import dumps_yaml
from dpone.output_table import render_table
from dpone.services.dag.views import ExplainNodeView

from .common import HR, render_dag_banner


def render_explain_node_text(view: ExplainNodeView, *, edge_ctx: DagEdgeContext) -> str:
    exp = view.result
    direction = str(view.meta.options.get("direction", "both") or "both")
    output = str(view.meta.options.get("output", "compact") or "compact")

    lines: list[str] = []
    lines.append(render_dag_banner(root=view.meta.root, base_path=view.meta.base_path, task_count=view.meta.task_count))

    n = exp.node
    lines.append("\nTask:")
    lines.append(f"  name:       {n.get('name')}")
    lines.append(f"  ref:        {n.get('ref')}")
    if n.get("task_group"):
        lines.append(f"  task_group: {n.get('task_group')}")
    if n.get("source"):
        lines.append(f"  source:     {n.get('source')}")
    if n.get("sink"):
        lines.append(f"  sink:       {n.get('sink')}")

    for w in exp.warnings or ():
        lines.append(f"WARN: {w}")

    if direction in ("in", "both"):
        lines.append("\nIncoming edges (upstream -> this):")
        if output == "full":
            for e in exp.incoming:
                lines.extend(_render_edge_full(e.upstream, e.downstream, list(e.reasons)))
        else:
            rows = []
            for e in exp.incoming:
                pb = _peer_brief(edge_ctx, e.upstream)
                rows.append(
                    [
                        pb.get("name"),
                        pb.get("ref") or "-",
                        pb.get("task_group") or "-",
                        ",".join(e.reason_kinds()) if e.reason_kinds() else "-",
                    ]
                )
            lines.append(render_table(["upstream", "up_ref", "up_group", "reason_kinds"], rows))

    if direction in ("out", "both"):
        lines.append("\nOutgoing edges (this -> downstream):")
        if output == "full":
            for e in exp.outgoing:
                lines.extend(_render_edge_full(e.upstream, e.downstream, list(e.reasons)))
        else:
            rows = []
            for e in exp.outgoing:
                pb = _peer_brief(edge_ctx, e.downstream)
                rows.append(
                    [
                        pb.get("name"),
                        pb.get("ref") or "-",
                        pb.get("task_group") or "-",
                        ",".join(e.reason_kinds()) if e.reason_kinds() else "-",
                    ]
                )
            lines.append(render_table(["downstream", "down_ref", "down_group", "reason_kinds"], rows))

    return "\n".join(lines).rstrip() + "\n"


def _peer_brief(edge_ctx: DagEdgeContext, name: str) -> dict[str, Any]:
    pn = edge_ctx.nodes_by_name.get(name)
    if not pn:
        return {"name": name}
    return {
        "name": pn.name,
        "ref": pn.ref,
        "task_group": pn.task_group,
        "file": Path(pn.config_path).name,
    }


def _render_edge_full(u: str, d: str, reasons: list[EdgeReason]) -> list[str]:
    out: list[str] = ["\n" + HR, f"{u} -> {d}"]
    if not reasons:
        out.append("(no attributed reasons)")
        return out

    rows = [[rr.kind, rr.description] for rr in reasons]
    out.append("\nReasons:")
    out.append(render_table(["kind", "description"], rows))

    for rr in reasons:
        if rr.evidence:
            out.append(f"\n[{rr.kind}] evidence:")
            out.append(dumps_yaml(rr.evidence, sort_keys=True).rstrip())

    return out
