from __future__ import annotations

from pathlib import Path
from typing import Any

from dpone.dag.edge_explain import DagEdgeContext
from dpone.output_table import render_table
from dpone.services.dag.views import ExplainNodeE2EView

from .common import HR, HR2, render_dag_banner, render_reasons_section
from .e2e_attribution import render_direct_dependency_explanations, render_group_to_group_attributions


def render_explain_node_e2e_text(view: ExplainNodeE2EView, *, edge_ctx: DagEdgeContext) -> str:
    exp = view.result
    direction = str(view.meta.options.get("direction", "both") or "both")
    output = str(view.meta.options.get("output", "compact") or "compact")

    extra: list[str] = []
    if view.focus_peer_raw:
        extra.append(f"Focus peer: {view.focus_peer_raw} -> resolved: {view.focus_peer_resolved}")

    lines: list[str] = []
    lines.append(
        render_dag_banner(
            root=view.meta.root, base_path=view.meta.base_path, task_count=view.meta.task_count, extra=extra
        )
    )

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
                lines.extend(_render_edge_full(e))
        else:
            rows = []
            for e in exp.incoming:
                pb = _peer_brief(edge_ctx, e.upstream.get("name"))
                rows.append(
                    [
                        pb.get("name"),
                        pb.get("ref") or "-",
                        pb.get("task_group") or "-",
                        _kinds(e),
                        _attribution_summary(e),
                    ]
                )
            lines.append(render_table(["upstream", "up_ref", "up_group", "reason_kinds", "attribution"], rows))

    if direction in ("out", "both"):
        lines.append("\nOutgoing edges (this -> downstream):")
        if output == "full":
            for e in exp.outgoing:
                lines.extend(_render_edge_full(e))
        else:
            rows = []
            for e in exp.outgoing:
                pb = _peer_brief(edge_ctx, e.downstream.get("name"))
                rows.append(
                    [
                        pb.get("name"),
                        pb.get("ref") or "-",
                        pb.get("task_group") or "-",
                        _kinds(e),
                        _attribution_summary(e),
                    ]
                )
            lines.append(render_table(["downstream", "down_ref", "down_group", "reason_kinds", "attribution"], rows))

    return "\n".join(lines).rstrip() + "\n"


def _peer_brief(edge_ctx: DagEdgeContext, name: str | None) -> dict[str, Any]:
    if not name:
        return {"name": "-"}
    pn = edge_ctx.nodes_by_name.get(name)
    if not pn:
        return {"name": name}
    return {
        "name": pn.name,
        "ref": pn.ref,
        "task_group": pn.task_group,
        "file": Path(pn.config_path).name,
    }


def _kinds(edge) -> str:
    kinds = sorted({str(r.get("kind")) for r in (edge.reasons or ()) if r.get("kind")})
    return ",".join(kinds) if kinds else "-"


def _attribution_summary(edge) -> str:
    parts: list[str] = []
    if getattr(edge, "direct_dep_attributions", None):
        for d in list(edge.direct_dep_attributions)[:3]:
            origin = d.compiled_origins[0].origin if getattr(d, "compiled_origins", None) else "unknown"
            parts.append(f"dep[{d.index}]@{origin}")
    if getattr(edge, "group_to_group_attributions", None):
        for g in list(edge.group_to_group_attributions)[:2]:
            parts.append(f"group:{g.dependent_group}<-{g.upstream_group}({g.trigger_count})")
    return " ".join(parts) if parts else "-"


def _render_edge_full(edge) -> list[str]:
    lines: list[str] = ["\n" + HR, f"{edge.upstream.get('name')} -> {edge.downstream.get('name')}"]

    if edge.reasons:
        lines.append(render_reasons_section(list(edge.reasons), title="Reasons", include_evidence=True))

    if getattr(edge, "direct_dep_attributions", None):
        lines.append("\n" + HR2)
        lines.append("End-to-end attribution: downstream depends_on items")
        lines.append(render_direct_dependency_explanations(edge.direct_dep_attributions))

    if getattr(edge, "group_to_group_attributions", None):
        lines.append("\n" + HR2)
        lines.append("End-to-end attribution: group-to-group triggers")
        lines.append(render_group_to_group_attributions(edge.group_to_group_attributions))

    return lines
