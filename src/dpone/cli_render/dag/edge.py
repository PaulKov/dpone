from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.services.dag.views import ExplainEdgeView


from dpone.services.dag.path_view import group_consecutive, reasons_signature

from .common import (
    HR,
    render_dag_banner,
    render_node_block,
    render_reasons_compact,
    render_reasons_section,
    render_warnings,
)


def render_explain_edge_text(view: ExplainEdgeView) -> str:
    exp = view.result
    lines: list[str] = []

    lines.append(
        render_dag_banner(
            root=view.meta.root,
            base_path=view.meta.base_path,
            task_count=view.meta.task_count,
        )
    )

    lines.append(render_node_block("Upstream", exp.upstream))
    lines.append(render_node_block("Downstream", exp.downstream))

    lines.append("\nEdge:")
    lines.append(f"  {view.upstream_name} -> {view.downstream_name}")
    lines.append(f"  direct_edge: {exp.direct_edge}")

    if exp.warnings:
        w = render_warnings(exp.warnings)
        if w:
            lines.append(w)

    if exp.reasons:
        lines.append(render_reasons_section(exp.reasons, title="Reasons", include_evidence=True))

    if exp.path:
        lines.append("\nShortest path (no direct edge):")
        lines.append("  " + " -> ".join(list(exp.path)))

    if view.path_edges:
        lines.append("\nPath edge explanations:")
        path_view = str(view.meta.options.get("path_view", "edges") or "edges")
        path_output = str(view.meta.options.get("path_output", "compact") or "compact")

        if path_view == "grouped":
            groups = group_consecutive(view.path_edges, key_fn=lambda x: reasons_signature(x.result.reasons))

            for idx, (_k, seg) in enumerate(groups, start=1):
                nodes_chain = [seg[0].upstream_name] + [x.downstream_name for x in seg]
                first_e = seg[0].result
                lines.append("\n" + "=" * 80)
                lines.append(f"Segment {idx}/{len(groups)} (edges={len(seg)}):")
                lines.append("  " + " -> ".join(nodes_chain))
                if first_e.reasons:
                    for rr in first_e.reasons:
                        if isinstance(rr, dict):
                            kind = rr.get("kind")
                            description = rr.get("description")
                        else:
                            kind = getattr(rr, "kind", "")
                            description = getattr(rr, "description", "")
                        lines.append(f"  - {kind}: {description}")

                if path_output == "full":
                    for step in seg:
                        lines.extend(_render_edge_step_full(step))
        else:
            for step in view.path_edges:
                if path_output == "full":
                    lines.extend(_render_edge_step_full(step))
                else:
                    lines.extend(_render_edge_step_compact(step))

    return "\n".join(lines).rstrip() + "\n"


def _render_edge_step_compact(step) -> list[str]:
    out: list[str] = ["\n" + HR, f"{step.upstream_name} -> {step.downstream_name}"]
    if step.result.reasons:
        out.append(render_reasons_compact(step.result.reasons))
    return out


def _render_edge_step_full(step) -> list[str]:
    out: list[str] = ["\n" + HR, f"{step.upstream_name} -> {step.downstream_name}"]
    if step.result.reasons:
        out.append(render_reasons_section(step.result.reasons, title="Reasons", include_evidence=True))
    return out
