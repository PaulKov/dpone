from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.services.dag.views import ExplainEdgeE2EView


from dpone.services.dag.path_view import group_consecutive, reasons_signature

from .common import (
    HR,
    HR2,
    render_dag_banner,
    render_node_block,
    render_reasons_compact,
    render_reasons_section,
    render_warnings,
)
from .e2e_attribution import compact_yaml, render_direct_dependency_explanations, render_group_to_group_attributions


def render_edge_e2e_text(view: ExplainEdgeE2EView) -> str:
    result = view.result
    path_view = str(view.meta.options.get("path_view", "edges") or "edges")
    path_output = str(view.meta.options.get("path_output", "compact") or "compact")

    lines: list[str] = []
    lines.append(render_dag_banner(root=view.meta.root, base_path=view.meta.base_path, task_count=view.meta.task_count))

    lines.append(render_node_block("Upstream", result.upstream))
    lines.append(render_node_block("Downstream", result.downstream))

    lines.append("\nEdge:")
    lines.append(f"  {view.upstream_name} -> {view.downstream_name}")
    lines.append(f"  direct_edge: {result.direct_edge}")

    if result.warnings:
        w = render_warnings(result.warnings)
        if w:
            lines.append(w)

    if result.reasons:
        lines.append(render_reasons_section(list(result.reasons), title="Reasons", include_evidence=True))

    if not result.direct_edge:
        if result.path:
            lines.append("\nShortest path (no direct edge):")
            lines.append("  " + " -> ".join(list(result.path)))

        if view.path_edges:
            lines.append("\nPath edge explanations (end-to-end):")

            if path_view == "grouped":
                groups = group_consecutive(view.path_edges, key_fn=lambda e: reasons_signature(e.reasons))
                for idx, (_k, seg) in enumerate(groups, start=1):
                    nodes_chain = [str(seg[0].upstream.get("name") or "-")] + [
                        str(x.downstream.get("name") or "-") for x in seg
                    ]
                    primary = seg[0].reasons[0] if seg[0].reasons else None
                    lines.append("\n" + HR2)
                    lines.append(f"Segment {idx}/{len(groups)} (edges={len(seg)}):")
                    lines.append("  " + " -> ".join(nodes_chain))
                    if primary:
                        lines.append(f"  - {primary.get('kind')}: {primary.get('description')}")

                    if path_output == "full":
                        for edge in seg:
                            lines.extend(_render_edge_full(edge))
            else:
                for edge in view.path_edges:
                    if path_output == "full":
                        lines.extend(_render_edge_full(edge))
                    else:
                        lines.extend(_render_edge_compact(edge))

        return "\n".join(lines).rstrip() + "\n"

    if result.direct_dep_attributions:
        lines.append("\n" + HR2)
        lines.append("End-to-end attribution: downstream depends_on items")
        lines.append(render_direct_dependency_explanations(result.direct_dep_attributions))

    if result.group_to_group_attributions:
        lines.append("\n" + HR2)
        lines.append("End-to-end attribution: group-to-group expansion triggers")
        lines.append(render_group_to_group_attributions(result.group_to_group_attributions))

    return "\n".join(lines).rstrip() + "\n"


def _render_edge_compact(edge) -> list[str]:
    lines: list[str] = ["\n" + HR, f"{edge.upstream.get('name')} -> {edge.downstream.get('name')}"]
    lines.append(render_reasons_compact(list(edge.reasons)))

    if edge.direct_dep_attributions:
        lines.append("  Attribution (depends_on):")
        for d in edge.direct_dep_attributions:
            lines.append(f"    depends_on[{d.index}]: {compact_yaml(d.compiled_item)}")
            for o in d.compiled_origins:
                lines.append(f"      origin: {o.origin}  raw_path: {o.path}")
            if d.parsed_dependency is not None:
                lines.append(f"      parsed: {compact_yaml(d.parsed_dependency)}")
            for r in d.parse_records:
                lines.append(f"      parse: {r.get('operation')} -> {r.get('target')}")

    if edge.group_to_group_attributions:
        lines.append("  Attribution (group-to-group triggers):")
        for g in edge.group_to_group_attributions:
            lines.append(
                f"    group '{g.dependent_group}' depends on '{g.upstream_group}' (triggers={g.trigger_count})"
            )
            for tr in g.triggers:
                origin = tr.compiled_origins[0].origin if getattr(tr, "compiled_origins", None) else "unknown"
                lines.append(
                    f"      trigger: {tr.trigger_task.get('name')} depends_on[{tr.depends_on_index}] origin={origin}"
                )

    return lines


def _render_edge_full(edge) -> list[str]:
    lines: list[str] = ["\n" + HR, f"{edge.upstream.get('name')} -> {edge.downstream.get('name')}"]

    if edge.reasons:
        lines.append(render_reasons_section(list(edge.reasons), title="Reasons", include_evidence=True))

    if edge.direct_dep_attributions:
        lines.append("\n" + HR2)
        lines.append("End-to-end attribution: downstream depends_on items")
        lines.append(render_direct_dependency_explanations(edge.direct_dep_attributions))

    if edge.group_to_group_attributions:
        lines.append("\n" + HR2)
        lines.append("End-to-end attribution: group-to-group expansion triggers")
        lines.append(render_group_to_group_attributions(edge.group_to_group_attributions))

    return lines
