from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.services.dag.views import ExplainDependenciesView


from pathlib import Path

from dpone.output import dumps_yaml
from dpone.output_table import render_table

from .common import HR, HR2, render_dag_banner, render_node_block, render_warnings
from .e2e_attribution import render_direct_dependency_explanations


def render_explain_deps_text(view: ExplainDependenciesView) -> str:
    result = view.result
    lines: list[str] = []

    lines.append(render_dag_banner(root=view.meta.root, base_path=view.meta.base_path, task_count=view.meta.task_count))

    lines.append(render_node_block("Downstream", result.downstream))

    if result.warnings:
        w = render_warnings(result.warnings)
        if w:
            lines.append(w)

    if not result.direct:
        lines.append("\nNo direct depends_on entries.")
    else:
        lines.append("\nDirect depends_on (end-to-end):")
        lines.append(render_direct_dependency_explanations(result.direct))

    if result.inherited_group_deps:
        lines.append("\n" + HR2)
        lines.append("Inherited TaskGroup dependencies (group-to-group expansion):")

        for g in result.inherited_group_deps:
            lines.append("\n" + HR)
            lines.append(f"Group '{g.dependent_group}' depends on '{g.upstream_group}'")

            for w in getattr(g, "warnings", ()) or ():
                lines.append(f"WARN: {w}")

            lines.append(f"Upstream tasks: {len(g.upstream_tasks)}")
            if g.upstream_tasks:
                preview = list(g.upstream_tasks)[:10]
                lines.append("  " + ", ".join(preview) + (" ..." if len(g.upstream_tasks) > 10 else ""))
            lines.append(f"Triggers in dependent group: {g.trigger_count}")

            if g.triggers:
                rows = []
                for tr in g.triggers:
                    rows.append(
                        [
                            tr.get("task_name"),
                            tr.get("dep_index"),
                            tr.get("origin", "-"),
                            Path(str(tr.get("manifest") or "-")).name if tr.get("manifest") else "-",
                        ]
                    )
                lines.append("\nTriggers (first N):")
                lines.append(render_table(["trigger_task", "dep_index", "origin", "manifest"], rows))

                for tr in g.triggers:
                    ci = tr.get("compiled_item")
                    if ci is None:
                        continue
                    lines.append(f"\nTrigger {tr.get('task_name')} depends_on[{tr.get('dep_index')}]:")
                    lines.append(dumps_yaml(ci, sort_keys=True).rstrip())

    return "\n".join(lines).rstrip() + "\n"
