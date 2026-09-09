from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.services.manifest.views import ManifestStatsView


from dpone.output_table import render_table


def render_manifest_stats_text(view: ManifestStatsView) -> str:
    lines: list[str] = [
        f"Manifests: {view.total_manifests}",
        f"Processes: {view.total_processes}",
    ]
    kind_rows = [[k, v] for k, v in sorted(view.kinds.items(), key=lambda kv: (-kv[1], kv[0]))]
    lines.append("\nBy kind:")
    lines.append(render_table(["kind", "count"], kind_rows))

    ds_rows = [[k, v] for k, v in sorted(view.by_dataset.items(), key=lambda kv: (-kv[1], kv[0]))[:30]]
    lines.append("\nTop sink datasets:")
    lines.append(render_table(["sink dataset", "processes"], ds_rows))

    grp_rows = [[k, v] for k, v in sorted(view.by_group.items(), key=lambda kv: (-kv[1], kv[0]))[:30]]
    lines.append("\nTop task_groups:")
    lines.append(render_table(["task_group", "processes"], grp_rows))
    return "\n".join(lines).rstrip() + "\n"
