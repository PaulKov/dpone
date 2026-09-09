from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.services.manifest.views import ManifestListView


from dpone.output_table import render_table


def render_manifest_list_text(view: ManifestListView) -> str:
    if not view.rows:
        return "No YAML files found.\n"
    rows = [[r.manifest, r.kind, r.selector, r.name, r.task_group, r.source, r.sink] for r in view.rows]
    return render_table(["manifest", "kind", "selector", "name", "task_group", "source", "sink"], rows) + "\n"
