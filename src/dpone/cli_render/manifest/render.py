from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.services.manifest.views import ManifestRenderView


from dpone.output import dumps_yaml


def render_manifest_render_text(view: ManifestRenderView) -> str:
    lines: list[str] = []
    for idx, doc in enumerate(view.docs):
        if idx:
            lines.append("---")
        lines.append(dumps_yaml(dict(doc.config), sort_keys=False).rstrip())
    return "\n".join(lines).rstrip() + "\n"
