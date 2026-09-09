from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.services.manifest.views import ManifestRegistryLintView


def render_manifest_registry_lint_text(view: ManifestRegistryLintView) -> str:
    if not view.issues:
        return "OK: registry covers all used sources\n"
    lines: list[str] = []
    for issue in view.issues:
        key = ""
        if issue.src_system and issue.src_database:
            key = f" ({issue.src_system}::{issue.src_database})"
        lines.append(f"{issue.severity.value} {issue.code} {issue.manifest_path.name}{key}: {issue.message}")
    return "\n".join(lines).rstrip() + "\n"
