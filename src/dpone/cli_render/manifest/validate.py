from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.services.manifest.views import ManifestValidateView


def render_manifest_validate_text(view: ManifestValidateView) -> str:
    if not view.issues:
        return "OK: no issues\n"
    lines: list[str] = []
    for issue in view.issues:
        selector = f"#{issue.selector}" if issue.selector else ""
        lines.append(f"{issue.severity.value} {issue.code} {issue.manifest_path.name}{selector}: {issue.message}")
    return "\n".join(lines).rstrip() + "\n"
