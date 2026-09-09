from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.services.manifest.views import ManifestMigrateView


def render_manifest_migrate_text(view: ManifestMigrateView, *, dry_run: bool) -> str:
    lines: list[str] = [
        f"Legacy processes: {view.total_legacy}",
        f"Batch manifests: {len(view.plan.batches)}",
    ]
    for batch in view.plan.batches:
        lines.append(f"- {batch.out_path}: {len(batch.processes)} processes")
    if dry_run:
        lines.append("(dry-run: files were NOT written)")
    return "\n".join(lines).rstrip() + "\n"
