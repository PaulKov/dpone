from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.services.manifest.views import ManifestVerifyView


def render_manifest_verify_text(view: ManifestVerifyView) -> str:
    report = view.report
    lines: list[str] = [
        f"Legacy processes: {report.total_legacy}",
        f"OK: {report.ok}",
        f"FAILED: {report.failed}",
    ]
    if report.issues:
        for issue in report.issues:
            ref = f" -> {issue.batch_ref}" if issue.batch_ref else ""
            selector = f"#{issue.selector}" if issue.selector else ""
            lines.append(f"{issue.code} {issue.legacy_path.name}{selector}{ref}: {issue.message}")
            for diff in issue.diffs:
                lines.append(f"  - {diff.path}: legacy={diff.legacy!r} batch={diff.batch!r}")
    return "\n".join(lines).rstrip() + "\n"
