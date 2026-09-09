"""Provider-neutral schema migration PR/MR review rendering."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

REVIEW_SCHEMA = "dpone.schema_migration_review.v1"


class MigrationReviewRenderer:
    """Renders deterministic PR/MR review evidence without SCM API calls."""

    def render_payload(self, bundle: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "schema_version": REVIEW_SCHEMA,
            "status": bundle.get("status"),
            "bundle_id": bundle.get("bundle_id"),
            "pack_id": bundle.get("pack_id"),
            "target": dict(bundle.get("target", {})) if isinstance(bundle.get("target"), Mapping) else {},
            "summary": dict(bundle.get("summary", {})) if isinstance(bundle.get("summary"), Mapping) else {},
            "blockers": list(bundle.get("blockers", [])) if isinstance(bundle.get("blockers"), list) else [],
            "warnings": list(bundle.get("warnings", [])) if isinstance(bundle.get("warnings"), list) else [],
            "markdown": self.render_markdown(bundle),
        }

    def render_markdown(self, bundle: Mapping[str, Any]) -> str:
        summary = bundle.get("summary", {}) if isinstance(bundle.get("summary"), Mapping) else {}
        target = bundle.get("target", {}) if isinstance(bundle.get("target"), Mapping) else {}
        lines = [
            "# Schema Migration Review",
            "",
            f"- status: {bundle.get('status')}",
            f"- pack_id: {bundle.get('pack_id')}",
            f"- bundle_id: {bundle.get('bundle_id')}",
            f"- target: {target.get('sink_type')}.{target.get('table')}",
            f"- strategy: {summary.get('strategy')}",
            f"- changes_count: {summary.get('changes_count', 0)}",
            f"- blockers_count: {summary.get('blockers_count', 0)}",
            "",
            "## Impact and approvals",
            "",
        ]
        required = summary.get("required_approvals", [])
        lines.append(
            "- required approvals: " + (", ".join(required) if isinstance(required, list) and required else "none")
        )
        if summary.get("approved_by"):
            lines.append(f"- approved_by: {summary.get('approved_by')}")
        if summary.get("from_environment") or summary.get("to_environment"):
            lines.append(f"- environment chain: {summary.get('from_environment')} -> {summary.get('to_environment')}")
        lines.extend(_markdown_list("Blockers", bundle.get("blockers", [])))
        lines.extend(_markdown_list("Warnings", bundle.get("warnings", [])))
        lines.extend(
            [
                "## Next CI commands",
                "",
                "```bash",
                "dpone schema migration bundle verify --bundle bundle.json --require-attestation --format json",
                "dpone schema migration review render --bundle bundle.json --format md --output review.md",
                "```",
                "",
            ]
        )
        return "\n".join(lines)


def _markdown_list(title: str, raw: object) -> list[str]:
    values = [str(item) for item in raw] if isinstance(raw, list) else []
    lines = ["", f"## {title}", ""]
    lines.extend(f"- {item}" for item in values) if values else lines.append("- none")
    lines.append("")
    return lines


__all__ = ["MigrationReviewRenderer", "REVIEW_SCHEMA"]
