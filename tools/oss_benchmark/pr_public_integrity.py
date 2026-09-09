"""PR-summary renderer for public evidence integrity."""

from __future__ import annotations

from typing import Any


def render_public_evidence_integrity_pr_section(payload: dict[str, Any]) -> list[str]:
    integrity = payload.get("public_evidence_integrity") or {}
    if not integrity:
        return []
    return [
        "### Public Evidence Integrity",
        "",
        f"- Status: **{integrity.get('status', 'n/a')}** with score `{integrity.get('score', 'n/a')}/100`.",
        f"- claim coverage: `{integrity.get('claim_coverage_percent', 'n/a')}%`.",
        f"- redaction violations: `{integrity.get('redaction_violation_count', 'n/a')}`.",
        "- Public artifacts use `$WORKSPACE` / `$BENCHMARK_CACHE` tokens for machine-specific paths.",
        "",
    ]
