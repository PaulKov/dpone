"""PR-summary renderer for source citation verification."""

from __future__ import annotations

from typing import Any


def render_source_verification_pr_section(payload: dict[str, Any]) -> list[str]:
    verification = payload.get("source_verification") or {}
    if not verification:
        return []
    summary = verification.get("summary") or {}
    return [
        "### Source Citation Verification",
        "",
        f"- Status: **{verification.get('status', 'n/a')}**.",
        f"- Source health: `{summary.get('source_health_score', 'n/a')}/100`; "
        f"claim traceability: `{summary.get('claim_traceability_percent', 'n/a')}%`.",
        f"- Sources: `{summary.get('verified_count', 0)}` verified, "
        f"`{summary.get('stale_count', 0)}` stale, "
        f"`{summary.get('unavailable_count', 0)}` unavailable.",
        "- Stale source values keep prior content hashes and last successful update timestamps.",
        "",
    ]
