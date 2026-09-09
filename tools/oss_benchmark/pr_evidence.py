"""PR-summary renderer for evidence trust."""

from __future__ import annotations

from typing import Any


def render_evidence_trust_pr_section(payload: dict[str, Any]) -> list[str]:
    """Render a compact auditability summary for benchmark refresh PRs."""

    trust = payload.get("evidence_trust") or {}
    if not trust:
        return []
    cross_check = trust.get("cross_check") or {}
    lines = [
        "### Evidence Trust & Auditability",
        "",
        f"- Evidence confidence: **{trust.get('overall_confidence_score', 'n/a')}/100** "
        f"(`{trust.get('overall_band', 'n/a')}`)",
        f"- Provenance ledger: `{trust.get('provenance_path', 'docs/benchmarks/data/oss-benchmark-provenance.json')}`",
        "- Checksum manifest: `SHA-256` artifact checksums are written to `oss-benchmark-provenance.json`.",
        f"- Optional LOC/SLOC cross-check: `{cross_check.get('status', 'n/a')}` via "
        f"`{cross_check.get('tool') or 'not installed'}`.",
        "",
    ]
    stale_projects = [
        item.get("name", slug)
        for slug, item in (trust.get("projects") or {}).items()
        if int(item.get("stale_groups") or 0) or int(item.get("unavailable_groups") or 0)
    ]
    if stale_projects:
        lines.append(f"- Stale/unavailable evidence retained for: {', '.join(stale_projects)}.")
        lines.append("")
    return lines
