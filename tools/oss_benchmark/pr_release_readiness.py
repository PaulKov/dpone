"""PR-summary renderer for benchmark release readiness."""

from __future__ import annotations

from typing import Any


def render_release_readiness_pr_section(payload: dict[str, Any]) -> list[str]:
    readiness = payload.get("benchmark_release_readiness") or {}
    if not readiness:
        return []
    seal = readiness.get("evidence_seal") or {}
    failed = [check for check in readiness.get("release_checks") or [] if check.get("status") == "failed"]
    return [
        "### Benchmark v3 Release Readiness",
        "",
        f"- Status: **{readiness.get('status', 'n/a')}**.",
        f"- Seal: `{seal.get('label', 'n/a')}` with score `{seal.get('score', 'n/a')}/100`.",
        f"- Failed checks: `{len(failed)}`.",
        f"- Recommended action: {readiness.get('recommended_action', 'n/a')}",
        "",
    ]
