"""Markdown rendering for benchmark public claims."""

from __future__ import annotations

from typing import Any


def render_claims_ledger_section(payload: dict[str, Any]) -> str:
    ledger = payload.get("claims_ledger") or {}
    claims = list(ledger.get("claims") or [])
    if not claims:
        return ""
    summary = ledger.get("summary") or {}
    lines = [
        "## Claims Ledger",
        "",
        "Every public benchmark claim below is resolved against raw JSON evidence, generated artifacts, or public sources. Claims without evidence are not rendered as verified sales statements.",
        "",
        (
            f"Verified `{summary.get('verified', 0)}`, stale `{summary.get('stale', 0)}`, "
            f"unverified `{summary.get('unverified', 0)}`."
        ),
        "",
        "| Claim | Type | Status | Confidence | Freshness | Gate impact | Evidence |",
        "|---|---:|---:|---:|---:|---:|---|",
    ]
    for claim in claims:
        evidence = "<br>".join(f"`{ref}`" for ref in claim.get("evidence_refs") or [])
        lines.append(
            "| "
            f"{claim.get('title', 'n/a')} | "
            f"`{claim.get('claim_type', 'n/a')}` | "
            f"`{claim.get('status', 'n/a')}` | "
            f"{claim.get('confidence', 0)} | "
            f"`{claim.get('freshness', 'n/a')}` | "
            f"`{claim.get('gate_impact', 'n/a')}` | "
            f"{evidence} |"
        )
    lines.append("")
    return "\n".join(lines)
