"""Markdown renderer for source citation verification."""

from __future__ import annotations

from typing import Any

from tools.oss_benchmark.payload_utils import format_int


def render_source_verification_section(payload: dict[str, Any]) -> str:
    """Render source health and claim-to-source traceability."""

    verification = payload.get("source_verification") or {}
    if not verification:
        return ""
    summary = verification.get("summary") or {}
    lines = [
        "",
        "## Source Citation Verification",
        "",
        "This section verifies the source registry behind public benchmark claims. URL and local source references are checked before publication; source health and stale source values stay visible instead of being overwritten by a failed refresh.",
        "",
        "![Source citation verification](assets/oss-source-verification.svg)",
        "",
        "| Source health | Value |",
        "|---|---:|",
        f"| Status | `{verification.get('status', 'n/a')}` |",
        f"| Source health score | {format_int(summary.get('source_health_score'))} |",
        f"| Claim traceability | {format_int(summary.get('claim_traceability_percent'))}% |",
        f"| Verified sources | {format_int(summary.get('verified_count'))} |",
        f"| Stale sources | {format_int(summary.get('stale_count'))} |",
        f"| Unavailable sources | {format_int(summary.get('unavailable_count'))} |",
        "",
        "### claim-to-source matrix",
        "",
        "| Claim | Sources | Status |",
        "|---|---:|---|",
    ]
    for claim in (verification.get("claim_matrix") or [])[:12]:
        lines.append(
            "| "
            f"`{claim.get('claim_id', 'n/a')}` | "
            f"{format_int(claim.get('source_count'))} | "
            f"`{claim.get('status', 'n/a')}` |"
        )
    lines.extend(
        [
            "",
            "### Source registry",
            "",
            "| Source | Type | Status | Claims | Last updated | Mode |",
            "|---|---|---|---:|---|---|",
        ]
    )
    for source in (verification.get("sources") or [])[:16]:
        lines.append(
            "| "
            f"{_source_link(source)} | "
            f"`{source.get('source_type', 'n/a')}` | "
            f"`{source.get('status', 'n/a')}` | "
            f"{format_int(len(source.get('linked_claim_ids') or []))} | "
            f"`{source.get('last_updated_at') or 'n/a'}` | "
            f"`{source.get('verification_mode', 'n/a')}` |"
        )
    lines.extend(
        [
            "",
            "Raw source verification evidence is stored under `source_verification` in `docs/benchmarks/data/oss-code-quality-benchmark-2026-06-12.json`.",
            "",
        ]
    )
    return "\n".join(lines)


def _source_link(source: dict[str, Any]) -> str:
    value = str(source.get("value") or "n/a")
    label = value if len(value) <= 72 else f"{value[:69]}..."
    if value.startswith(("http://", "https://")):
        return f"[{label}]({value})"
    return f"`{label}`"
