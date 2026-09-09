"""Markdown renderer for public evidence-integrity evidence."""

from __future__ import annotations

from typing import Any

from tools.oss_benchmark.payload_utils import format_int


def render_public_evidence_integrity_section(payload: dict[str, Any]) -> str:
    integrity = payload.get("public_evidence_integrity") or {}
    if not integrity:
        return ""
    lines = [
        "",
        "## Public Evidence Integrity",
        "",
        "This section verifies that public benchmark artifacts avoid machine-specific paths and that customer-facing claims have source-backed evidence.",
        "",
        "![Public evidence integrity](assets/oss-public-evidence-integrity.svg)",
        "",
        "| Integrity check | Value |",
        "|---|---:|",
        f"| Status | `{integrity.get('status', 'n/a')}` |",
        f"| Score | {format_int(integrity.get('score'))} |",
        f"| Claim coverage | {format_int(integrity.get('claim_coverage_percent'))}% |",
        f"| redaction violations | {format_int(integrity.get('redaction_violation_count'))} |",
        "",
        "### claim evidence ledger",
        "",
        "| Claim | Section | Confidence | Source | Status |",
        "|---|---|---|---|---|",
    ]
    for claim in (integrity.get("claim_evidence") or [])[:12]:
        lines.append(
            "| "
            f"`{claim.get('claim_id', 'n/a')}` | "
            f"{claim.get('section', 'n/a')} | "
            f"`{claim.get('confidence', 'n/a')}` | "
            f"`{claim.get('source', 'n/a')}` | "
            f"`{claim.get('status', 'n/a')}` |"
        )
    lines.extend(
        [
            "",
            "Public evidence policy: raw JSON, Markdown, SVG and PR artifacts use stable path tokens such as `$WORKSPACE` and `$BENCHMARK_CACHE` instead of local filesystem paths.",
            "",
        ]
    )
    return "\n".join(lines)
