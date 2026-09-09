"""Markdown renderer for benchmark release readiness."""

from __future__ import annotations

from typing import Any

from tools.oss_benchmark.payload_utils import format_int


def render_release_readiness_section(payload: dict[str, Any]) -> str:
    readiness = payload.get("benchmark_release_readiness") or {}
    if not readiness:
        return ""
    return render_release_readiness_markdown(readiness, standalone=False)


def render_release_readiness_markdown(readiness: dict[str, Any], *, standalone: bool = True) -> str:
    seal = readiness.get("evidence_seal") or {}
    policy = readiness.get("freeze_policy") or {}
    stage = str(readiness.get("release_stage") or "benchmark-v3")
    lines = [
        "# Benchmark v3 Release Readiness" if standalone else "## Benchmark v3 Release Readiness",
        "",
        f"The benchmark is treated as a `{stage}` evidence product: stable metric groups keep contract compatibility, while certification, claims, budget and export layers provide release guardrails.",
        "",
        "![Benchmark release readiness](assets/oss-release-readiness-seal.svg)",
        "",
        "| Release seal | Value |",
        "|---|---|",
        f"| Status | `{readiness.get('status', 'n/a')}` |",
        f"| Seal | `{seal.get('label', 'n/a')}` |",
        f"| Score | {format_int(seal.get('score'))}/100 |",
        f"| Generated at | `{seal.get('generated_at', 'n/a')}` |",
        f"| Source revision | `{seal.get('source_revision', 'n/a')}` |",
        "",
        "### Evidence policy",
        "",
        f"- stable metric groups: `{', '.join(policy.get('stable_metric_groups') or [])}`.",
        f"- experimental metric groups: `{', '.join(policy.get('experimental_metric_groups') or [])}`.",
        f"- Policy: {policy.get('policy', 'n/a')}",
        "",
        "### Release checks",
        "",
        "| Check | Status | Evidence |",
        "|---|---|---|",
    ]
    for check in readiness.get("release_checks") or []:
        lines.append(
            f"| {check.get('label', 'n/a')} | `{check.get('status', 'n/a')}` | {check.get('evidence', 'n/a')} |"
        )
    lines.extend(["", f"Recommended action: **{readiness.get('recommended_action', 'n/a')}**", ""])
    return "\n".join(lines)
