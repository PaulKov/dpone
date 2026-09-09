"""Markdown renderer for evidence confidence and provenance."""

from __future__ import annotations

from typing import Any

from tools.oss_benchmark.payload_utils import format_int, get_value


def render_evidence_trust_section(payload: dict[str, Any]) -> str:
    """Render the auditability section for the public benchmark."""

    trust = payload.get("evidence_trust") or {}
    if not trust:
        return ""
    lines = [
        "",
        "## Evidence Trust & Auditability",
        "",
        "Evidence confidence separates raw measurements from derived scores and inferred market posture. The benchmark is built for self-service review: every refresh publishes a provenance ledger, artifact checksums and an optional LOC/SLOC cross-check result.",
        "",
        "![Evidence confidence](assets/oss-evidence-confidence.svg)",
        "",
        f"Overall evidence confidence: **{format_int(trust.get('overall_confidence_score'))}/100** (`{trust.get('overall_band', 'n/a')}`).",
        "",
        "| Project | Confidence | Band | Fresh groups | Stale groups | Unavailable groups |",
        "|---|---:|---|---:|---:|---:|",
    ]
    for item in (trust.get("projects") or {}).values():
        lines.append(
            "| "
            f"{item.get('name', 'n/a')} | "
            f"{format_int(item.get('confidence_score'))} | "
            f"{item.get('band', 'n/a')} | "
            f"{format_int(item.get('fresh_groups'))} | "
            f"{format_int(item.get('stale_groups'))} | "
            f"{format_int(item.get('unavailable_groups'))} |"
        )
    lines.extend(_render_mode_counts(trust))
    lines.extend(_render_provenance_ledger(trust))
    lines.extend(_render_reproducibility_manifest(trust))
    return "\n".join(lines)


def _render_mode_counts(trust: dict[str, Any]) -> list[str]:
    lines = [
        "",
        "### Measured vs derived vs inferred",
        "",
        "| Evidence mode | Metric groups | Meaning |",
        "|---|---:|---|",
    ]
    meanings = {
        "measured": "direct source-tree collection or static dependency analysis",
        "derived": "deterministic score calculated from measured evidence",
        "inferred": "public documentation or repository signal interpreted through a fixed rubric",
        "stale": "previous metric retained after a failed refresh",
        "closed-core note": "closed-core comparator posture where source code is unavailable",
    }
    for item in trust.get("mode_counts") or []:
        mode = str(item.get("mode", "n/a"))
        lines.append(f"| `{mode}` | {format_int(item.get('count'))} | {meanings.get(mode, 'n/a')} |")
    return lines


def _render_provenance_ledger(trust: dict[str, Any]) -> list[str]:
    path = str(trust.get("provenance_path") or "docs/benchmarks/data/oss-benchmark-provenance.json")
    lines = [
        "",
        "### Metric provenance ledger",
        "",
        f"Raw provenance is published at `{path}` ([open ledger](data/oss-benchmark-provenance.json)).",
        "",
        "| Metric group | Mode | Collector | Formula |",
        "|---|---|---|---|",
    ]
    for item in (trust.get("metric_provenance") or [])[:12]:
        lines.append(
            "| "
            f"`{item.get('metric_group', 'n/a')}` | "
            f"`{item.get('mode', 'n/a')}` | "
            f"`{item.get('collector', 'n/a')}` | "
            f"`{item.get('formula_version', 'n/a')}` |"
        )
    return lines


def _render_reproducibility_manifest(trust: dict[str, Any]) -> list[str]:
    cross_check = trust.get("cross_check") or {}
    status = cross_check.get("status", "n/a")
    tool = cross_check.get("tool") or "not installed"
    reason = cross_check.get("reason") or _cross_check_delta(cross_check)
    return [
        "",
        "### Reproducibility manifest",
        "",
        "The provenance ledger records Python/platform metadata, analyzer schema versions and SHA-256 checksums for generated benchmark artifacts. This makes the public markdown, SVG assets and JSON evidence independently traceable to the same refresh run.",
        "",
        "| Control | Value |",
        "|---|---|",
        f"| Checksum manifest | `{trust.get('provenance_path', 'docs/benchmarks/data/oss-benchmark-provenance.json')}` |",
        f"| Optional LOC/SLOC cross-check | `{status}` via `{tool}` |",
        f"| Cross-check detail | {reason} |",
        "",
    ]


def _cross_check_delta(cross_check: dict[str, Any]) -> str:
    external = get_value(cross_check, "external_sloc")
    benchmark = get_value(cross_check, "benchmark_sloc")
    delta = get_value(cross_check, "delta")
    if external is None or benchmark is None:
        return "n/a"
    return f"external SLOC `{external}`, benchmark SLOC `{benchmark}`, delta `{delta}`"
