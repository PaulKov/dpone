"""Markdown rendering for BI-ready benchmark exports."""

from __future__ import annotations

from typing import Any


def render_evidence_exports_section(payload: dict[str, Any]) -> str:
    manifest = payload.get("evidence_exports") or {}
    files = list(manifest.get("files") or [])
    if not files:
        return ""
    lines = [
        '<a id="evidence-warehouse-export"></a>',
        "",
        "## Evidence Warehouse Export",
        "",
        "The benchmark also emits BI-ready CSV facts so quality, freshness, gates and claims can be trended outside Markdown.",
        "",
        f"Output directory: `{manifest.get('output_dir', 'n/a')}`.",
        "",
        "| Export | Purpose |",
        "|---|---|",
    ]
    for name in files:
        lines.append(f"| `{name}` | {_purpose(name)} |")
    lines.append("")
    return "\n".join(lines)


def _purpose(name: str) -> str:
    return {
        "projects.csv": "project-level size, freshness and release identity facts",
        "metric_groups.csv": "per-project freshness and stale-age facts",
        "quality_gates.csv": "normalized release gate results",
        "claims.csv": "public claim status and confidence facts",
        "runtime_certification.csv": "runtime certification scenario facts",
        "certification_scenarios.csv": "executable certification scenario facts",
        "contract_checks.csv": "per-scenario executable contract check facts",
        "run_ledger.csv": "operational run ledger facts with hashes and durations",
        "release_deltas.csv": "baseline comparison facts",
        "debt_ledger.csv": "quality-budget debt facts",
    }.get(name, "benchmark fact export")
