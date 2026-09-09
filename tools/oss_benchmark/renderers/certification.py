"""Markdown rendering for runtime certification evidence."""

from __future__ import annotations

from typing import Any


def render_runtime_certification_section(payload: dict[str, Any]) -> str:
    matrix = payload.get("runtime_certification") or {}
    scenarios = list(matrix.get("scenarios") or [])
    if not scenarios:
        return ""
    summary = matrix.get("summary") or {}
    lines = [
        '<a id="runtime-certification-matrix"></a>',
        "",
        "## Runtime Certification Matrix",
        "",
        "Runtime certification connects release claims to concrete checks and generated artifacts. Failed scenarios remain visible instead of being converted into optimistic narrative.",
        "",
        f"Passed `{summary.get('passed', 0)}`, failed `{summary.get('failed', 0)}`.",
        "",
        "| Scenario | Source | Strategy | Sink | Status | Checks | Artifacts |",
        "|---|---:|---:|---:|---:|---|---|",
    ]
    for scenario in scenarios:
        checks = "<br>".join(
            f"{check.get('name', 'n/a')}: `{check.get('status', 'n/a')}`" for check in scenario.get("checks") or []
        )
        artifacts = "<br>".join(f"`{path}`" for path in scenario.get("artifact_paths") or []) or "n/a"
        lines.append(
            "| "
            f"{scenario.get('scenario_id', 'n/a')} | "
            f"{scenario.get('source', 'n/a')} | "
            f"`{scenario.get('strategy', 'n/a')}` | "
            f"{scenario.get('sink', 'n/a')} | "
            f"`{scenario.get('status', 'n/a')}` | "
            f"{checks} | "
            f"{artifacts} |"
        )
    lines.append("")
    return "\n".join(lines)
