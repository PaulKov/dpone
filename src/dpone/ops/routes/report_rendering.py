"""Rendering and persistence for route readiness report contracts."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def report_json(report: Any) -> str:
    return json.dumps(report.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"


def write_report(report: Any) -> None:
    Path(report.output_dir).mkdir(parents=True, exist_ok=True)
    Path(report.json_path).write_text(report.to_json(), encoding="utf-8")
    Path(report.markdown_path).write_text(report.to_markdown(), encoding="utf-8")


def route_readiness_markdown(report: Any) -> str:
    lines = [
        "# dpone route readiness",
        "",
        f"- Route: `{report.route.case_id}`",
        f"- Passed: `{report.passed}`",
        f"- Level: `{report.level}`",
        f"- Score: `{report.score}`",
    ]
    if report.profile:
        lines.extend(
            [
                f"- Docs: `{report.profile.docs_link}`",
                f"- Native fast path: `{report.profile.native_fast_path}`",
                f"- Certification status: `{report.profile.certification_status}`",
            ]
        )
    lines.extend(["", "## Evidence", "", "| evidence | kind | required | status | sha256 | summary | path |"])
    lines.append("|---|---|---|---|---|---|---|")
    for item in report.evidence:
        status = "missing" if item.missing else ("pass" if item.passed else "fail")
        lines.append(
            f"| `{item.name}` | `{item.kind}` | `{item.required}` | {status} | "
            f"`{item.sha256}` | {item.summary} | `{item.path}` |"
        )
    _append_blockers_warnings_actions(
        lines, report, complete_message="Route evidence is complete for the configured policy."
    )
    return "\n".join(lines)


def route_schema_evolution_markdown(report: Any) -> str:
    lines = [
        "# Route schema evolution",
        "",
        f"- Route: `{report.route.case_id}`",
        f"- Passed: `{report.passed}`",
        f"- Level: `{report.level}`",
        f"- Apply mode: `{report.apply_decision.mode}`",
        f"- Safe to apply: `{report.apply_decision.safe_to_apply}`",
        f"- Requires approval: `{report.apply_decision.requires_approval}`",
        "",
        "## Change",
        "",
        f"- Kind: `{report.change.get('kind', '')}`",
        f"- Source: `{report.change.get('source_table', '')}.{report.change.get('source_column', '')}`",
        f"- Target: `{report.change.get('target_table', '')}.{report.change.get('target_column', '')}`",
    ]
    _append_blockers_warnings_actions(lines, report, complete_message="Schema evolution evidence is green.")
    return "\n".join(lines)


def route_reconciliation_repair_markdown(report: Any) -> str:
    lines = [
        "# Route reconciliation repair",
        "",
        f"- Route: `{report.route.case_id}`",
        f"- Passed: `{report.passed}`",
        f"- Level: `{report.level}`",
        f"- Repair actions: `{len(report.repair_plan.actions)}`",
        f"- Source boundary: `{report.repair_plan.source_boundary}`",
        f"- Target boundary: `{report.repair_plan.target_boundary}`",
        "",
        "| action | key | reason |",
        "|---|---|---|",
    ]
    if report.repair_plan.actions:
        for action in report.repair_plan.actions:
            key = json.dumps(dict(action.key), ensure_ascii=False, sort_keys=True)
            lines.append(f"| `{action.action}` | `{key}` | {action.reason} |")
    else:
        lines.append("| `none` | `{}` | source and target are reconciled |")
    _append_blockers_warnings_actions(lines, report, complete_message="No repair action is required.")
    return "\n".join(lines)


def _append_blockers_warnings_actions(lines: list[str], report: Any, *, complete_message: str) -> None:
    lines.extend(["", "## Blockers", ""])
    lines.extend(f"- `{item}`" for item in report.blockers) if report.blockers else lines.append("- none")
    lines.extend(["", "## Warnings", ""])
    lines.extend(f"- `{item}`" for item in report.warnings) if report.warnings else lines.append("- none")
    lines.extend(["", "## Next actions", ""])
    lines.extend(f"- {item}" for item in report.next_actions) if report.next_actions else lines.append(
        f"- {complete_message}"
    )
    lines.append("")


__all__ = [
    "report_json",
    "route_readiness_markdown",
    "route_reconciliation_repair_markdown",
    "route_schema_evolution_markdown",
    "write_report",
]
