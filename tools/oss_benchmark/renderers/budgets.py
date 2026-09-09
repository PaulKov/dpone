"""Markdown rendering for benchmark quality budgets."""

from __future__ import annotations

from typing import Any

from tools.oss_benchmark.payload_utils import format_gate_value


def render_quality_budgets_section(payload: dict[str, Any]) -> str:
    budgets = payload.get("quality_budgets") or {}
    findings = list(budgets.get("findings") or [])
    if not findings:
        return ""
    summary = budgets.get("summary") or {}
    lines = [
        '<a id="quality-budget-as-code"></a>',
        "",
        "## Quality Budget As Code",
        "",
        f"Budget policy: `{budgets.get('budget_path', 'n/a')}`. Status: **`{budgets.get('status', 'n/a')}`**.",
        "",
        (
            f"Passed `{summary.get('passed', 0)}`, warning `{summary.get('warning', 0)}`, "
            f"failed `{summary.get('failed', 0)}`."
        ),
        "",
        "| Project | Metric | Value | Warning | Failure | Status |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for finding in findings:
        if finding.get("status") == "passed":
            continue
        lines.append(
            "| "
            f"{finding.get('project_name', finding.get('project_id', 'n/a'))} | "
            f"`{finding.get('metric', 'n/a')}` | "
            f"{format_gate_value(finding.get('value'))} | "
            f"{format_gate_value(finding.get('warn_threshold'))} | "
            f"{format_gate_value(finding.get('fail_threshold'))} | "
            f"`{finding.get('status', 'n/a')}` |"
        )
    if all(finding.get("status") == "passed" for finding in findings):
        lines.append("| all | `quality_budget` | n/a | n/a | n/a | `passed` |")
    lines.extend(_debt_ledger_lines(budgets))
    return "\n".join(lines)


def _debt_ledger_lines(budgets: dict[str, Any]) -> list[str]:
    debt = list(budgets.get("debt_ledger") or [])
    lines = ["", "### Debt Ledger", ""]
    if not debt:
        lines.extend(["No quality-budget debt is visible in the current evidence.", ""])
        return lines
    lines.extend(["| Project | Debt | Status | Release handling |", "|---|---|---:|---|"])
    for finding in debt[:30]:
        handling = "blocks release" if finding.get("status") == "failed" else "visible managed warning"
        lines.append(
            "| "
            f"{finding.get('project_name', finding.get('project_id', 'n/a'))} | "
            f"`{finding.get('metric', 'n/a')}` = {format_gate_value(finding.get('value'))} | "
            f"`{finding.get('status', 'n/a')}` | "
            f"{handling} |"
        )
    lines.append("")
    return lines
