from __future__ import annotations

from typing import Any


def render_gitops_airflow_cluster_doctor_markdown(report: Any) -> str:
    lines = [
        "# GitOps Airflow cluster doctor",
        "",
        f"- passed: `{report.passed}`",
        f"- mode: `{report.mode}`",
        f"- runner policy: `{report.runner_policy}`",
        f"- namespace: `{report.namespace}`",
        f"- service account: `{report.service_account}`",
        "",
        "## Checks",
    ]
    if report.checks:
        lines.extend(f"- `{check.name}` passed=`{check.passed}` severity=`{check.severity}`" for check in report.checks)
    else:
        lines.append("- none")
    lines.extend(["", "## Secret Refs"])
    if report.secret_refs:
        for ref in report.secret_refs:
            keys = ", ".join(ref.required_keys) if ref.required_keys else "metadata only"
            lines.append(f"- `{ref.name}` kind=`{ref.kind}` keys=`{keys}`")
    else:
        lines.append("- none")
    lines.extend(["", "## Commands"])
    if report.commands:
        for command in report.commands:
            lines.append(f"- `{command.name}` required=`{command.required}` executed=`{command.executed}`")
    else:
        lines.append("- none")
    _append_issues(lines, "Warnings", report.warnings)
    _append_issues(lines, "Blockers", report.blockers)
    return "\n".join(lines) + "\n"


def _append_issues(lines: list[str], title: str, issues: tuple[Any, ...]) -> None:
    lines.extend(["", f"## {title}"])
    if not issues:
        lines.append("- none")
        return
    for issue in issues:
        lines.append(f"- `{issue.code}` at `{issue.path}`: {issue.message}")


__all__ = ["render_gitops_airflow_cluster_doctor_markdown"]
