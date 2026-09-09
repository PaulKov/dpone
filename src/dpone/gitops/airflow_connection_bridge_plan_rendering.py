from __future__ import annotations

from typing import Any


def render_gitops_airflow_connection_bridge_plan_markdown(report: Any) -> str:
    lines = [
        "# GitOps Airflow connection bridge plan",
        "",
        f"- passed: `{report.passed}`",
        f"- artifact dir: `{report.artifact_dir}`",
        f"- output: `{report.output_path}`",
        f"- mode: `{report.mode}`",
        f"- runtime mode: `{report.runtime_mode}`",
        "",
        "## Required connections",
    ]
    if report.required_connection_ids:
        lines.extend(f"- `{connection_id}`" for connection_id in report.required_connection_ids)
    else:
        lines.append("- none")
    lines.extend(["", "## Artifacts"])
    if report.artifacts:
        for artifact in report.artifacts:
            lines.append(
                f"- `{artifact.path}` kind=`{artifact.kind}` required=`{artifact.required}` reason=`{artifact.reason}`"
            )
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


__all__ = ["render_gitops_airflow_connection_bridge_plan_markdown"]
