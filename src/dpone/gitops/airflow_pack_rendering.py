from __future__ import annotations

from typing import Any


def render_gitops_airflow_pack_markdown(report: Any) -> str:
    lines = [
        "# GitOps Airflow runtime pack",
        "",
        f"- Status: {'passed' if report.passed else 'blocked'}",
        f"- Mode: `{report.mode}`",
        f"- Runner policy: `{report.runner_policy}`",
        f"- Artifact dir: `{report.artifact_dir}`",
        f"- Output path: `{report.output_path}`",
        "",
        "## Steps",
        "",
    ]
    for step in report.steps:
        credential = ", live credentials" if step.credential_required else ""
        lines.append(f"- `{step.name}` ({step.phase}{credential}): `{step.command}`")
    lines.extend(["", "## Artifacts", ""])
    for artifact in report.artifacts:
        state = "passed" if artifact.passed else artifact.reason
        required = "required" if artifact.required else "optional"
        lines.append(f"- `{artifact.path}` ({artifact.name}, {required}, {state})")
    if report.next_actions:
        lines.extend(["", "## Next Actions", ""])
        for action in report.next_actions:
            lines.append(f"- {action}")
    _append_issues(lines, title="Warnings", issues=report.warnings)
    _append_issues(lines, title="Blockers", issues=report.blockers)
    return "\n".join(lines).rstrip() + "\n"


def _append_issues(lines: list[str], *, title: str, issues: tuple[Any, ...]) -> None:
    if not issues:
        return
    lines.extend(["", f"## {title}", ""])
    for issue in issues:
        lines.append(f"- `{issue.code}` at `{issue.path}`: {issue.message}")


__all__ = ["render_gitops_airflow_pack_markdown"]
