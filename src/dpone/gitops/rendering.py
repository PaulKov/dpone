from __future__ import annotations

from dpone.gitops.bundle_verify import GitOpsBundleVerifyReport
from dpone.gitops.models import (
    GitOpsAffectedReport,
    GitOpsBundleReport,
    GitOpsPlanReport,
    GitOpsVerifyReport,
)


def render_gitops_bundle_markdown(report: GitOpsBundleReport) -> str:
    lines = [
        "# GitOps bundle",
        "",
        f"- Status: {'passed' if report.passed else 'blocked'}",
        f"- Output directory: `{report.output_dir}`",
        f"- Affected report: `{report.affected_path}`",
        f"- Summary: `{report.summary_path}`",
        f"- Entries: {len(report.entries)}",
        f"- Policy profile: `{report.policy.profile}`",
        f"- Verify lock: {'yes' if report.policy.verify_lock else 'no'}",
        "",
        "## Manifests",
        "",
    ]
    if not report.entries:
        lines.append("- None")
    for entry in report.entries:
        state = "passed" if entry.passed else "blocked"
        lines.append(f"- `{entry.manifest}` ({state})")
        lines.append(f"  - Plan: `{entry.plan_path}`")
        lines.append(f"  - Verify: `{entry.verify_path}`")
    if report.attestation is not None:
        lines.extend(["", "## Attestation", ""])
        lines.append(f"- Bundle digest: `{report.attestation.bundle_digest}`")
        for artifact in report.attestation.artifacts:
            lines.append(f"- `{artifact.path}` `{artifact.sha256}`")
    _append_issues(lines, title="Warnings", issues=report.warnings)
    _append_issues(lines, title="Blockers", issues=report.blockers)
    return "\n".join(lines).rstrip() + "\n"


def render_gitops_bundle_verify_markdown(report: GitOpsBundleVerifyReport) -> str:
    lines = [
        "# GitOps bundle verify",
        "",
        f"- Status: {'passed' if report.passed else 'blocked'}",
        f"- Bundle: `{report.bundle_path}`",
        f"- Schema: {'passed' if report.schema_check.passed else 'blocked'}",
        f"- Attestation: {'present' if report.attestation_check.present else 'missing'}",
        "",
        "## Artifact checks",
        "",
    ]
    if not report.artifact_checks:
        lines.append("- None")
    for check in report.artifact_checks:
        state = "passed" if check.passed else "blocked"
        lines.append(f"- `{check.path}` ({state})")
    _append_issues(lines, title="Warnings", issues=report.warnings)
    _append_issues(lines, title="Blockers", issues=report.blockers)
    return "\n".join(lines).rstrip() + "\n"


def render_gitops_affected_markdown(report: GitOpsAffectedReport) -> str:
    lines = [
        "# GitOps affected",
        "",
        f"- Status: {'passed' if report.passed else 'blocked'}",
        f"- Changed files: {len(report.changed_files)}",
        f"- Impacted manifests: {len(report.impacted_manifests)}",
        "",
        "## Changed files",
        "",
    ]
    for path in report.changed_files:
        lines.append(f"- `{path}`")
    lines.extend(["", "## Impacted manifests", ""])
    if not report.impacted_manifests:
        lines.append("- None")
    for impacted in report.impacted_manifests:
        lines.append(f"- `{impacted.manifest}`")
        for reason in impacted.reasons:
            lines.append(f"  - `{reason.changed_path}` matched `{reason.matched_path}` ({reason.kind})")
        for command in impacted.suggested_commands:
            lines.append(f"  - Suggested: `{command}`")
        if impacted.emitted_plan:
            lines.append(f"  - Emitted plan: `{impacted.emitted_plan}`")
    _append_issues(lines, title="Warnings", issues=report.warnings)
    _append_issues(lines, title="Blockers", issues=report.blockers)
    return "\n".join(lines).rstrip() + "\n"


def render_gitops_plan_markdown(report: GitOpsPlanReport) -> str:
    lines = [
        "# GitOps plan",
        "",
        f"- Status: {'passed' if report.passed else 'blocked'}",
        f"- Manifest: `{report.manifest}`",
        f"- Workload root: `{report.workload_root}`",
        f"- Runner: `{report.runner.kind}`",
        f"- Command: `{report.command.text}`",
        "",
        "## Sparse paths",
        "",
    ]
    for entry in report.sparse_paths:
        required = "required" if entry.required else "optional"
        lines.append(f"- `{entry.path}` ({entry.kind}, {required})")
    _append_issues(lines, title="Warnings", issues=report.warnings)
    _append_issues(lines, title="Blockers", issues=report.blockers)
    return "\n".join(lines).rstrip() + "\n"


def render_gitops_verify_markdown(report: GitOpsVerifyReport) -> str:
    lines = [
        "# GitOps verify",
        "",
        f"- Status: {'passed' if report.passed else 'blocked'}",
        f"- Plan: `{report.plan}`",
        f"- Worktree: `{report.worktree}`",
        f"- Manifest: `{report.manifest}`",
        "",
        "## Checked paths",
        "",
    ]
    for check in report.checked_paths:
        state = "present" if check.exists else "missing"
        required = "required" if check.required else "optional"
        lines.append(f"- `{check.path}` ({required}, {state})")
    if report.lock_checks:
        lines.extend(["", "## Lock checks", ""])
        for check in report.lock_checks:
            state = "passed" if check.passed else "failed"
            lines.append(f"- `{check.path}` ({state})")
    _append_issues(lines, title="Warnings", issues=report.warnings)
    _append_issues(lines, title="Blockers", issues=report.blockers)
    return "\n".join(lines).rstrip() + "\n"


def _append_issues(lines: list[str], *, title: str, issues: tuple[object, ...]) -> None:
    if not issues:
        return
    lines.extend(["", f"## {title}", ""])
    for issue in issues:
        lines.append(f"- `{issue.code}` `{issue.path}`: {issue.message}")


__all__ = [
    "render_gitops_affected_markdown",
    "render_gitops_bundle_markdown",
    "render_gitops_bundle_verify_markdown",
    "render_gitops_plan_markdown",
    "render_gitops_verify_markdown",
]
