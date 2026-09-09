from __future__ import annotations

from collections.abc import Sequence

from dpone.gitops.models import (
    GitOpsAffectedReport,
    GitOpsImpactedManifest,
    GitOpsImpactReason,
    GitOpsIssue,
)
from dpone.gitops.path_policy import GitOpsPathValidationError, safe_relative_path
from dpone.manifest.sparse_paths_models import SparsePathEntry, SparsePathReport


class GitOpsImpactAnalyzer:
    """Builds a reverse dependency impact report from sparse-path reports."""

    def analyze(
        self,
        *,
        changed_files: Sequence[object],
        sparse_reports: Sequence[SparsePathReport],
        runner: str,
    ) -> GitOpsAffectedReport:
        normalized, blockers = _normalize_changed_files(changed_files)
        if blockers:
            return GitOpsAffectedReport(changed_files=normalized, impacted_manifests=(), blockers=tuple(blockers))

        impacted: list[GitOpsImpactedManifest] = []
        warnings: list[GitOpsIssue] = []
        report_blockers: list[GitOpsIssue] = []
        for report in sparse_reports:
            warnings.extend(_copy_issues(report.warnings, source_prefix=report.manifest))
            report_blockers.extend(_copy_issues(report.blockers, source_prefix=report.manifest))
            reasons = _impact_reasons(changed_files=normalized, entries=report.entries)
            if not reasons:
                continue
            impacted.append(
                GitOpsImpactedManifest(
                    manifest=report.manifest,
                    workload_root=report.workload_root,
                    reasons=reasons,
                    suggested_commands=(_suggested_command(report.manifest, runner=runner),),
                )
            )

        return GitOpsAffectedReport(
            changed_files=normalized,
            impacted_manifests=tuple(impacted),
            warnings=tuple(warnings),
            blockers=tuple(report_blockers),
        )


def with_emitted_plans(
    report: GitOpsAffectedReport,
    emitted_plans: dict[str, str],
) -> GitOpsAffectedReport:
    return GitOpsAffectedReport(
        changed_files=report.changed_files,
        impacted_manifests=tuple(
            GitOpsImpactedManifest(
                manifest=item.manifest,
                workload_root=item.workload_root,
                reasons=item.reasons,
                suggested_commands=item.suggested_commands,
                emitted_plan=emitted_plans.get(item.manifest, item.emitted_plan),
            )
            for item in report.impacted_manifests
        ),
        warnings=report.warnings,
        blockers=report.blockers,
    )


def _normalize_changed_files(changed_files: Sequence[object]) -> tuple[tuple[str, ...], list[GitOpsIssue]]:
    normalized: list[str] = []
    blockers: list[GitOpsIssue] = []
    for raw in changed_files:
        try:
            normalized.append(safe_relative_path(raw, source="--changed-files").as_posix())
        except GitOpsPathValidationError as exc:
            blockers.append(
                GitOpsIssue(
                    code="invalid_changed_path",
                    message=str(exc),
                    path=str(raw or ""),
                    source="--changed-files",
                )
            )
    return tuple(normalized), blockers


def _impact_reasons(
    *,
    changed_files: tuple[str, ...],
    entries: tuple[SparsePathEntry, ...],
) -> tuple[GitOpsImpactReason, ...]:
    reasons: list[GitOpsImpactReason] = []
    seen: set[tuple[str, str]] = set()
    for changed_file in changed_files:
        for entry in entries:
            if not _matches(changed_file, entry):
                continue
            key = (changed_file, entry.path)
            if key in seen:
                continue
            seen.add(key)
            reasons.append(
                GitOpsImpactReason(
                    changed_path=changed_file,
                    matched_path=entry.path,
                    kind=entry.kind,
                    source=entry.source,
                    reason=entry.reason,
                )
            )
    return tuple(reasons)


def _matches(changed_file: str, entry: SparsePathEntry) -> bool:
    entry_path = entry.path
    if entry.is_dir or entry_path.endswith("/"):
        prefix = entry_path.rstrip("/")
        return changed_file == prefix or changed_file.startswith(f"{prefix}/")
    return changed_file == entry_path


def _copy_issues(issues: tuple[object, ...], *, source_prefix: str) -> list[GitOpsIssue]:
    copied: list[GitOpsIssue] = []
    for issue in issues:
        copied.append(
            GitOpsIssue(
                code=issue.code,
                message=issue.message,
                path=issue.path,
                source=f"{source_prefix}:{issue.source}",
            )
        )
    return copied


def _suggested_command(manifest: str, *, runner: str) -> str:
    command = f"dpone gitops plan {manifest}"
    if runner != "generic":
        return f"{command} --runner {runner}"
    return command


__all__ = ["GitOpsImpactAnalyzer", "with_emitted_plans"]
