"""Compile-report failure projection for dbt artifact publication."""

from __future__ import annotations

from dataclasses import replace

from dpone.contracts.dbt_publish_models import DbtCompileReport, DbtPublishIssue


def blocked_report(
    report: DbtCompileReport,
    *,
    code: str,
    message: str,
    path: str,
    remediation: str | None = None,
) -> DbtCompileReport:
    """Append one safe artifact-publication blocker and clear output claims."""

    remediations = {
        "DPONE_DBT_PUBLISH_OUTPUT_CONFLICT": (
            "Choose an empty output directory or remove the stale generated tree "
            "after confirming no concurrent CI writer owns it."
        ),
        "DPONE_DBT_OUTPUT_WRITE_FAILED": ("Check output permissions and free space, then retry the atomic compile."),
        "DPONE_DBT_PACKAGE_LOCK_REQUIRED": ("Run `dbt deps && dbt parse`, then retry the dpone compile."),
        "DPONE_DBT_PACKAGES_NOT_RESOLVED": ("Run `dbt deps && dbt parse`, then retry the dpone compile."),
        "DPONE_DBT_COMPILE_FAILED": ("Run `dpone dbt check` and fix the reported authoring or policy blockers."),
    }
    return replace(
        report,
        blockers=(
            *report.blockers,
            DbtPublishIssue(
                code=code,
                message=message,
                path=path,
                remediation=remediation or remediations.get(code),
            ),
        ),
        artifacts={},
    )
