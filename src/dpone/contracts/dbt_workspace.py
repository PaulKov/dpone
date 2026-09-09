"""Immutable workspace discovery and check results; no framework or I/O policy."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from dpone.contracts.airflow_deployment import is_canonical_sha256_digest

if TYPE_CHECKING:
    from dpone.contracts.dbt_publish_models import DbtCompileReport, DbtPublishIssue


@dataclass(frozen=True, slots=True)
class DbtWorkspaceProject:
    project_path: str
    project_name: str | None
    publishing: bool
    profiles_path: str | None
    manifest_path: str | None
    reason: Literal["policy_present", "not_configured", "invalid"]

    def to_dict(self) -> dict[str, Any]:
        return {
            "project_path": self.project_path,
            "project_name": self.project_name,
            "publishing": self.publishing,
            "profiles_path": self.profiles_path,
            "manifest_path": self.manifest_path,
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class DbtWorkspaceDiscoveryReport:
    projects: tuple[DbtWorkspaceProject, ...] = ()
    blockers: tuple[DbtPublishIssue, ...] = ()

    @property
    def passed(self) -> bool:
        return not self.blockers

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": "dpone.dbt-workspace-discovery.v1",
            "passed": self.passed,
            "projects": [project.to_dict() for project in self.projects],
            "blockers": [issue.to_jsonable() for issue in self.blockers],
        }


@dataclass(frozen=True, slots=True)
class DbtWorkspaceProjectCheck:
    project: DbtWorkspaceProject
    report: DbtCompileReport

    def to_dict(self) -> dict[str, Any]:
        return {"project_path": self.project.project_path, "report": self.report.to_jsonable()}


@dataclass(frozen=True, slots=True)
class DbtWorkspaceCheckReport:
    discovery: DbtWorkspaceDiscoveryReport
    projects: tuple[DbtWorkspaceProjectCheck, ...] = ()
    blockers: tuple[DbtPublishIssue, ...] = ()

    @property
    def passed(self) -> bool:
        return self.discovery.passed and not self.blockers and all(row.report.passed for row in self.projects)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": "dpone.dbt-workspace-check.v1",
            "passed": self.passed,
            "discovery": self.discovery.to_dict(),
            "projects": [row.to_dict() for row in self.projects],
            "blockers": [issue.to_jsonable() for issue in self.blockers],
        }


@dataclass(frozen=True, slots=True)
class DbtWorkspaceCompileReport:
    """A successful publication or a retained check with explicit failure."""

    check: DbtWorkspaceCheckReport
    output_dir: str
    release_id: str | None = None
    source_snapshot_sha256: str | None = None
    subject_sha256: str | None = None
    blockers: tuple[DbtPublishIssue, ...] = ()

    def __post_init__(self) -> None:
        identities = (self.release_id, self.source_snapshot_sha256, self.subject_sha256)
        successful = self.check.passed and bool(self.check.projects) and not self.blockers
        if any(value is not None for value in identities):
            if not successful or not all(is_canonical_sha256_digest(value) for value in identities):
                raise ValueError("workspace publication identities require a complete successful result")
        elif successful:
            raise ValueError("workspace compile report requires publication identities or a failure")
        if not isinstance(self.output_dir, str) or not self.output_dir:
            raise ValueError("workspace output directory is missing")

    @property
    def passed(self) -> bool:
        return self.release_id is not None

    @property
    def exit_code(self) -> int:
        if self.passed:
            return 0
        if any(issue.code in {"DPONE_DBT_INTERNAL", "DPONE_DBT_OUTPUT_WRITE_FAILED"} for issue in self.blockers):
            return 5
        return 2

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": "dpone.dbt-workspace-compile.v1",
            "passed": self.passed,
            "check": self.check.to_dict(),
            "output_dir": self.output_dir,
            "release_id": self.release_id,
            "source_snapshot_sha256": self.source_snapshot_sha256,
            "subject_sha256": self.subject_sha256,
            "blockers": [issue.to_jsonable() for issue in self.blockers],
        }
