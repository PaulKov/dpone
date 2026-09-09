"""Route onboarding public contracts for doctor, discovery, and bootstrap."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal, Protocol

from dpone.ops.routes.bootstrap_rendering import (
    json_text,
    report_markdown,
    route_bootstrap_markdown,
    route_doctor_markdown,
    source_discovery_markdown,
    write_report,
)

CONNECTION_DOCTOR_SCHEMA_VERSION = "dpone.connection_doctor.v1"
SOURCE_DISCOVERY_SCHEMA_VERSION = "dpone.source_discovery.v1"
ROUTE_BOOTSTRAP_SCHEMA_VERSION = "dpone.route_bootstrap.v1"
ROUTE_DOCTOR_SCHEMA_VERSION = "dpone.route_doctor.v1"

OnboardingStatus = Literal["ready", "warning", "blocked"]


@dataclass(frozen=True, slots=True)
class OnboardingCheck:
    """One normalized onboarding check."""

    name: str
    kind: str
    required: bool
    passed: bool
    status: OnboardingStatus
    summary: str
    remediation: str = ""

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "kind": self.kind,
            "required": self.required,
            "passed": self.passed,
            "status": self.status,
            "summary": self.summary,
            "remediation": self.remediation,
        }


@dataclass(frozen=True, slots=True)
class DiscoveredColumn:
    """One source column discovered from schema evidence."""

    name: str
    data_type: str
    nullable: bool
    kind: str
    key_candidate: bool
    cursor_candidate: bool
    risk_level: OnboardingStatus
    risk: str

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "type": self.data_type,
            "nullable": self.nullable,
            "kind": self.kind,
            "key_candidate": self.key_candidate,
            "cursor_candidate": self.cursor_candidate,
            "risk_level": self.risk_level,
            "risk": self.risk,
        }


@dataclass(frozen=True, slots=True)
class DiscoveredTable:
    """One source table profile discovered from schema evidence."""

    schema: str
    name: str
    row_count: int | None
    columns: tuple[DiscoveredColumn, ...]

    @property
    def qualified_name(self) -> str:
        return f"{self.schema}.{self.name}" if self.schema else self.name

    @property
    def primary_key_candidates(self) -> tuple[str, ...]:
        return tuple(column.name for column in self.columns if column.key_candidate)

    @property
    def cursor_candidates(self) -> tuple[str, ...]:
        return tuple(column.name for column in self.columns if column.cursor_candidate)

    def to_dict(self) -> dict[str, object]:
        return {
            "schema": self.schema,
            "name": self.name,
            "qualified_name": self.qualified_name,
            "row_count": self.row_count,
            "primary_key_candidates": list(self.primary_key_candidates),
            "cursor_candidates": list(self.cursor_candidates),
            "columns": [column.to_dict() for column in self.columns],
        }


@dataclass(frozen=True, slots=True)
class ArtifactSummary:
    """Normalized upstream artifact summary for route doctor."""

    name: str
    path: str
    required: bool
    passed: bool
    missing: bool
    sha256: str
    status: OnboardingStatus
    summary: str
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "path": self.path,
            "required": self.required,
            "passed": self.passed,
            "missing": self.missing,
            "sha256": self.sha256,
            "status": self.status,
            "summary": self.summary,
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
        }


@dataclass(frozen=True, slots=True)
class ConnectionDoctorReport:
    """Stable JSON/Markdown connection doctor contract."""

    route: _RouteIdentity
    profile: _RouteProfile | None
    passed: bool
    status: OnboardingStatus
    score: float
    checks: tuple[OnboardingCheck, ...]
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    next_actions: tuple[str, ...]
    output_dir: str
    json_path: str
    markdown_path: str

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": CONNECTION_DOCTOR_SCHEMA_VERSION,
            "route": self.route.to_dict(),
            "profile": self.profile.to_dict() if self.profile else None,
            "passed": self.passed,
            "status": self.status,
            "score": self.score,
            "checks": [check.to_dict() for check in self.checks],
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "next_actions": list(self.next_actions),
            "output_dir": self.output_dir,
            "json_path": self.json_path,
            "markdown_path": self.markdown_path,
        }

    def to_json(self) -> str:
        return json_text(self.to_dict())

    def to_markdown(self) -> str:
        return report_markdown(
            title="Connection doctor",
            identity=self.route.case_id,
            status=self.status,
            passed=self.passed,
            score=self.score,
            checks=self.checks,
            blockers=self.blockers,
            warnings=self.warnings,
            next_actions=self.next_actions,
        )

    def write(self) -> None:
        write_report(self.output_dir, self.json_path, self.markdown_path, self.to_json(), self.to_markdown())


@dataclass(frozen=True, slots=True)
class SourceDiscoveryReport:
    """Stable JSON/Markdown source discovery contract."""

    source: str
    dataset: str
    passed: bool
    status: OnboardingStatus
    score: float
    tables: tuple[DiscoveredTable, ...]
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    next_actions: tuple[str, ...]
    output_dir: str
    json_path: str
    markdown_path: str

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": SOURCE_DISCOVERY_SCHEMA_VERSION,
            "source": self.source,
            "dataset": self.dataset,
            "passed": self.passed,
            "status": self.status,
            "score": self.score,
            "tables": [table.to_dict() for table in self.tables],
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "next_actions": list(self.next_actions),
            "output_dir": self.output_dir,
            "json_path": self.json_path,
            "markdown_path": self.markdown_path,
        }

    def to_json(self) -> str:
        return json_text(self.to_dict())

    def to_markdown(self) -> str:
        return source_discovery_markdown(self)

    def write(self) -> None:
        write_report(self.output_dir, self.json_path, self.markdown_path, self.to_json(), self.to_markdown())


@dataclass(frozen=True, slots=True)
class RouteBootstrapReport:
    """Stable JSON/Markdown route bootstrap contract."""

    route: _RouteIdentity
    profile: _RouteProfile | None
    dataset: str
    passed: bool
    status: OnboardingStatus
    score: float
    manifest: Mapping[str, object]
    manifest_path: str
    type_risks: Mapping[str, object]
    next_commands: tuple[str, ...]
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    next_actions: tuple[str, ...]
    output_dir: str
    json_path: str
    markdown_path: str

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": ROUTE_BOOTSTRAP_SCHEMA_VERSION,
            "route": self.route.to_dict(),
            "profile": self.profile.to_dict() if self.profile else None,
            "dataset": self.dataset,
            "passed": self.passed,
            "status": self.status,
            "score": self.score,
            "manifest": dict(self.manifest),
            "manifest_path": self.manifest_path,
            "type_risks": dict(self.type_risks),
            "next_commands": list(self.next_commands),
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "next_actions": list(self.next_actions),
            "output_dir": self.output_dir,
            "json_path": self.json_path,
            "markdown_path": self.markdown_path,
        }

    def to_json(self) -> str:
        return json_text(self.to_dict())

    def to_markdown(self) -> str:
        return route_bootstrap_markdown(self)

    def write(self) -> None:
        write_report(self.output_dir, self.json_path, self.markdown_path, self.to_json(), self.to_markdown())


@dataclass(frozen=True, slots=True)
class RouteDoctorReport:
    """Stable JSON/Markdown route doctor aggregate contract."""

    route: _RouteIdentity
    profile: _RouteProfile | None
    passed: bool
    status: OnboardingStatus
    score: float
    artifacts: tuple[ArtifactSummary, ...]
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    next_actions: tuple[str, ...]
    output_dir: str
    json_path: str
    markdown_path: str

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": ROUTE_DOCTOR_SCHEMA_VERSION,
            "route": self.route.to_dict(),
            "profile": self.profile.to_dict() if self.profile else None,
            "passed": self.passed,
            "status": self.status,
            "score": self.score,
            "artifacts": [artifact.to_dict() for artifact in self.artifacts],
            "artifact_index": {artifact.name: artifact.to_dict() for artifact in self.artifacts},
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "next_actions": list(self.next_actions),
            "output_dir": self.output_dir,
            "json_path": self.json_path,
            "markdown_path": self.markdown_path,
        }

    def to_json(self) -> str:
        return json_text(self.to_dict())

    def to_markdown(self) -> str:
        return route_doctor_markdown(self)

    def write(self) -> None:
        write_report(self.output_dir, self.json_path, self.markdown_path, self.to_json(), self.to_markdown())


class _RouteIdentity(Protocol):
    @property
    def case_id(self) -> str: ...

    @property
    def colon_id(self) -> str: ...

    def to_dict(self) -> dict[str, str]: ...


class _RouteProfile(Protocol):
    @property
    def install_extras(self) -> tuple[str, ...]: ...

    @property
    def docs_link(self) -> str: ...

    @property
    def native_fast_path(self) -> str: ...

    def to_dict(self) -> dict[str, object]: ...


__all__ = [
    "ArtifactSummary",
    "ConnectionDoctorReport",
    "DiscoveredColumn",
    "DiscoveredTable",
    "OnboardingCheck",
    "OnboardingStatus",
    "RouteBootstrapReport",
    "RouteDoctorReport",
    "SourceDiscoveryReport",
]
