"""Public contracts for the Route Conformance Lab."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Literal, Protocol

from dpone.ops.routes.conformance_rendering import report_markdown, write_report

ROUTE_CONFORMANCE_SCHEMA_VERSION = "dpone.route_conformance.v1"
ROUTE_CONFORMANCE_SUMMARY_SCHEMA_VERSION = "dpone.route_conformance_summary.v1"
ROUTE_CONFORMANCE_RELEASE_GATE_SCHEMA_VERSION = "dpone.route_conformance_release_gate.v1"

ConformanceStatus = Literal["verified", "warning", "blocked"]


@dataclass(frozen=True, slots=True)
class RouteConformanceDatasetProfile:
    """Synthetic dataset profile for one route conformance run."""

    name: str
    row_count: int = 10_000
    column_count: int = 200
    chunk_size: int = 1_000
    include_nested: bool = True
    include_schema_evolution: bool = True
    seed: int = 17

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "row_count": self.row_count,
            "column_count": self.column_count,
            "chunk_size": self.chunk_size,
            "include_nested": self.include_nested,
            "include_schema_evolution": self.include_schema_evolution,
            "seed": self.seed,
        }


@dataclass(frozen=True, slots=True)
class RouteConformanceColumn:
    """One logical column plus expected physical contract."""

    name: str
    logical_type: str
    physical_contract: str
    nullable: bool
    ordinal: int
    primary_key: bool = False
    parent_key: bool = False

    def with_physical_contract(self, value: str) -> RouteConformanceColumn:
        return RouteConformanceColumn(
            name=self.name,
            logical_type=self.logical_type,
            physical_contract=value,
            nullable=self.nullable,
            ordinal=self.ordinal,
            primary_key=self.primary_key,
            parent_key=self.parent_key,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "logical_type": self.logical_type,
            "physical_contract": self.physical_contract,
            "nullable": self.nullable,
            "ordinal": self.ordinal,
            "primary_key": self.primary_key,
            "parent_key": self.parent_key,
        }


@dataclass(frozen=True, slots=True)
class RouteConformanceSnapshot:
    """Rows and physical contracts captured for one side of verification."""

    kind: str
    columns: tuple[RouteConformanceColumn, ...]
    rows: tuple[Mapping[str, object], ...]
    fingerprint: str

    @property
    def row_count(self) -> int:
        return len(self.rows)

    @property
    def column_count(self) -> int:
        return len(self.columns)

    def to_dict(self, *, include_rows: bool = True) -> dict[str, object]:
        payload: dict[str, object] = {
            "kind": self.kind,
            "row_count": self.row_count,
            "column_count": self.column_count,
            "fingerprint": self.fingerprint,
            "columns": [column.to_dict() for column in self.columns],
        }
        if include_rows:
            payload["rows"] = [dict(row) for row in self.rows]
        return payload


@dataclass(frozen=True, slots=True)
class RouteConformanceDataset:
    """Generated source fixture before route verification."""

    profile: RouteConformanceDatasetProfile
    columns: tuple[RouteConformanceColumn, ...]
    rows: tuple[Mapping[str, object], ...]
    fingerprint: str
    schema_evolution_plan: Mapping[str, object]

    @property
    def chunk_count(self) -> int:
        return (len(self.rows) + self.profile.chunk_size - 1) // self.profile.chunk_size

    def to_snapshot(self, kind: str) -> RouteConformanceSnapshot:
        return RouteConformanceSnapshot(kind=kind, columns=self.columns, rows=self.rows, fingerprint=self.fingerprint)

    def with_rows_and_columns(
        self,
        rows: Sequence[Mapping[str, object]],
        columns: Sequence[RouteConformanceColumn],
    ) -> RouteConformanceDataset:
        return RouteConformanceDataset(
            profile=self.profile,
            columns=tuple(columns),
            rows=tuple(dict(row) for row in rows),
            fingerprint=self.fingerprint,
            schema_evolution_plan=self.schema_evolution_plan,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            **self.profile.to_dict(),
            "fingerprint": self.fingerprint,
            "schema_evolution_plan": dict(self.schema_evolution_plan),
        }


@dataclass(frozen=True, slots=True)
class RouteConformanceChunk:
    """Chunk-level typed hash verification result."""

    index: int
    start: int
    end: int
    source_hash: str
    sink_hash: str
    passed: bool

    def to_dict(self) -> dict[str, object]:
        return {
            "index": self.index,
            "start": self.start,
            "end": self.end,
            "source_hash": self.source_hash,
            "sink_hash": self.sink_hash,
            "passed": self.passed,
        }


@dataclass(frozen=True, slots=True)
class RouteConformanceVerificationResult:
    """Exact source/sink comparison result."""

    passed: bool
    source_rows: int
    sink_rows: int
    chunk_count: int
    source_hash: str
    sink_hash: str
    chunks: tuple[RouteConformanceChunk, ...]
    physical_contract_mismatches: tuple[Mapping[str, object], ...]
    mismatch_samples: tuple[Mapping[str, object], ...]
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "passed": self.passed,
            "source_rows": self.source_rows,
            "sink_rows": self.sink_rows,
            "chunk_count": self.chunk_count,
            "source_hash": self.source_hash,
            "sink_hash": self.sink_hash,
            "chunks": [chunk.to_dict() for chunk in self.chunks],
            "physical_contract_mismatches": [dict(item) for item in self.physical_contract_mismatches],
            "mismatch_samples": [dict(item) for item in self.mismatch_samples],
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
        }


@dataclass(frozen=True, slots=True)
class RouteConformanceDecision:
    """Policy decision for one conformance run."""

    passed: bool
    status: ConformanceStatus
    score: float
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    next_actions: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "passed": self.passed,
            "status": self.status,
            "score": self.score,
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "next_actions": list(self.next_actions),
        }


@dataclass(frozen=True, slots=True)
class RouteConformanceReport:
    """Stable report for one route conformance run."""

    route: _RouteIdentity
    profile: _RouteProfile | None
    dataset: RouteConformanceDatasetProfile
    verification: RouteConformanceVerificationResult
    schema_evolution: Mapping[str, object]
    decision: RouteConformanceDecision
    artifacts: Mapping[str, str]
    output_dir: str
    json_path: str
    markdown_path: str

    @property
    def passed(self) -> bool:
        return self.decision.passed

    @property
    def blockers(self) -> tuple[str, ...]:
        return self.decision.blockers

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": ROUTE_CONFORMANCE_SCHEMA_VERSION,
            "route": self.route.to_dict(),
            "profile": self.profile.to_dict() if self.profile else None,
            "passed": self.decision.passed,
            "status": self.decision.status,
            "score": self.decision.score,
            "dataset": self.dataset.to_dict(),
            "verification": self.verification.to_dict(),
            "schema_evolution": dict(self.schema_evolution),
            "artifacts": dict(self.artifacts),
            "blockers": list(self.decision.blockers),
            "warnings": list(self.decision.warnings),
            "next_actions": list(self.decision.next_actions),
            "output_dir": self.output_dir,
            "json_path": self.json_path,
            "markdown_path": self.markdown_path,
        }

    def to_markdown(self) -> str:
        rows = [
            f"- Dataset: `{self.dataset.name}` rows={self.dataset.row_count} columns={self.dataset.column_count}",
            f"- Typed hash: `{self.verification.source_hash}`",
            f"- Chunks verified: `{self.verification.chunk_count}`",
            f"- Schema evolution: `{self.schema_evolution.get('status', 'unknown')}`",
        ]
        return report_markdown(
            title="Route Conformance Lab",
            identity=self.route.case_id,
            passed=self.decision.passed,
            status=self.decision.status,
            score=self.decision.score,
            rows=rows,
            blockers=self.decision.blockers,
            warnings=self.decision.warnings,
            next_actions=self.decision.next_actions,
        )

    def write(self) -> None:
        write_report(self.output_dir, self.json_path, self.markdown_path, self.to_dict(), self.to_markdown())


@dataclass(frozen=True, slots=True)
class RouteConformanceArtifactSummary:
    """One route conformance artifact used by summary and release gates."""

    name: str
    path: str
    passed: bool
    missing: bool
    sha256: str
    route_case_id: str
    dataset: Mapping[str, object]
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "path": self.path,
            "passed": self.passed,
            "missing": self.missing,
            "sha256": self.sha256,
            "route_case_id": self.route_case_id,
            "dataset": dict(self.dataset),
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
        }


@dataclass(frozen=True, slots=True)
class RouteConformanceAggregateReport:
    """Stable report for conformance summaries and release gates."""

    schema_version: str
    title: str
    release: str
    passed: bool
    status: ConformanceStatus
    score: float
    routes: tuple[RouteConformanceArtifactSummary, ...]
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    next_actions: tuple[str, ...]
    output_dir: str
    json_path: str
    markdown_path: str

    def to_dict(self) -> dict[str, object]:
        payload = {
            "schema_version": self.schema_version,
            "passed": self.passed,
            "status": self.status,
            "score": self.score,
            "routes": [route.to_dict() for route in self.routes],
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "next_actions": list(self.next_actions),
            "output_dir": self.output_dir,
            "json_path": self.json_path,
            "markdown_path": self.markdown_path,
        }
        if self.release:
            payload["release"] = self.release
        return payload

    def to_markdown(self) -> str:
        rows = [
            f"- `{route.name}` passed={route.passed} route={route.route_case_id} sha256=`{route.sha256}`"
            for route in self.routes
        ]
        return report_markdown(
            title=self.title,
            identity=self.release or "route-conformance-summary",
            passed=self.passed,
            status=self.status,
            score=self.score,
            rows=rows,
            blockers=self.blockers,
            warnings=self.warnings,
            next_actions=self.next_actions,
        )

    def write(self) -> None:
        write_report(self.output_dir, self.json_path, self.markdown_path, self.to_dict(), self.to_markdown())


class _RouteIdentity(Protocol):
    @property
    def case_id(self) -> str: ...

    def to_dict(self) -> dict[str, str]: ...


class _RouteProfile(Protocol):
    def to_dict(self) -> dict[str, object]: ...


__all__ = [
    "ConformanceStatus",
    "ROUTE_CONFORMANCE_RELEASE_GATE_SCHEMA_VERSION",
    "ROUTE_CONFORMANCE_SCHEMA_VERSION",
    "ROUTE_CONFORMANCE_SUMMARY_SCHEMA_VERSION",
    "RouteConformanceAggregateReport",
    "RouteConformanceArtifactSummary",
    "RouteConformanceChunk",
    "RouteConformanceColumn",
    "RouteConformanceDataset",
    "RouteConformanceDatasetProfile",
    "RouteConformanceDecision",
    "RouteConformanceReport",
    "RouteConformanceSnapshot",
    "RouteConformanceVerificationResult",
]
