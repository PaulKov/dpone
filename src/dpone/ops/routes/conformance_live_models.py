"""Live route conformance public contracts."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.ops.routes.conformance_models import (
        RouteConformanceDatasetProfile,
        RouteConformanceDecision,
        RouteConformanceReport,
        RouteConformanceVerificationResult,
    )


from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol

from dpone.ops.routes.conformance_rendering import report_markdown, write_report

ROUTE_CONFORMANCE_LIVE_SCHEMA_VERSION = "dpone.route_conformance_live.v1"


@dataclass(frozen=True, slots=True)
class RouteConformanceLiveConfig:
    """Configuration for one live conformance run."""

    adapter: str
    dataset: RouteConformanceDatasetProfile
    min_rows: int = 0
    min_columns: int = 0
    require_schema_evolution: bool = False
    drift_mode: str = "none"

    def to_dict(self) -> dict[str, object]:
        return {
            "adapter": self.adapter,
            "dataset": self.dataset.to_dict(),
            "min_rows": self.min_rows,
            "min_columns": self.min_columns,
            "require_schema_evolution": self.require_schema_evolution,
            "drift_mode": self.drift_mode,
        }


@dataclass(frozen=True, slots=True)
class RouteConformanceLiveStep:
    """One live runner step receipt."""

    name: str
    status: str
    summary: str
    rows: int = 0
    blockers: tuple[str, ...] = tuple()

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "status": self.status,
            "summary": self.summary,
            "rows": self.rows,
            "blockers": list(self.blockers),
        }


@dataclass(frozen=True, slots=True)
class RouteConformanceLiveReport:
    """Stable JSON/Markdown report for live route conformance."""

    route: _RouteIdentity
    profile: _RouteProfile | None
    adapter: str
    config: RouteConformanceLiveConfig
    conformance: RouteConformanceReport | None
    verification: RouteConformanceVerificationResult
    decision: RouteConformanceDecision
    live_steps: tuple[RouteConformanceLiveStep, ...]
    schema_evolution: Mapping[str, object]
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
            "schema_version": ROUTE_CONFORMANCE_LIVE_SCHEMA_VERSION,
            "route": self.route.to_dict(),
            "profile": self.profile.to_dict() if self.profile else None,
            "adapter": self.adapter,
            "config": self.config.to_dict(),
            "passed": self.decision.passed,
            "status": self.decision.status,
            "score": self.decision.score,
            "verification": self.verification.to_dict(),
            "schema_evolution": dict(self.schema_evolution),
            "conformance": self.conformance.to_dict() if self.conformance else None,
            "live_steps": [step.to_dict() for step in self.live_steps],
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
            f"- Adapter: `{self.adapter}`",
            f"- Dataset: `{self.config.dataset.name}` rows={self.config.dataset.row_count} columns={self.config.dataset.column_count}",
            f"- Source rows: `{self.verification.source_rows}`",
            f"- Sink rows: `{self.verification.sink_rows}`",
            f"- Chunks verified: `{self.verification.chunk_count}`",
            f"- Schema evolution: `{self.schema_evolution.get('status', 'unknown')}`",
        ]
        rows.extend(f"- Step `{step.name}`: `{step.status}` - {step.summary}" for step in self.live_steps)
        return report_markdown(
            title="Route Conformance Live Runner",
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


class _RouteIdentity(Protocol):
    @property
    def case_id(self) -> str: ...

    def to_dict(self) -> dict[str, str]: ...


class _RouteProfile(Protocol):
    def to_dict(self) -> dict[str, object]: ...


__all__ = [
    "ROUTE_CONFORMANCE_LIVE_SCHEMA_VERSION",
    "RouteConformanceLiveConfig",
    "RouteConformanceLiveReport",
    "RouteConformanceLiveStep",
]
