"""Public read-only Studio projection for one authored pipeline."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

PIPELINE_SUMMARY_SCHEMA = "dpone.pipeline-summary.v1"


@dataclass(frozen=True, slots=True)
class PipelineProcessSummary:
    name: str
    source: str
    sink: str
    strategy: str

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class PipelineNextAction:
    id: str
    label: str
    argv: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "label": self.label,
            "argv": list(self.argv),
        }


@dataclass(frozen=True, slots=True)
class PipelineSummary:
    id: str
    source_path: str
    valid: bool
    operational_status: str
    authoring_mode: str | None
    recipe: str | None
    processes: tuple[PipelineProcessSummary, ...]
    quality_gates: tuple[dict[str, Any], ...]
    schedule: str | None
    artifact_state: dict[str, Any]
    support: dict[str, Any] | None
    certification: dict[str, Any] | None
    next_actions: tuple[PipelineNextAction, ...]
    errors: tuple[dict[str, Any], ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": PIPELINE_SUMMARY_SCHEMA,
            "id": self.id,
            "source_path": self.source_path,
            "valid": self.valid,
            "operational_status": self.operational_status,
            "authoring_mode": self.authoring_mode,
            "recipe": self.recipe,
            "processes": [item.to_dict() for item in self.processes],
            "quality_gates": list(self.quality_gates),
            "schedule": self.schedule,
            "artifact_state": dict(self.artifact_state),
            "support": self.support,
            "certification": self.certification,
            "next_actions": [item.to_dict() for item in self.next_actions],
            "errors": list(self.errors),
        }


__all__ = [
    "PIPELINE_SUMMARY_SCHEMA",
    "PipelineNextAction",
    "PipelineProcessSummary",
    "PipelineSummary",
]
