"""Input boundary for local, plan-first Airflow rerun preparation."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol


class AirflowRerunPlanInputError(RuntimeError):
    """Structured input failure independent from a concrete filesystem adapter."""

    def __init__(self, code: str, message: str, *, path: str) -> None:
        super().__init__(message)
        self.code = code
        self.path = path

    def to_dict(self) -> dict[str, str]:
        return {
            "schema": "dpone.error.v1",
            "code": self.code,
            "stage": "airflow_rerun_plan",
            "severity": "error",
            "message": str(self),
            "path": self.path,
        }


@dataclass(frozen=True, slots=True)
class AirflowRerunPlanInputs:
    evidence_path: Path
    evidence: dict[str, Any]
    index_path: Path
    current_index: dict[str, Any]
    cache_root: Path


class AirflowRerunPlanInputPort(Protocol):
    def load(
        self,
        *,
        evidence_path: str | Path,
        current_index_path: str | Path,
        cache_root: str | Path | None,
    ) -> AirflowRerunPlanInputs: ...

    def original_artifacts_availability(
        self,
        cache_root: Path,
        *,
        release_id: str,
        deployment_id: str,
    ) -> tuple[bool, str]: ...


__all__ = [
    "AirflowRerunPlanInputError",
    "AirflowRerunPlanInputPort",
    "AirflowRerunPlanInputs",
]
