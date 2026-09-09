"""Runtime observability primitives: metrics, Prometheus export, errors."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime

from dpone._compat import UTC


def _now() -> str:
    return datetime.now(UTC).isoformat()


@dataclass(frozen=True)
class ErrorClassification:
    category: str
    retryable: bool
    reason: str


class ErrorClassifier:
    TRANSIENT = ("timeout", "temporarily", "connection reset", "deadlock", "throttle", "rate limit")
    AUTH = ("permission denied", "access denied", "unauthorized", "forbidden", "login failed")
    DATA = ("truncation", "cannot convert", "invalid input", "duplicate key", "constraint")

    @classmethod
    def classify(cls, exc: BaseException) -> ErrorClassification:
        message = str(exc).lower()
        if any(token in message for token in cls.AUTH):
            return ErrorClassification("authorization", False, "credential or permission failure")
        if any(token in message for token in cls.TRANSIENT):
            return ErrorClassification("transient", True, "retryable infrastructure failure")
        if any(token in message for token in cls.DATA):
            return ErrorClassification("data_quality", False, "data or schema violation")
        return ErrorClassification("unknown", False, "unclassified failure")


@dataclass
class StageMetrics:
    stage: str
    rows: int
    bytes_processed: int
    duration_seconds: float
    status: str = "success"

    @property
    def rows_per_second(self) -> float:
        return round(self.rows / self.duration_seconds, 2) if self.duration_seconds > 0 else 0.0

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["rows_per_second"] = self.rows_per_second
        return payload


@dataclass
class PipelineRunMetrics:
    run_id: str
    pipeline: str
    started_at: str = field(default_factory=_now)
    stages: list[StageMetrics] = field(default_factory=list)

    def record_stage(
        self, stage: str, *, rows: int, bytes_processed: int = 0, duration_seconds: float, status: str
    ) -> None:
        self.stages.append(StageMetrics(stage, int(rows), int(bytes_processed), float(duration_seconds), status))

    @property
    def total_rows(self) -> int:
        return sum(stage.rows for stage in self.stages)

    def to_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "pipeline": self.pipeline,
            "started_at": self.started_at,
            "total_rows": self.total_rows,
            "stages": [stage.to_dict() for stage in self.stages],
        }

    def to_prometheus(self) -> str:
        lines: list[str] = []
        for stage in self.stages:
            labels = f'pipeline="{self.pipeline}",run_id="{self.run_id}",stage="{stage.stage}",status="{stage.status}"'
            lines.append(f"dpone_stage_rows{{{labels}}} {stage.rows}")
            lines.append(f"dpone_stage_duration_seconds{{{labels}}} {stage.duration_seconds}")
            lines.append(f"dpone_stage_rows_per_second{{{labels}}} {stage.rows_per_second}")
        return "\n".join(lines) + ("\n" if lines else "")
