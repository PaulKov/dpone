"""Runtime orchestration for safe sample execution and evidence persistence."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

from dpone.services.safe_sample_redaction import contains_sensitive_assignment

if TYPE_CHECKING:
    from dpone.services.safe_sample_execution_plan import SafeSampleExecutionPlan
    from dpone.services.safe_sample_runtime_evidence import SafeSampleRuntimeEvidenceWriteReport
    from dpone.services.safe_sample_runtime_executor import SafeSampleRuntimeExecutionResult


class SafeSampleRuntimeExecutorPort(Protocol):
    """Executor boundary for safe sample runtime orchestration."""

    def execute(self, plan: SafeSampleExecutionPlan) -> SafeSampleRuntimeExecutionResult:
        """Execute the runtime prelude and data-copy boundary."""


class SafeSampleRuntimeEvidenceWriterPort(Protocol):
    """Evidence writer boundary for safe sample runtime orchestration."""

    def write(
        self,
        result: SafeSampleRuntimeExecutionResult,
        output_dir: str | Path,
    ) -> SafeSampleRuntimeEvidenceWriteReport:
        """Persist runtime evidence and return write metadata."""


@dataclass(frozen=True, slots=True)
class SafeSampleRuntimeRunReport:
    release_id: str | None
    deployment_id: str | None
    execution_status: str
    data_outcome: str
    runtime_execution: dict[str, Any]
    evidence_write: dict[str, Any] | None
    errors: tuple[dict[str, Any], ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": "dpone.safe-sample-runtime-run.v1",
            "release_id": self.release_id,
            "deployment_id": self.deployment_id,
            "execution_status": self.execution_status,
            "data_outcome": self.data_outcome,
            "runtime_execution": dict(self.runtime_execution),
            "evidence_write": dict(self.evidence_write) if self.evidence_write is not None else None,
            "errors": [dict(error) for error in self.errors],
        }


class SafeSampleRuntimeRunner:
    """Execute a safe sample plan and atomically persist its runtime evidence."""

    def __init__(
        self,
        *,
        executor: SafeSampleRuntimeExecutorPort,
        evidence_writer: SafeSampleRuntimeEvidenceWriterPort,
    ) -> None:
        self._executor = executor
        self._evidence_writer = evidence_writer

    def run(self, plan: SafeSampleExecutionPlan, *, output_dir: str | Path) -> SafeSampleRuntimeRunReport:
        execution = self._executor.execute(plan)
        runtime_execution = execution.to_dict()
        errors = _errors(runtime_execution)
        evidence_write: dict[str, Any] | None = None
        execution_status = str(runtime_execution.get("execution_status") or "failed")
        try:
            evidence_write = self._evidence_writer.write(execution, output_dir).to_dict()
        except Exception as exc:  # noqa: BLE001 - runner must emit stable runtime evidence errors.
            execution_status = "failed"
            errors.append(
                _error(
                    "DPONE_SAFE_SAMPLE_EVIDENCE_WRITE_FAILED",
                    "Safe sample runtime evidence could not be written: " + _safe_error_message(exc),
                )
            )
        return SafeSampleRuntimeRunReport(
            release_id=_optional_string(runtime_execution.get("release_id")),
            deployment_id=_optional_string(runtime_execution.get("deployment_id")),
            execution_status=execution_status,
            data_outcome=str(runtime_execution.get("data_outcome") or "unknown"),
            runtime_execution=runtime_execution,
            evidence_write=evidence_write,
            errors=tuple(errors),
        )


def _errors(payload: dict[str, Any]) -> list[dict[str, Any]]:
    raw_errors = payload.get("errors")
    if not isinstance(raw_errors, list):
        return []
    return [dict(error) for error in raw_errors if isinstance(error, dict)]


def _optional_string(value: Any) -> str | None:
    text = str(value or "")
    return text or None


def _safe_error_message(exc: Exception) -> str:
    message = " ".join(str(exc).split()) or exc.__class__.__name__
    if _contains_sensitive_token(message):
        return "details redacted; check runtime evidence output path and storage permissions."
    return message[:500]


def _contains_sensitive_token(message: str) -> bool:
    return contains_sensitive_assignment(message)


def _error(code: str, message: str) -> dict[str, Any]:
    return {
        "schema": "dpone.error.v1",
        "code": code,
        "stage": "safe_sample_runtime_runner",
        "severity": "error",
        "message": message,
        "fixes": [],
    }


__all__ = [
    "SafeSampleRuntimeEvidenceWriterPort",
    "SafeSampleRuntimeExecutorPort",
    "SafeSampleRuntimeRunReport",
    "SafeSampleRuntimeRunner",
]
