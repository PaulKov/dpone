"""Durable per-model publication records used by workflow summarization."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True, slots=True)
class DurableSemanticRefreshModelPublication:
    """One model journal projection read from durable control state."""

    operation_id: str
    operation_plan_sha256: str
    workflow_execution_binding_sha256: str
    attempt_binding_sha256: str
    status: str
    artifact_manifest_sha256: str | None
    clickhouse_terminal_receipt_sha256: str | None
    terminal_receipt_sha256: str | None
    target_generation: int | None
    scope_revision: int | None

    def __post_init__(self) -> None:
        for field_name in (
            "operation_id",
            "operation_plan_sha256",
            "workflow_execution_binding_sha256",
            "attempt_binding_sha256",
        ):
            _digest(getattr(self, field_name), field_name)
        if self.status == "COMPLETE":
            for field_name in (
                "artifact_manifest_sha256",
                "clickhouse_terminal_receipt_sha256",
                "terminal_receipt_sha256",
            ):
                _digest(getattr(self, field_name), field_name)
            _positive(self.target_generation, "target_generation")
            _positive(self.scope_revision, "scope_revision")
        elif self.status not in {
            "PREPARING",
            "PREPARED",
            "COMMITTING",
            "TARGET_COMMITTED",
            "FAILED_PRE_COMMIT",
            "COMMIT_UNKNOWN",
            "COMMITTED_INCOMPLETE",
        }:
            raise ValueError("durable model publication status is unsupported")
        else:
            for field_name in (
                "artifact_manifest_sha256",
                "clickhouse_terminal_receipt_sha256",
                "terminal_receipt_sha256",
            ):
                value = getattr(self, field_name)
                if value is not None:
                    _digest(value, field_name)
            for field_name in ("target_generation", "scope_revision"):
                value = getattr(self, field_name)
                if value is not None:
                    _positive(value, field_name)


class SemanticRefreshWorkflowPublicationPort(Protocol):
    """Read the exact model journal closure for one logical workflow execution."""

    def read(
        self,
        *,
        workflow_execution_id: str,
        workflow_execution_binding_sha256: str,
        expected_operation_ids: tuple[str, ...],
    ) -> tuple[DurableSemanticRefreshModelPublication, ...]:
        """Return canonical operation order or fail closed."""


def _digest(value: object, field_name: str) -> str:
    if (
        not isinstance(value, str)
        or not value.startswith("sha256:")
        or len(value) != 71
        or any(character not in "0123456789abcdef" for character in value[7:])
    ):
        raise ValueError(f"{field_name} must be a canonical lowercase sha256 digest")
    return value


def _positive(value: object, field_name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise ValueError(f"{field_name} must be a positive integer")
    return value


__all__ = [
    "DurableSemanticRefreshModelPublication",
    "SemanticRefreshWorkflowPublicationPort",
]
