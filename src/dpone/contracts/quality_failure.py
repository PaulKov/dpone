"""Typed, redaction-safe outcome for a blocking quality-gate failure."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from dpone.governance.quality import QualityGateReport

QualityFailureBoundary = Literal["pre_commit", "post_commit", "resume_validation"]
QualityTargetState = Literal["not_mutated", "mutation_returned_success", "unknown"]
QualityCheckpointState = Literal["not_advanced", "committed", "not_applicable", "unknown"]
QualitySourceState = Literal["not_advanced", "unknown"]
QualityRetryClassification = Literal[
    "retry_before_target_mutation",
    "retry_via_native_resume",
    "retry_may_repeat_target_mutation",
    "operator_verification_required",
]

_FAILURE_BOUNDARIES = frozenset({"pre_commit", "post_commit", "resume_validation"})
_TARGET_STATES = frozenset({"not_mutated", "mutation_returned_success", "unknown"})
_CHECKPOINT_STATES = frozenset({"not_advanced", "committed", "not_applicable", "unknown"})
_SOURCE_STATES = frozenset({"not_advanced", "unknown"})
_RETRY_CLASSIFICATIONS = frozenset(
    {
        "retry_before_target_mutation",
        "retry_via_native_resume",
        "retry_may_repeat_target_mutation",
        "operator_verification_required",
    }
)


@dataclass(frozen=True, slots=True)
class QualityGateFailureOutcome:
    """Safe facts known by the coordinator that owns the mutation boundary."""

    failure_boundary: QualityFailureBoundary
    target_state: QualityTargetState
    checkpoint_state: QualityCheckpointState
    source_state: QualitySourceState
    retry_classification: QualityRetryClassification
    inserted_rows: int | None = None
    updated_rows: int | None = None
    final_rows: int | None = None
    extracted_rows: int | None = None
    attempts: int = 1

    def __post_init__(self) -> None:
        _require_member("failure_boundary", self.failure_boundary, _FAILURE_BOUNDARIES)
        _require_member("target_state", self.target_state, _TARGET_STATES)
        _require_member("checkpoint_state", self.checkpoint_state, _CHECKPOINT_STATES)
        _require_member("source_state", self.source_state, _SOURCE_STATES)
        _require_member(
            "retry_classification",
            self.retry_classification,
            _RETRY_CLASSIFICATIONS,
        )
        for field_name in ("inserted_rows", "updated_rows", "final_rows", "extracted_rows"):
            object.__setattr__(
                self,
                field_name,
                _canonical_non_negative_int(getattr(self, field_name)),
            )
        object.__setattr__(self, "attempts", _positive_attempts(self.attempts))

    @classmethod
    def from_load_result(
        cls,
        *,
        failure_boundary: QualityFailureBoundary,
        target_state: QualityTargetState,
        checkpoint_state: QualityCheckpointState,
        source_state: QualitySourceState,
        retry_classification: QualityRetryClassification,
        load_result: object,
        attempts: int = 1,
    ) -> QualityGateFailureOutcome:
        """Capture only public counters already owned by a real ``LoadResult``."""

        return cls(
            failure_boundary=failure_boundary,
            target_state=target_state,
            checkpoint_state=checkpoint_state,
            source_state=source_state,
            retry_classification=retry_classification,
            inserted_rows=getattr(load_result, "inserted_rows", None),
            updated_rows=getattr(load_result, "updated_rows", None),
            final_rows=getattr(load_result, "total_rows", None),
            extracted_rows=getattr(load_result, "staging_rows", None),
            attempts=attempts,
        )

    @classmethod
    def from_post_commit_result(
        cls,
        load_result: object,
        *,
        native: bool,
        checkpoint_committed: bool,
    ) -> QualityGateFailureOutcome:
        """Describe a returned target mutation without claiming extra durability."""

        checkpoint_state: QualityCheckpointState = (
            "committed" if checkpoint_committed else "unknown" if native else "not_applicable"
        )
        retry_classification: QualityRetryClassification = (
            "retry_via_native_resume"
            if checkpoint_committed
            else "operator_verification_required"
            if native
            else "retry_may_repeat_target_mutation"
        )
        return cls.from_load_result(
            failure_boundary="post_commit",
            target_state="mutation_returned_success",
            checkpoint_state=checkpoint_state,
            source_state="not_advanced",
            retry_classification=retry_classification,
            load_result=load_result,
        )

    @classmethod
    def from_resume_result(
        cls,
        load_result: object,
        *,
        native: bool,
    ) -> QualityGateFailureOutcome:
        """Describe sink-free resume validation without inventing a mutation."""

        return cls.from_load_result(
            failure_boundary="resume_validation",
            target_state="not_mutated",
            checkpoint_state="committed" if native else "unknown",
            source_state="not_advanced",
            retry_classification=("retry_via_native_resume" if native else "operator_verification_required"),
            load_result=load_result,
        )

    def failure_context(self) -> dict[str, str]:
        """Return the additive public context without counters or diagnostics."""

        return {
            "failure_boundary": self.failure_boundary,
            "target_state": self.target_state,
            "checkpoint_state": self.checkpoint_state,
            "source_state": self.source_state,
            "retry_classification": self.retry_classification,
        }

    def to_jsonable(self) -> dict[str, object]:
        """Render the complete bounded outcome without connector diagnostics."""

        return {
            **self.failure_context(),
            "inserted_rows": self.inserted_rows,
            "updated_rows": self.updated_rows,
            "final_rows": self.final_rows,
            "extracted_rows": self.extracted_rows,
            "attempts": self.attempts,
        }

    def result_fields(self) -> dict[str, object]:
        """Return fields that replace synthetic counters in a failed run result."""

        return {
            "inserted_rows": self.inserted_rows,
            "updated_rows": self.updated_rows,
            "final_rows": self.final_rows,
            "extracted_rows": self.extracted_rows,
            "failure_context": self.failure_context(),
        }


class QualityGateFailure(RuntimeError):
    """A blocking quality policy failed with an optional authoritative outcome."""

    code = "DPONE_QUALITY_GATES_FAILED"

    def __init__(
        self,
        report: QualityGateReport,
        outcome: QualityGateFailureOutcome | None = None,
    ) -> None:
        self.report = report
        self.outcome = outcome
        super().__init__("quality gates failed")


class QualityGateReceiptError(RuntimeError):
    """Stable receipt-validation failure with optional mutation-boundary facts."""

    code = "DPONE_QUALITY_GATE_RECEIPT_INVALID"
    default_message = "quality gate receipt is invalid"

    def __init__(
        self,
        message: str | None = None,
        *,
        outcome: QualityGateFailureOutcome | None = None,
    ) -> None:
        self.outcome = outcome
        super().__init__(message or self.default_message)

    def with_outcome(self, outcome: QualityGateFailureOutcome) -> QualityGateReceiptError:
        """Copy this safe failure while attaching coordinator-owned outcome facts."""

        return type(self)(str(self), outcome=outcome)


class QualityGateReceiptRequired(QualityGateReceiptError):
    """A non-empty current policy has no runner-bound producer receipt."""

    code = "DPONE_QUALITY_GATE_RECEIPT_REQUIRED"
    default_message = "quality gate receipt is required"


class QualityGateReceiptMismatch(QualityGateReceiptError):
    """Receipt binding or result coverage differs from the current policy."""

    code = "DPONE_QUALITY_GATE_RECEIPT_MISMATCH"
    default_message = "quality gate receipt does not match current policy"


class QualityGateReceiptInvalid(QualityGateReceiptError):
    """Receipt structure contains an unsupported or malformed value."""

    code = "DPONE_QUALITY_GATE_RECEIPT_INVALID"
    default_message = "quality gate receipt is invalid"


def _canonical_non_negative_int(value: object) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def _positive_attempts(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        return 1
    return value


def _require_member(field: str, value: object, allowed: frozenset[str]) -> None:
    if not isinstance(value, str) or value not in allowed:
        raise ValueError(f"{field} has an unsupported value")


__all__ = [
    "QualityCheckpointState",
    "QualityFailureBoundary",
    "QualityGateFailure",
    "QualityGateFailureOutcome",
    "QualityGateReceiptError",
    "QualityGateReceiptInvalid",
    "QualityGateReceiptMismatch",
    "QualityGateReceiptRequired",
    "QualityRetryClassification",
    "QualitySourceState",
    "QualityTargetState",
]
