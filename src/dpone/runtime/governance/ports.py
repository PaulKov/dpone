"""Connector-neutral load governance ports.

The runtime uses these small contracts to keep bulk/native finalization out of
source adapters, sink adapters, and Airflow glue. A sink may stage data first,
let governance project lineage and run quality gates, and only then mutate the
target table.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

from dpone.runtime.governance.validation_snapshot import (
    snapshot_staged_validation_values,
)


@dataclass(frozen=True, slots=True)
class StagedLoadHandle:
    """A sink-owned staged dataset awaiting governance and finalization."""

    staging_config: Any
    payload_schema: Sequence[tuple[str, str]]
    staged_rows: int
    finalization_config: Any | None = None
    decoded_config: Any | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class LineageProjectionResult:
    """Result of projecting dpone lineage columns into a staged dataset."""

    handle: StagedLoadHandle
    projected: bool
    columns: Sequence[str] = ()
    row_identity_mode: str = "off"
    warnings: tuple[str, ...] = ()
    evidence: Mapping[str, Any] = field(default_factory=dict)

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "kind": "dpone.runtime.lineage_projection.v1",
            "projected": self.projected,
            "columns": list(self.columns),
            "row_identity_mode": self.row_identity_mode,
            "warnings": list(self.warnings),
            "evidence": dict(self.evidence),
        }


class StagedLoadPort(Protocol):
    """Sink port for stage -> govern -> finalize loading."""

    def stage_payload(self, load_config: Any, payload: Any) -> StagedLoadHandle: ...

    def finalize_staged_load(
        self,
        load_config: Any,
        staged: StagedLoadHandle | StagedLoadValidationReceipt,
    ) -> Any: ...

    def abort_staged_load(self, handle: StagedLoadHandle) -> None:
        """Rollback the attempt and clean every attempt-local resource."""
        ...


class StagedLoadValidationPort(Protocol):
    """Optional sink capability for gates that run after projection/quality."""

    def validate_staged_load(self, load_config: Any, handle: StagedLoadHandle) -> Any: ...


@dataclass(frozen=True, slots=True)
class StagedLoadValidationReceipt:
    """Bind a completed optional validation to one sink, config, and handle."""

    handle: StagedLoadHandle
    validated: bool
    _sink: Any = field(repr=False, compare=False)
    _load_config: Any = field(repr=False, compare=False)
    _validated_inputs: tuple[Any, StagedLoadHandle] | None = field(repr=False, compare=False)
    _validation_token: Any = field(repr=False, compare=False)

    def assert_bound_to(self, *, sink: Any, load_config: Any) -> None:
        """Reject cross-lifecycle use before target finalization."""

        if not self.validated or self._sink is not sink or self._load_config is not load_config:
            raise ValueError("staged_load_validation_receipt_invalid")

    def frozen_inputs(self, *, sink: Any, load_config: Any) -> tuple[Any, Any, StagedLoadHandle]:
        """Return the sink token and isolated copies that passed validation."""

        self.assert_bound_to(sink=sink, load_config=load_config)
        if self._validated_inputs is None:
            raise ValueError("staged_load_validation_receipt_invalid")
        validated_config, validated_handle = snapshot_staged_validation_values(
            *self._validated_inputs,
        )
        return self._validation_token, validated_config, validated_handle


class StagedLoadPostCommitCleanupError(RuntimeError):
    """Stable non-retryable failure after the target commit was confirmed."""

    code = "staged_cleanup_failed"

    def __init__(self, cleanup_error: BaseException, handle: Any) -> None:
        super().__init__("staged cleanup failed after confirmed target commit")
        self.details = staged_load_post_commit_cleanup_details(cleanup_error, handle)


def validate_staged_load_if_supported(
    sink: Any,
    load_config: Any,
    handle: StagedLoadHandle,
) -> StagedLoadValidationReceipt:
    """Run a sink-owned staged-data gate at the pre-target boundary."""

    validator = getattr(sink, "validate_staged_load", None)
    validated = callable(validator)
    validated_inputs = snapshot_staged_validation_values(load_config, handle) if validated else None
    validation_token = validator(*validated_inputs) if validator is not None and validated_inputs is not None else None
    return StagedLoadValidationReceipt(
        handle=handle,
        validated=validated,
        _sink=sink,
        _load_config=load_config,
        _validated_inputs=validated_inputs,
        _validation_token=validation_token,
    )


def finalize_staged_load_after_validation(
    sink: Any,
    load_config: Any,
    receipt: StagedLoadValidationReceipt,
) -> Any:
    """Finalize without repeating a gate already run by the coordinator."""

    if receipt.validated and getattr(sink, "supports_staged_validation_receipts", False) is True:
        receipt.assert_bound_to(sink=sink, load_config=load_config)
        return sink.finalize_staged_load(load_config, receipt)
    return sink.finalize_staged_load(load_config, receipt.handle)


def staged_load_handle_details(handle: Any) -> dict[str, Any]:
    """Project a staged handle into safe operational evidence."""

    staging_config = getattr(handle, "staging_config", None)
    finalization_config = getattr(handle, "finalization_config", None)
    decoded_config = getattr(handle, "decoded_config", None)
    return {
        "staged_rows": getattr(handle, "staged_rows", None),
        "schema_columns": len(getattr(handle, "payload_schema", ()) or ()),
        "operation_schema": getattr(staging_config, "target_schema", None),
        "staging_table": getattr(staging_config, "target_table", None),
        "finalization_schema": getattr(finalization_config, "target_schema", None),
        "finalization_table": getattr(finalization_config, "target_table", None),
        "decoded_schema": getattr(decoded_config, "target_schema", None),
        "decoded_table": getattr(decoded_config, "target_table", None),
        "metadata": dict(getattr(handle, "metadata", {}) or {}),
    }


def staged_load_failure_details(
    error: BaseException,
    handle: Any,
    *,
    cleanup_status: str,
) -> dict[str, Any]:
    """Describe a failed-before-target staged attempt without losing its code."""

    metadata = dict(getattr(handle, "metadata", {}) or {})
    operation_tables = metadata.get("operation_tables")
    return {
        "failure_boundary": "pre_commit",
        "target_outcome": "failed_before_target",
        "error_code": getattr(error, "code", type(error).__name__),
        "operation_tables": dict(operation_tables) if isinstance(operation_tables, Mapping) else {},
        "cleanup_attempted": True,
        "cleanup_status": cleanup_status,
        "cleanup_verification_required": True,
    }


def staged_load_commit_unknown_details(error: BaseException, handle: Any) -> dict[str, Any]:
    """Describe a target invocation whose commit outcome needs reconciliation."""

    metadata = dict(getattr(handle, "metadata", {}) or {})
    operation_tables = metadata.get("operation_tables")
    return {
        "failure_boundary": "commit_unknown",
        "target_outcome": "commit_unknown",
        "error_code": getattr(error, "code", type(error).__name__),
        "operation_tables": dict(operation_tables) if isinstance(operation_tables, Mapping) else {},
        "cleanup_attempted": False,
        "cleanup_status": "retained_for_reconciliation",
        "cleanup_verification_required": True,
        "safe_to_retry": False,
    }


def staged_load_post_commit_cleanup_details(error: BaseException, handle: Any) -> dict[str, Any]:
    """Describe failed staging cleanup after a confirmed target commit."""

    metadata = dict(getattr(handle, "metadata", {}) or {})
    operation_tables = metadata.get("operation_tables")
    return {
        "failure_boundary": "post_commit",
        "target_outcome": "committed",
        "error_code": StagedLoadPostCommitCleanupError.code,
        "cleanup_error_type": type(error).__name__,
        "operation_tables": dict(operation_tables) if isinstance(operation_tables, Mapping) else {},
        "cleanup_attempted": True,
        "cleanup_status": "failed",
        "cleanup_verification_required": True,
        "safe_to_retry": False,
    }


def abort_staged_load_preserving_primary(sink: Any, handle: Any) -> str:
    """Attempt abort without replacing the primary error."""

    try:
        sink.abort_staged_load(handle)
    except Exception:
        return "failed"
    return "succeeded"


class SinkSideLineageProjector(Protocol):
    """Projects lineage into staged data without Python row materialization."""

    def project(
        self,
        *,
        load_config: Any,
        handle: StagedLoadHandle,
        lineage_options: Any,
        load_record: Any,
    ) -> LineageProjectionResult: ...


class NoopSinkSideLineageProjector:
    """Projection fallback for sinks or manifests with lineage disabled."""

    def project(
        self,
        *,
        load_config: Any,
        handle: StagedLoadHandle,
        lineage_options: Any,
        load_record: Any,
    ) -> LineageProjectionResult:
        del load_config, lineage_options, load_record
        return LineageProjectionResult(handle=handle, projected=False)


__all__ = [
    "LineageProjectionResult",
    "NoopSinkSideLineageProjector",
    "SinkSideLineageProjector",
    "StagedLoadHandle",
    "StagedLoadPostCommitCleanupError",
    "StagedLoadPort",
    "StagedLoadValidationReceipt",
    "StagedLoadValidationPort",
    "abort_staged_load_preserving_primary",
    "finalize_staged_load_after_validation",
    "staged_load_commit_unknown_details",
    "staged_load_failure_details",
    "staged_load_handle_details",
    "staged_load_post_commit_cleanup_details",
    "validate_staged_load_if_supported",
]
