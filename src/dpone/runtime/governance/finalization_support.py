"""Value and observation helpers for governed staged-load finalization."""

from __future__ import annotations

from dataclasses import replace
from typing import Any

from dpone.runtime.governance.acceptance_metrics import (
    AcceptanceEvidenceContext,
    AcceptanceMetricRun,
    AcceptanceMetricsRecorder,
)
from dpone.runtime.governance.ports import StagedLoadPostCommitCleanupError
from dpone.runtime.governance.service import LoadGovernanceService, QualityGateReceipt
from dpone.runtime.process_io import add_exception_note
from dpone.runtime.sinks.load_result import LoadResult


def staged_probe_result(handle: Any) -> LoadResult:
    """Build the immutable row-count view consumed by pre-commit quality."""

    staged_rows = validated_staged_rows(handle)
    return LoadResult(
        inserted_rows=staged_rows,
        updated_rows=0,
        total_rows=staged_rows,
        staging_rows=staged_rows,
    )


def validated_staged_rows(handle: Any) -> int:
    """Return a trustworthy non-negative staged row count."""

    staged_rows = getattr(handle, "staged_rows", None)
    if isinstance(staged_rows, bool) or not isinstance(staged_rows, int) or staged_rows < 0:
        raise ValueError("staged_rows must be a non-negative integer")
    return staged_rows


def with_governance_metrics(
    load_result: Any,
    projection: Any,
    quality_receipt: QualityGateReceipt,
    quality_evidence: dict[str, object],
    acceptance_metrics: dict[str, Any] | None = None,
) -> Any:
    """Attach bounded governance results to a successful load result."""

    metrics = {
        **(getattr(load_result, "reconciliation_metrics", None) or {}),
        "governance_finalization": "pre_finalize",
        "lineage_projection": projection.to_jsonable(),
        "quality_gates": quality_evidence,
        **(acceptance_metrics or {}),
    }
    if quality_receipt.scope_summary is not None:
        metrics["native_transfer_quality_scope"] = dict(quality_receipt.scope_summary)
    return replace(
        load_result,
        reconciliation_metrics=metrics,
        quality_gate_receipt=quality_receipt,
    )


def load_result_details(load_result: Any) -> dict[str, Any]:
    """Project a load result into bounded audit evidence."""

    return {
        "inserted_rows": getattr(load_result, "inserted_rows", None),
        "updated_rows": getattr(load_result, "updated_rows", None),
        "staging_rows": getattr(load_result, "staging_rows", None),
        "total_rows": getattr(load_result, "total_rows", None),
        "replaced_rows": getattr(load_result, "replaced_rows", None),
    }


def classify_post_commit_cleanup_failure(
    cleanup_error: Exception,
    handle: Any,
    primary: Exception | None,
) -> StagedLoadPostCommitCleanupError:
    """Classify cleanup after commit without replacing an earlier failure."""

    classified = StagedLoadPostCommitCleanupError(cleanup_error, handle)
    if primary is not None:
        add_exception_note(primary, f"post-commit staging cleanup failed: {type(cleanup_error).__name__}")
    return classified


def capture_acceptance(
    recorder: AcceptanceMetricsRecorder,
    governance_service: LoadGovernanceService,
    run: AcceptanceMetricRun,
    *,
    sides: tuple[str, ...],
    boundary: str,
    source: Any | None,
    sink: Any,
    load_config: Any,
    extract_result: Any,
    staged_handle: Any,
    load_record: Any,
) -> None:
    """Capture requested physical sides with the same evidence boundary."""
    for side in sides:
        recorder.capture(
            run,
            side=side,
            load_config=load_config,
            extract_result=extract_result,
            payload_schema=staged_handle.payload_schema,
            source=source,
            sink=sink,
            evidence=AcceptanceEvidenceContext(load_record, governance_service, boundary),
            staged_handle=staged_handle,
        )


__all__ = [
    "capture_acceptance",
    "classify_post_commit_cleanup_failure",
    "load_result_details",
    "staged_probe_result",
    "validated_staged_rows",
    "with_governance_metrics",
]
