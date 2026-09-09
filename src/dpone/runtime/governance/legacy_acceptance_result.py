"""Post-commit result projection for legacy acceptance coordination."""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING, Any

from dpone.runtime.native_transfer_quality_evidence import (
    normalize_native_quality_scope_summary,
)

if TYPE_CHECKING:
    from dpone.runtime.governance.acceptance_metrics import AcceptanceMetricRun
    from dpone.runtime.governance.service import QualityGateReceipt


def with_acceptance_metrics(
    load_result: Any,
    run: AcceptanceMetricRun,
    *,
    governance_finalization: str | None = None,
    quality_receipt: QualityGateReceipt | None = None,
    quality_evidence: dict[str, object] | None = None,
    scope_summary: dict[str, object] | None = None,
) -> Any:
    """Project acceptance and quality evidence onto one immutable result."""

    metrics = {
        **(getattr(load_result, "reconciliation_metrics", None) or {}),
        **run.metrics(load_result),
    }
    if governance_finalization is not None:
        metrics["governance_finalization"] = governance_finalization
    normalized_scope = (
        dict(quality_receipt.scope_summary)
        if quality_receipt is not None and quality_receipt.scope_summary is not None
        else normalize_native_quality_scope_summary(scope_summary)
    )
    if normalized_scope is not None:
        metrics["native_transfer_quality_scope"] = normalized_scope
    if quality_receipt is None:
        return replace(load_result, reconciliation_metrics=metrics)
    metrics["quality_gates"] = quality_evidence or {}
    return replace(
        load_result,
        reconciliation_metrics=metrics,
        quality_gate_receipt=quality_receipt,
    )


def quality_scope_summary(quality_snapshots: Any | None) -> dict[str, object] | None:
    """Return the canonical native quality scope, when one exists."""

    if quality_snapshots is None:
        return None
    return quality_snapshots.scope_summary.to_jsonable()


def mark_blocking_post_commit_failure(error: Exception) -> None:
    """Keep a required acceptance failure retry-visible after target commit."""

    error.blocks_committed_success = True  # type: ignore[attr-defined]


__all__ = [
    "mark_blocking_post_commit_failure",
    "quality_scope_summary",
    "with_acceptance_metrics",
]
