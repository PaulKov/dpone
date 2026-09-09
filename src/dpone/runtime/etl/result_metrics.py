from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.governance.quality import QualityGateReport


from collections.abc import Mapping
from typing import Any

from dpone.contracts.quality_failure import (
    QualityGateFailure,
    QualityGateFailureOutcome,
    QualityGateReceiptError,
)
from dpone.governance.quality import QualityGatePolicy
from dpone.runtime.governance.service import report_from_staged_quality_receipt


def initial_etl_result() -> dict[str, Any]:
    return {
        "extracted_rows": 0,
        "loaded_rows": 0,
        "inserted_rows": 0,
        "updated_rows": 0,
        "final_rows": 0,
        "errors": [],
        "status": "pending",
        "run_id": None,
    }


def etl_start_payload(load_config: Any) -> dict[str, Any]:
    return {
        "source_schema": load_config.source_schema,
        "source_table": load_config.source_table,
        "target_schema": load_config.target_schema,
        "target_table": load_config.target_table,
        "load_strategy": load_config.load_strategy.value,
        "unique_key": load_config.unique_key,
        "batch_size": load_config.batch_size,
        "custom_predicate": load_config.custom_predicate,
    }


def staged_quality_gate_report(
    load_result: Any,
    *,
    load_config: Any | None = None,
) -> QualityGateReport | None:
    """Return only validated staged producer evidence, never mapping-shaped claims."""

    receipt = getattr(load_result, "quality_gate_receipt", None)
    if load_config is None:
        return report_from_staged_quality_receipt(receipt)
    options = getattr(load_config, "options", None)
    quality = options.get("quality") if isinstance(options, Mapping) else None
    return report_from_staged_quality_receipt(
        receipt,
        QualityGatePolicy.from_config(quality),
    )


def enrich_post_commit_quality_failure_result(result: dict[str, Any], exc: BaseException) -> None:
    """Copy only an authoritative post-commit outcome into the processor result."""

    if not isinstance(exc, QualityGateFailure | QualityGateReceiptError):
        return
    outcome = exc.outcome
    if not isinstance(outcome, QualityGateFailureOutcome) or outcome.failure_boundary != "post_commit":
        return
    result.update(outcome.result_fields())
    result["attempts"] = outcome.attempts


def populate_success_result(
    result: dict[str, Any],
    load_result: Any,
    *,
    validation_info: dict[str, Any] | None,
    reconciliation_metrics: dict[str, Any] | None,
) -> None:
    """Publish one canonical successful-load metric set."""

    loaded_rows = (load_result.replaced_rows or 0) if load_result.replaced_rows else load_result.inserted_rows
    result.update(
        {
            "extracted_rows": load_result.staging_rows or 0,
            "loaded_rows": loaded_rows,
            "inserted_rows": load_result.inserted_rows,
            "updated_rows": load_result.updated_rows,
            "total_rows": load_result.total_rows,
            "final_rows": load_result.total_rows,
            "staging_rows": load_result.staging_rows or 0,
            "soft_deleted_rows": load_result.soft_deleted_rows or 0,
            "reactivated_rows": load_result.reactivated_rows or 0,
            "unchanged_rows": load_result.unchanged_rows or 0,
            "hard_deleted_rows": load_result.hard_deleted_rows or 0,
            "active_rows": load_result.active_rows,
            "commit_receipt_id": load_result.commit_receipt_id,
            "commit_outcome": load_result.commit_outcome,
            "replaced_rows": load_result.replaced_rows or 0,
            "deleted_lookback_rows": load_result.deleted_lookback_rows or 0,
            "status": "success",
            "validation_info": validation_info,
            "reconciliation_metrics": reconciliation_metrics,
        }
    )


def with_runtime_metrics(
    load_result: Any,
    runtime_metrics: dict[str, Any],
    evidence_path: str | None,
) -> Any:
    """Attach lifecycle metrics without replacing protected producer evidence."""

    existing = load_result.reconciliation_metrics or {}
    protected = {"quality_gates", "native_transfer_quality_scope"}
    runtime_metrics = {
        key: value for key, value in runtime_metrics.items() if key not in protected or key not in existing
    }
    if evidence_path:
        runtime_metrics["data_contract_evidence"] = evidence_path
    return (
        replace(load_result, reconciliation_metrics={**existing, **runtime_metrics}) if runtime_metrics else load_result
    )
