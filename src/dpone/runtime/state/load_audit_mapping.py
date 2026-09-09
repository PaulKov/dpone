"""Shared mapping helpers for load audit storage backends."""

from __future__ import annotations

from typing import Any

from dpone.runtime.lineage.audit import LoadAuditRecord


def load_audit_insert_params(record: LoadAuditRecord) -> tuple[Any, ...]:
    """Return canonical insert-column values for a load audit record."""

    return (
        record.run_id,
        record.load_id,
        record.status,
        record.process_name,
        record.source_schema,
        record.source_table,
        record.target_schema,
        record.target_table,
        record.strategy,
        record.started_at,
        record.staged_at,
        record.committed_at,
        record.failed_at,
        record.extracted_rows,
        record.staged_rows,
        record.inserted_rows,
        record.updated_rows,
        record.loaded_rows,
        record.error_message,
        record.artifact_uri,
    )


def load_audit_exact_metric_params(record: LoadAuditRecord) -> tuple[Any, ...]:
    """Return additive exact metrics used by backends that expose the v2 audit shape."""

    return (
        record.deleted_rows,
        record.reactivated_rows,
        record.unchanged_rows,
        record.soft_deleted_rows,
        record.hard_deleted_rows,
        record.active_rows,
        record.total_rows,
        record.commit_receipt_id,
        record.commit_outcome,
    )


__all__ = ["load_audit_exact_metric_params", "load_audit_insert_params"]
