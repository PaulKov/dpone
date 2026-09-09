"""Backfill ledger initialization and public result projection."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from dpone.backfill.execution_policy import row_count_verification
from dpone.backfill.mapping import BackfillExecutionSelection
from dpone.backfill.progress import backfill_progress
from dpone.backfill.state import (
    CHUNK_STATUS_FAILED,
    CHUNK_STATUS_SUCCESS,
    BackfillChunkRecord,
    BackfillLedger,
    FileBackfillStateStore,
)

if TYPE_CHECKING:
    from dpone.backfill.models import BackfillChunk

_EVIDENCE_KEYS = (
    "mssql_staging_evidence",
    "mssql_mutation_fence_evidence",
    "mssql_target_fence_evidence",
    "mssql_receipt_recovery",
)


def project_chunk_execution_evidence(result: Mapping[str, Any]) -> dict[str, Any]:
    """Select versioned, credential-free producer evidence for the ledger."""

    metrics = result.get("reconciliation_metrics")
    if not isinstance(metrics, Mapping):
        return {}
    projected: dict[str, Any] = {}
    for key in _EVIDENCE_KEYS:
        value = metrics.get(key)
        if isinstance(value, Mapping) and _is_public_json_mapping(value):
            projected[key] = dict(value)
    if metrics.get("mssql_transaction_replay_suppressed") is True:
        projected["mssql_transaction_replay_suppressed"] = True
    return projected


def _is_public_json_mapping(value: Mapping[Any, Any]) -> bool:
    return all(
        isinstance(key, str) and isinstance(item, str | int | bool | type(None)) and not isinstance(item, float)
        for key, item in value.items()
    )


def load_or_create_backfill_ledger(
    *,
    store: FileBackfillStateStore,
    run_key: str,
    dataset: str,
    inner_mode: str,
    chunks: tuple[BackfillChunk, ...],
    spec_config: Any,
    plan_hash: str,
    config_hash: str,
    portable_scope_column_contract: Mapping[str, Any] | None = None,
) -> BackfillLedger:
    """Load a compatible ledger or persist the immutable campaign shape."""

    existing = store.load_verified(run_key, plan_hash=plan_hash, config_hash=config_hash)
    if existing is not None and len(existing.chunks) == len(chunks):
        return existing
    ledger = BackfillLedger(
        run_key=run_key,
        dataset=dataset,
        inner_mode=inner_mode,
        plan_hash=plan_hash,
        config_hash=config_hash,
        chunk_config={
            "column": spec_config.column,
            "from": spec_config.start,
            "to": spec_config.end,
            "step": spec_config.step,
            "kind": spec_config.kind,
        },
        portable_scope_column_contract=(
            dict(portable_scope_column_contract) if portable_scope_column_contract is not None else None
        ),
        chunks=[
            BackfillChunkRecord(
                index=chunk.index,
                start=chunk.start,
                end=chunk.end,
                idempotency_key=chunk.idempotency_key,
            )
            for chunk in chunks
        ],
    )
    store.save(ledger)
    return ledger


def build_backfill_result(
    *,
    ledger: BackfillLedger,
    store: FileBackfillStateStore,
    chunks: tuple[BackfillChunk, ...],
    retry_policy: str,
    selected: int,
    skipped: int,
    errors: list[str],
    duration_seconds: float,
    selection: BackfillExecutionSelection | None = None,
) -> dict[str, Any]:
    """Build a public result without forcing an O(n) local-cache serialization."""

    counts = ledger.counts()
    state_path, cache_status = _result_state_cache(store, ledger.run_key)
    scope_indexes = set(selection.chunk_indexes) if selection is not None else {chunk.index for chunk in chunks}
    succeeded = [
        record for record in ledger.chunks if record.index in scope_indexes and record.status == CHUNK_STATUS_SUCCESS
    ]
    extracted = sum(record.rows_extracted for record in succeeded)
    loaded = sum(record.rows_loaded for record in succeeded)
    if ledger.status == "cancel_requested":
        status = operation_status = "cancelled"
    else:
        complete = (
            len(succeeded) == len(scope_indexes)
            if selection is not None
            else counts[CHUNK_STATUS_SUCCESS] == len(chunks)
        )
        status = "success" if complete and not errors else "error"
        operation_status = "error" if errors else "success"
    result: dict[str, Any] = {
        "status": status,
        "extracted_rows": extracted,
        "loaded_rows": loaded,
        "inserted_rows": loaded,
        "updated_rows": 0,
        "final_rows": loaded,
        "duration_seconds": duration_seconds,
        "errors": errors,
        "backfill": {
            "run_key": ledger.run_key,
            "dataset": ledger.dataset,
            "inner_mode": ledger.inner_mode,
            "retry_policy": retry_policy,
            "operation_status": operation_status,
            "state_backend": "audit_schema" if getattr(store, "dialect", None) else "local_file",
            "state_capabilities": store.state_capabilities(),
            "state_authority": _result_state_authority(store, ledger.run_key),
            "state_path": state_path,
            "state_cache_status": cache_status,
            "chunks_total": len(chunks),
            "chunks_selected": selected,
            "chunks_committed": counts[CHUNK_STATUS_SUCCESS],
            "chunks_failed": counts[CHUNK_STATUS_FAILED],
            "chunks_skipped_resume": skipped,
            "campaign_status": ledger.status,
            "cancel_reason": ledger.cancel_reason,
            "verification": row_count_verification(ledger),
            "progress": backfill_progress(ledger),
            "publication": ledger.publication.to_jsonable() if ledger.publication is not None else None,
            "chunks": [record.to_jsonable() for record in ledger.chunks],
        },
    }
    if selection is not None:
        result["backfill"]["mapping"] = {
            "mode": selection.mode,
            "plan_fingerprint": selection.mapping_plan_fingerprint,
            "backfill_plan_hash": selection.backfill_plan_hash,
            "item_index": selection.item_index,
            "first_chunk_index": selection.first_chunk_index,
            "last_chunk_index": selection.last_chunk_index,
            "chunks_count": selection.chunks_count,
            "item_status": status,
        }
    return result


def _result_state_cache(store: FileBackfillStateStore, run_key: str) -> tuple[str | None, str]:
    path = store.path_for(run_key)
    if not path.is_file():
        return None, "unavailable"
    status = "authoritative" if getattr(store, "dialect", None) is None else "present_non_authoritative"
    return str(path), status


def _result_state_authority(store: FileBackfillStateStore, run_key: str) -> dict[str, Any]:
    dialect = getattr(store, "dialect", None)
    if dialect is None:
        return {"backend": "local_file", "run_key": run_key}
    return {
        "backend": "audit_schema",
        "dialect": dialect,
        "schema": getattr(store, "schema", None),
        "campaigns_table": getattr(store, "campaigns_table", None),
        "chunks_table": getattr(store, "chunks_table", None),
        "run_key": run_key,
    }
