"""Evidence validation and projection for MSSQL transaction finalization."""

from __future__ import annotations

import hashlib
from dataclasses import replace
from datetime import UTC, datetime  # type: ignore[attr-defined]
from typing import Any

from dpone.backfill.shadow_append_authority import require_shadow_append_authority
from dpone.contracts.mssql_transaction_governance import (
    MssqlGenericCommitReceipt,
    MssqlPayloadCommitEvidence,
    MssqlReceiptMetrics,
    MssqlSourceLifecycleEvidence,
)
from dpone.runtime.consumed_payload_evidence import ConsumedPayloadEvidence
from dpone.runtime.extraction_lifecycle import ExtractionLifecycleReceipt
from dpone.runtime.sinks.load_result import LoadResult
from dpone.runtime.sinks.mssql_target_mutation_plan import MssqlTargetMutationPlan
from dpone.runtime.sinks.strategies.mssql.mssql_transaction_lock import (
    operation_lock_resource,
    target_lock_resource,
)
from dpone.runtime.support.mssql_object_name import MSSQLObjectName


def receipt_metrics(result: LoadResult, *, staging_rows: int) -> MssqlReceiptMetrics:
    """Project strategy metrics into durable receipt metrics."""

    return MssqlReceiptMetrics(
        inserted_rows=result.inserted_rows,
        updated_rows=result.updated_rows,
        total_rows=result.total_rows,
        staging_rows=staging_rows,
        replaced_rows=result.replaced_rows,
        soft_deleted_rows=result.soft_deleted_rows,
        reactivated_rows=result.reactivated_rows,
        unchanged_rows=result.unchanged_rows,
        hard_deleted_rows=result.hard_deleted_rows,
        active_rows=result.active_rows,
    )


def require_payload_evidence(staging: Any, *, staging_rows: int) -> MssqlPayloadCommitEvidence:
    """Validate native staging consumption before any target transaction."""

    evidence = getattr(staging, "consumed_payload_evidence", None)
    if not isinstance(evidence, ConsumedPayloadEvidence):
        raise RuntimeError("mssql_transaction.consumed_payload_evidence_required")
    complete = evidence.require_complete(native=True)
    native_contract = complete.native_contract_sha256
    native_rows = complete.actual_native_rows
    if native_contract is None or native_rows is None:  # protected by require_complete; narrows the type
        raise RuntimeError("mssql_transaction.native_contract_evidence_required")
    if staging_rows != complete.actual_raw_rows or getattr(staging, "row_count", None) != native_rows:
        raise RuntimeError("mssql_transaction.staging_payload_evidence_mismatch")
    return MssqlPayloadCommitEvidence(
        manifest_sha256=bytes.fromhex(complete.manifest_sha256),
        declared_rows=complete.declared_rows,
        actual_raw_rows=complete.actual_raw_rows,
        actual_native_rows=native_rows,
        native_contract_sha256=bytes.fromhex(native_contract),
    )


def receipt_matches(
    receipt: MssqlGenericCommitReceipt,
    *,
    payload_evidence: MssqlPayloadCommitEvidence,
    source_lifecycle: MssqlSourceLifecycleEvidence,
    mutation_plan: MssqlTargetMutationPlan,
) -> bool:
    """Match a durable receipt to the exact input and mutation contract."""

    return (
        receipt.payload_evidence == payload_evidence
        and receipt.source_lifecycle == source_lifecycle
        and receipt.mutation_plan_sha256 == mutation_plan.digest
        and receipt.target_before_sha256 == mutation_plan.expected_before_sha256
        and receipt.target_after_sha256 == mutation_plan.expected_after_sha256
    )


def require_source_lifecycle(receipt: ExtractionLifecycleReceipt | None) -> MssqlSourceLifecycleEvidence:
    """Require completed extraction and hash its optional source token."""

    if receipt is None or receipt.extraction_completed_at is None:
        raise RuntimeError("mssql_transaction.completed_source_lifecycle_required")
    token = receipt.source_token
    return MssqlSourceLifecycleEvidence(
        extraction_started_at_utc=receipt.extraction_started_at,
        extraction_completed_at_utc=receipt.extraction_completed_at,
        clock_authority=receipt.clock_authority,
        snapshot_acquired_at_utc=receipt.snapshot_acquired_at,
        snapshot_authority=receipt.snapshot_authority,
        source_token_sha256=(hashlib.sha256(token.encode("utf-8")).digest() if token is not None else None),
    )


def capture_loaded_at_utc(connector: Any) -> datetime:
    """Capture the transaction-local SQL Server UTC timestamp."""

    rows = connector.get_records("SELECT SYSUTCDATETIME() AS loaded_at_utc", as_dict=True)
    if len(rows) != 1 or not isinstance(rows[0].get("loaded_at_utc"), datetime):
        raise RuntimeError("mssql_transaction.target_loaded_at_unavailable")
    value = rows[0]["loaded_at_utc"]
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def mutation_fence_evidence(connector: Any, load_config: Any, operation: Any) -> dict[str, Any]:
    """Read back the session and exact lock scope actually acquired."""

    rows = connector.get_records("SELECT @@SPID AS session_id", as_dict=True)
    session_id = rows[0].get("session_id") if len(rows) == 1 else None
    if isinstance(session_id, bool) or not isinstance(session_id, int) or session_id < 1:
        raise RuntimeError("mssql_transaction.mutation_fence_session_unavailable")
    shadow_authority = require_shadow_append_authority(load_config)
    if shadow_authority is None:
        scope = "target"
        resource = target_lock_resource(operation.attempt.target_identity)
    else:
        scope = "operation"
        resource = operation_lock_resource(operation.operation_key)
    return {
        "schema": "dpone.mssql.mutation-fence-evidence.v1",
        "mode": "transaction_exclusive_applock",
        "scope": scope,
        "lock_owner": "transaction",
        "lock_mode": "exclusive",
        "resource_sha256": hashlib.sha256(resource.encode("utf-8")).hexdigest(),
        "session_id": session_id,
    }


def with_target_fence_evidence(result: LoadResult, evidence: dict[str, Any]) -> LoadResult:
    """Attach mutation-fence evidence and its target-scope compatibility view."""

    metrics = dict(result.reconciliation_metrics or {})
    metrics["mssql_mutation_fence_evidence"] = dict(evidence)
    if evidence.get("scope") == "target":
        target_evidence = dict(evidence)
        target_evidence["schema"] = "dpone.mssql.target-fence-evidence.v1"
        metrics["mssql_target_fence_evidence"] = target_evidence
    return replace(result, reconciliation_metrics=metrics)


def project_loaded_at(connector: Any, staging: Any, loaded_at_utc: datetime) -> None:
    """Project the authoritative target timestamp into lineage-enabled staging."""

    columns = {str(value).casefold() for value in getattr(staging, "columns", ())}
    if "__dpone__loaded_at" not in columns:
        return
    target = MSSQLObjectName.from_parts(
        database=getattr(staging, "database", None),
        schema=str(staging.schema),
        table=str(staging.table),
        strict=True,
    ).quoted()
    connector.execute_query(
        f"UPDATE {target} SET [__dpone__loaded_at] = ?",
        (loaded_at_utc.astimezone(UTC).replace(tzinfo=None),),
    )


__all__ = [
    "capture_loaded_at_utc",
    "mutation_fence_evidence",
    "project_loaded_at",
    "receipt_matches",
    "receipt_metrics",
    "require_payload_evidence",
    "require_source_lifecycle",
    "with_target_fence_evidence",
]
