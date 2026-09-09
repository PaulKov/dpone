"""Strict row decoding for generic SQL Server transaction evidence."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from dpone.contracts.mssql_transaction_governance import (
    MssqlAttemptRequest,
    MssqlGenericCommitReceipt,
    MssqlOperationRequest,
    MssqlPayloadCommitEvidence,
    MssqlReceiptMetrics,
    MssqlSourceLifecycleEvidence,
    MssqlTransactionAttempt,
    MssqlTransactionOperation,
)


def attempt_from_rows(request: MssqlAttemptRequest, rows: Any) -> MssqlTransactionAttempt:
    """Decode exactly one attempt and reject any digest/coordinate mismatch."""

    values = list(rows or ())
    if len(values) != 1:
        raise RuntimeError("mssql_transaction.attempt_missing_or_ambiguous")
    row = values[0]
    exact = (
        binary(row.get("attempt_key", request.attempt_key)) == request.attempt_key
        and binary(row.get("target_identity")) == request.target_identity
        and binary(row.get("invocation_digest")) == request.invocation.invocation_digest
        and binary(row.get("route_fingerprint")) == request.route_fingerprint
        and str(row.get("target_database") or "") == request.target_database
        and str(row.get("target_schema") or "") == request.target_schema
        and str(row.get("target_table") or "") == request.target_table
        and str(row.get("strategy") or "") == request.strategy
    )
    if not exact:
        raise RuntimeError("mssql_transaction.attempt_identity_collision")
    return MssqlTransactionAttempt(
        request=request,
        generation=int(row.get("generation") or 0),
        is_current_generation=bool(row.get("is_current_generation", True)),
    )


def receipt_from_row(row: Any) -> MssqlGenericCommitReceipt:
    """Decode one immutable receipt row into the public contract."""

    return MssqlGenericCommitReceipt(
        receipt_id=str(row.get("receipt_id") or ""),
        operation_key=binary(row.get("operation_key")),
        attempt_key=binary(row.get("attempt_key")),
        target_identity=binary(row.get("target_identity")),
        generation=int(row.get("generation") or 0),
        scope_hash=binary(row.get("scope_hash")),
        operation_epoch=int(row.get("operation_epoch") or 0),
        owner_digest=binary(row.get("owner_digest")),
        route_fingerprint=binary(row.get("route_fingerprint")),
        load_id=str(row.get("load_id") or ""),
        strategy=str(row.get("strategy") or ""),
        mutation_plan_sha256=binary(row.get("mutation_plan_sha256")),
        target_before_sha256=binary(row.get("target_before_sha256")),
        target_after_sha256=binary(row.get("target_after_sha256")),
        loaded_at_utc=utc_datetime(row.get("loaded_at_utc"), field="receipt_loaded_at_utc"),
        committed_at_utc=utc_datetime(row.get("committed_at_utc"), field="receipt_committed_at_utc"),
        payload_evidence=MssqlPayloadCommitEvidence(
            manifest_sha256=binary(row.get("payload_manifest_sha256")),
            declared_rows=int(row.get("declared_rows") or 0),
            actual_raw_rows=int(row.get("actual_raw_rows") or 0),
            actual_native_rows=int(row.get("actual_native_rows") or 0),
            native_contract_sha256=binary(row.get("native_contract_sha256")),
        ),
        source_lifecycle=MssqlSourceLifecycleEvidence(
            extraction_started_at_utc=utc_datetime(
                row.get("extraction_started_at_utc"), field="extraction_started_at_utc"
            ),
            extraction_completed_at_utc=utc_datetime(
                row.get("extraction_completed_at_utc"), field="extraction_completed_at_utc"
            ),
            clock_authority=str(row.get("extraction_clock_authority") or ""),
            snapshot_acquired_at_utc=(
                utc_datetime(row.get("snapshot_acquired_at_utc"), field="snapshot_acquired_at_utc")
                if row.get("snapshot_acquired_at_utc") is not None
                else None
            ),
            snapshot_authority=(
                str(row.get("snapshot_authority")) if row.get("snapshot_authority") is not None else None
            ),
            source_token_sha256=(
                binary(row.get("source_token_sha256")) if row.get("source_token_sha256") is not None else None
            ),
        ),
        metrics=MssqlReceiptMetrics(
            inserted_rows=int(row.get("inserted_rows") or 0),
            updated_rows=int(row.get("updated_rows") or 0),
            total_rows=int(row.get("total_rows") or 0),
            staging_rows=optional_int(row.get("staging_rows")),
            replaced_rows=optional_int(row.get("replaced_rows")),
            soft_deleted_rows=optional_int(row.get("soft_deleted_rows")),
            reactivated_rows=optional_int(row.get("reactivated_rows")),
            unchanged_rows=optional_int(row.get("unchanged_rows")),
            hard_deleted_rows=optional_int(row.get("hard_deleted_rows")),
            active_rows=optional_int(row.get("active_rows")),
        ),
    )


def operation_from_rows(
    attempt: MssqlTransactionAttempt,
    request: MssqlOperationRequest,
    rows: Any,
) -> MssqlTransactionOperation:
    """Decode the exact current operation-owner epoch."""

    values = list(rows or ())
    if len(values) != 1:
        raise RuntimeError("mssql_transaction.operation_missing_or_ambiguous")
    row = values[0]
    operation_key = request.operation_key(attempt)
    exact = (
        binary(row.get("operation_key", operation_key)) == operation_key
        and binary(row.get("attempt_key")) == attempt.attempt_key
        and binary(row.get("scope_hash")) == request.scope_hash
        and binary(row.get("current_owner_digest")) == request.owner_digest
    )
    if not exact:
        raise RuntimeError("mssql_transaction.operation_identity_collision")
    return MssqlTransactionOperation(
        attempt=attempt,
        operation_key=operation_key,
        scope_hash=request.scope_hash,
        owner_digest=request.owner_digest,
        epoch=int(row.get("current_epoch") or 0),
        lease_expires_at_utc=(
            utc_datetime(row.get("lease_expires_at_utc"), field="operation_lease_expires_at_utc")
            if row.get("lease_expires_at_utc") is not None
            else None
        ),
    )


def binary(value: Any) -> bytes:
    """Normalize pyodbc binary values without accepting textual digests."""

    return bytes(value) if isinstance(value, (bytes, bytearray, memoryview)) else b""


def optional_int(value: Any) -> int | None:
    return None if value is None else int(value)


def utc_datetime(value: Any, *, field: str) -> datetime:
    """Decode a SQL Server ``datetime2`` value under its explicit UTC authority."""

    if not isinstance(value, datetime):
        raise RuntimeError(f"mssql_transaction.{field}_invalid")
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


__all__ = ["attempt_from_rows", "binary", "operation_from_rows", "receipt_from_row", "utc_datetime"]
