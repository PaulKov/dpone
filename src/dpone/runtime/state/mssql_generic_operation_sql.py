"""Parameterized SQL for generic MSSQL operation-owner epochs."""

from __future__ import annotations

from typing import Any


def claim_operation_sql(
    operation_table: str,
    receipt_table: str,
    fence_table: str,
    attempt_table: str,
) -> str:
    """Return an atomic receipt-observation and claim batch under SERIALIZABLE."""

    return f"""
        SET NOCOUNT ON;
        SET XACT_ABORT ON;
        SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
        DECLARE @operation_key binary(32) = ?;
        DECLARE @attempt_key binary(32) = ?;
        DECLARE @scope_hash binary(32) = ?;
        DECLARE @owner_digest binary(32) = ?;
        DECLARE @lease_expires_at_utc datetime2(7) = ?;
        DECLARE @target_identity binary(32) = ?;
        DECLARE @generation bigint = ?;
        DECLARE @route_fingerprint binary(32) = ?;
        DECLARE @committed_receipt_id nvarchar(128);

        IF @lease_expires_at_utc IS NOT NULL AND @lease_expires_at_utc <= SYSUTCDATETIME()
            THROW 51000, 'DPONE_LOAD_OPERATION_LEASE_EXPIRED', 1;

        IF NOT EXISTS (
            SELECT 1
            FROM {fence_table} AS f WITH (UPDLOCK, HOLDLOCK)
            INNER JOIN {attempt_table} AS a WITH (HOLDLOCK)
                ON a.target_identity = f.target_identity
               AND a.attempt_key = f.current_attempt_key
            WHERE f.target_identity = @target_identity
              AND f.current_attempt_key = @attempt_key
              AND f.current_generation = @generation
              AND f.current_route_fingerprint = @route_fingerprint
              AND a.attempt_key = @attempt_key
              AND a.generation = @generation
              AND a.route_fingerprint = @route_fingerprint
        )
            THROW 51000, 'DPONE_LOAD_OPERATION_STALE_ATTEMPT_GENERATION', 1;

        IF NOT EXISTS (
            SELECT 1 FROM {operation_table} WITH (UPDLOCK, HOLDLOCK)
            WHERE operation_key = @operation_key
        )
        BEGIN
            INSERT INTO {operation_table} (
                operation_key, attempt_key, scope_hash, current_epoch,
                current_owner_digest, lease_expires_at_utc, updated_at_utc
            ) VALUES (
                @operation_key, @attempt_key, @scope_hash, 1,
                @owner_digest, @lease_expires_at_utc, SYSUTCDATETIME()
            );
        END
        ELSE
        BEGIN
            IF EXISTS (
                SELECT 1 FROM {operation_table} WITH (UPDLOCK, HOLDLOCK)
                WHERE operation_key = @operation_key
                  AND (attempt_key <> @attempt_key OR scope_hash <> @scope_hash)
            )
                THROW 51000, 'DPONE_LOAD_OPERATION_IDENTITY_COLLISION', 1;

            SELECT @committed_receipt_id = receipt_id
            FROM {receipt_table} WITH (HOLDLOCK)
            WHERE operation_key = @operation_key;

            IF @committed_receipt_id IS NULL
            BEGIN
                DECLARE @persisted_owner binary(32);
                DECLARE @persisted_expiry datetime2(7);
                SELECT @persisted_owner = current_owner_digest,
                       @persisted_expiry = lease_expires_at_utc
                FROM {operation_table} WITH (UPDLOCK, HOLDLOCK)
                WHERE operation_key = @operation_key;

                IF @persisted_owner = @owner_digest
                BEGIN
                    IF (@persisted_expiry IS NULL AND @lease_expires_at_utc IS NOT NULL)
                       OR (@persisted_expiry IS NOT NULL AND @lease_expires_at_utc IS NULL)
                        THROW 51000, 'DPONE_LOAD_OPERATION_LEASE_MODE_MISMATCH', 1;
                    IF @persisted_expiry IS NOT NULL
                       AND @persisted_expiry <= SYSUTCDATETIME()
                        THROW 51000, 'DPONE_LOAD_OPERATION_LEASE_EXPIRED_OWNER_REACQUIRE_REQUIRED', 1;

                    UPDATE {operation_table} WITH (UPDLOCK, HOLDLOCK)
                    SET lease_expires_at_utc = CASE
                            WHEN @persisted_expiry IS NULL THEN NULL
                            WHEN @lease_expires_at_utc > @persisted_expiry
                                THEN @lease_expires_at_utc
                            ELSE @persisted_expiry
                        END,
                        updated_at_utc = SYSUTCDATETIME()
                    WHERE operation_key = @operation_key;
                END
                ELSE
                BEGIN
                    IF @persisted_expiry IS NULL OR @persisted_expiry > SYSUTCDATETIME()
                        THROW 51000, 'DPONE_LOAD_OPERATION_ALREADY_OWNED', 1;
                    IF @lease_expires_at_utc IS NULL
                        THROW 51000, 'DPONE_LOAD_OPERATION_LEASE_MODE_MISMATCH', 1;

                    UPDATE {operation_table} WITH (UPDLOCK, HOLDLOCK)
                    SET current_epoch = current_epoch + 1,
                        current_owner_digest = @owner_digest,
                        lease_expires_at_utc = @lease_expires_at_utc,
                        updated_at_utc = SYSUTCDATETIME()
                    WHERE operation_key = @operation_key;
                END;
            END;
        END;

        SELECT operation_key, attempt_key, scope_hash, current_epoch,
               current_owner_digest, lease_expires_at_utc,
               @committed_receipt_id AS committed_receipt_id
        FROM {operation_table} WITH (HOLDLOCK)
        WHERE operation_key = @operation_key;
    """


def claim_operation_params(
    attempt: Any,
    request: Any,
) -> tuple[Any, ...]:
    return (
        request.operation_key(attempt),
        attempt.attempt_key,
        request.scope_hash,
        request.owner_digest,
        request.lease_expires_at_utc,
        attempt.target_identity,
        attempt.generation,
        attempt.route_fingerprint,
    )


__all__ = ["claim_operation_params", "claim_operation_sql"]
