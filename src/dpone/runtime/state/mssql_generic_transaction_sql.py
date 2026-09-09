"""SQL batches for pre-source SQL Server generation allocation."""

from __future__ import annotations

from typing import Any


def allocation_sql(*, fence: str, attempt: str) -> str:
    """Return the one SERIALIZABLE, idempotent generation-allocation batch."""

    return f"""
        SET NOCOUNT ON;
        SET XACT_ABORT ON;
        SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
        DECLARE @generation bigint;
        DECLARE @fence_exists bit = 0;
        DECLARE @persisted_attempt_key binary(32);

        SELECT @persisted_attempt_key = attempt_key,
               @generation = generation
        FROM {attempt} WITH (UPDLOCK, HOLDLOCK)
        WHERE invocation_digest = ?;

        IF @persisted_attempt_key IS NULL
        BEGIN
            SELECT @persisted_attempt_key = attempt_key,
                   @generation = generation
            FROM {attempt} WITH (UPDLOCK, HOLDLOCK)
            WHERE attempt_key = ?;
        END;

        IF @generation IS NULL
        BEGIN
            SELECT @generation = current_generation + 1, @fence_exists = 1
            FROM {fence} WITH (UPDLOCK, HOLDLOCK)
            WHERE target_identity = ?;
            IF @fence_exists = 0
                SET @generation = 1;

            INSERT INTO {attempt} (
                attempt_key, target_identity, generation, invocation_digest,
                route_fingerprint, first_load_id, target_database,
                target_schema, target_table, strategy, created_at_utc
            ) VALUES (?, ?, @generation, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME());
            SET @persisted_attempt_key = ?;

            IF @fence_exists = 1
                UPDATE {fence} WITH (UPDLOCK, HOLDLOCK)
                SET current_generation = @generation,
                    current_attempt_key = ?,
                    current_route_fingerprint = ?,
                    updated_at_utc = SYSUTCDATETIME()
                WHERE target_identity = ?;
            ELSE
                INSERT INTO {fence} (
                    target_identity, current_generation, current_attempt_key,
                    current_route_fingerprint, updated_at_utc
                ) VALUES (?, @generation, ?, ?, SYSUTCDATETIME());
        END;

        SELECT a.attempt_key, a.target_identity, a.generation, a.invocation_digest,
               a.route_fingerprint, a.target_database, a.target_schema, a.target_table, a.strategy,
               CAST(CASE WHEN f.current_attempt_key = a.attempt_key
                              AND f.current_generation = a.generation
                              AND f.current_route_fingerprint = a.route_fingerprint
                         THEN 1 ELSE 0 END AS bit) AS is_current_generation
        FROM {attempt} AS a WITH (HOLDLOCK)
        LEFT JOIN {fence} AS f WITH (HOLDLOCK)
          ON f.target_identity = a.target_identity
        WHERE a.attempt_key = @persisted_attempt_key;
    """


def allocation_params(request: Any) -> tuple[Any, ...]:
    """Bind the allocation batch without interpolating invocation values."""

    return (
        request.invocation.invocation_digest,
        request.attempt_key,
        request.target_identity,
        request.attempt_key,
        request.target_identity,
        request.invocation.invocation_digest,
        request.route_fingerprint,
        request.load_id,
        request.target_database,
        request.target_schema,
        request.target_table,
        request.strategy,
        request.attempt_key,
        request.attempt_key,
        request.route_fingerprint,
        request.target_identity,
        request.target_identity,
        request.attempt_key,
        request.route_fingerprint,
    )


__all__ = ["allocation_params", "allocation_sql"]
