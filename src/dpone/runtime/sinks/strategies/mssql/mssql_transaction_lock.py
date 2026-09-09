"""Canonical SQL Server target/operation transaction-lock policy."""

from __future__ import annotations

from typing import Any

from dpone.backfill.shadow_append_authority import require_shadow_append_authority
from dpone.runtime.support.mssql_scalar import single_int


def transaction_lock_timeout_ms(load_config: Any) -> int:
    options = getattr(load_config, "options", {}) or {}
    raw = options.get("mssql_transaction_lock_timeout_ms", 300_000)
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError("mssql_transaction.lock_timeout_invalid") from exc
    if not 1 <= value <= 3_600_000:
        raise ValueError("mssql_transaction.lock_timeout_invalid")
    return value


def target_lock_resource(target_identity: bytes) -> str:
    """Bind target-wide serialization to the immutable physical identity."""

    if type(target_identity) is not bytes or len(target_identity) != 32:
        raise ValueError("mssql_transaction.target_lock_identity_invalid")
    return "dpone:target:" + target_identity.hex()


def operation_lock_resource(operation_key: bytes) -> str:
    """Bind receipt-level serialization to one immutable operation identity."""

    if type(operation_key) is not bytes or len(operation_key) != 32:
        raise ValueError("mssql_transaction.operation_lock_identity_invalid")
    return "dpone:operation:" + operation_key.hex()


def acquire_target_lock(
    connector: Any,
    resource: str,
    *,
    database: str,
    timeout_ms: int,
) -> None:
    """Acquire the database-scoped target fence for the current transaction."""

    if not isinstance(resource, str) or not resource.startswith("dpone:target:"):
        raise ValueError("mssql_transaction.target_lock_resource_invalid")
    procedure = f"{connector.quote_identifier(database)}.sys.sp_getapplock"
    rows = connector.get_records(
        f"DECLARE @result int; EXEC @result = {procedure} @Resource = ?, "
        "@LockMode = 'Exclusive', @LockOwner = 'Transaction', @LockTimeout = ?; "
        "SELECT @result AS lock_result;",
        (resource, timeout_ms),
        as_dict=True,
    )
    result = single_int(
        rows,
        "lock_result",
        error_code="mssql_transaction.target_lock_result_invalid",
    )
    if result < 0:
        raise RuntimeError("mssql_transaction.target_lock_unavailable")


def acquire_operation_lock(
    connector: Any,
    *,
    database: str,
    operation_key: bytes,
    timeout_ms: int,
) -> None:
    procedure = f"{connector.quote_identifier(database)}.sys.sp_getapplock"
    resource = operation_lock_resource(operation_key)
    rows = connector.get_records(
        f"DECLARE @result int; EXEC @result = {procedure} @Resource = ?, "
        "@LockMode = 'Exclusive', @LockOwner = 'Transaction', @LockTimeout = ?; "
        "SELECT @result AS lock_result;",
        (resource, timeout_ms),
        as_dict=True,
    )
    result = single_int(
        rows,
        "lock_result",
        error_code="mssql_transaction.operation_lock_result_invalid",
    )
    if result < 0:
        raise RuntimeError("mssql_transaction.operation_lock_unavailable")


def acquire_target_then_operation_locks(
    connector: Any,
    *,
    database: str,
    target_identity: bytes,
    operation_key: bytes,
    timeout_ms: int,
    target_lock_acquirer: Any = acquire_target_lock,
    operation_lock_acquirer: Any = acquire_operation_lock,
) -> None:
    """Acquire the one allowed generic-finalizer lock order.

    The target fence serializes target mutation while independent workers keep
    extracting and materializing staging concurrently.  The narrower operation
    fence remains second so replay/receipt ownership cannot invert this order.
    """

    target_lock_acquirer(
        connector,
        target_lock_resource(target_identity),
        database=database,
        timeout_ms=timeout_ms,
    )
    operation_lock_acquirer(
        connector,
        database=database,
        operation_key=operation_key,
        timeout_ms=timeout_ms,
    )


def acquire_mutation_locks(
    connector: Any,
    load_config: Any,
    operation: Any,
    *,
    target_lock_acquirer: Any = acquire_target_lock,
    operation_lock_acquirer: Any = acquire_operation_lock,
) -> None:
    """Apply the canonical lock policy for ordinary and shadow mutations."""

    request = operation.attempt.request
    timeout_ms = transaction_lock_timeout_ms(load_config)
    if require_shadow_append_authority(load_config) is None:
        acquire_target_then_operation_locks(
            connector,
            database=request.target_database,
            target_identity=operation.attempt.target_identity,
            operation_key=operation.operation_key,
            timeout_ms=timeout_ms,
            target_lock_acquirer=target_lock_acquirer,
            operation_lock_acquirer=operation_lock_acquirer,
        )
        return
    operation_lock_acquirer(
        connector,
        database=request.target_database,
        operation_key=operation.operation_key,
        timeout_ms=timeout_ms,
    )


__all__ = [
    "acquire_mutation_locks",
    "acquire_operation_lock",
    "acquire_target_lock",
    "acquire_target_then_operation_locks",
    "operation_lock_resource",
    "target_lock_resource",
    "transaction_lock_timeout_ms",
]
