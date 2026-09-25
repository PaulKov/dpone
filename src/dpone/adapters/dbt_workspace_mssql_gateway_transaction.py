"""Fixed SQL transaction envelopes for protected workspace gateway procedures."""

from __future__ import annotations

from dpone.adapters.dbt_workspace_mssql_gateway_security import (
    WORKSPACE_GATEWAY_SESSION_OPTIONS,
    workspace_gateway_identifier,
)


def workspace_gateway_transaction(
    control_schema: str, body: str, *, channel: bool, read_only: bool = False, registration: bool = False
) -> str:
    """Wrap installer-owned SQL with the inventory-before-channel lock order.

    The calling fixed procedure validates its inputs before this envelope. Channel
    procedures declare ``@channel_sha256``. ``body`` must only be a renderer-owned SQL
    fragment, never a SQL runtime parameter. Results must be selected after this
    envelope returns so an uncommitted mutation cannot be reported as successful.
    Ordinary gateway entry points reject ambient transactions; semantic guard helper
    procedures use their separately attested existing caller transaction instead.
    """
    schema = workspace_gateway_identifier(control_schema)
    inventory_mode = "Exclusive" if registration else "Shared"
    channel_mode = "Shared" if read_only else "Exclusive"
    channel_lock = (
        f"""
    DECLARE @channel_lock nvarchar(255) = N'dpone:workspace-channel:{schema}:' + @channel_sha256;
    EXEC @lock_result = sys.sp_getapplock @Resource = @channel_lock,
        @LockMode = N'{channel_mode}', @LockOwner = N'Transaction', @LockTimeout = 0;
    IF @lock_result < 0 THROW 51004, 'workspace channel contention', 1;
""".strip()
        if channel
        else ""
    )
    return f"""DECLARE @entry_trancount int = @@TRANCOUNT;
IF @entry_trancount <> 0 THROW 51000, 'workspace gateway ambient transaction is unsupported', 1;
SET XACT_ABORT ON;
{WORKSPACE_GATEWAY_SESSION_OPTIONS}
SET LOCK_TIMEOUT 0;
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
BEGIN TRY
    BEGIN TRANSACTION;
    DECLARE @lock_result int;
    EXEC @lock_result = sys.sp_getapplock @Resource = N'dpone:workspace-inventory:{schema}',
        @LockMode = N'{inventory_mode}', @LockOwner = N'Transaction', @LockTimeout = 0;
    IF @lock_result < 0 THROW 51004, 'workspace inventory contention', 1;
    {channel_lock}
    {body}
    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
    THROW;
END CATCH;"""
