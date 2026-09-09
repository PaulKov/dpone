"""Locked external migration DDL for generic MSSQL governance catalogs."""

from __future__ import annotations

from dpone.contracts.mssql_object_name import MSSQLObjectName
from dpone.runtime.state.mssql_generic_transaction_migration_proof_ddl import (
    render_invocation_index_proof,
    render_migration_common_proof,
)
from dpone.runtime.state.mssql_generic_transaction_names import ATTEMPT_TABLE


def render_generic_transaction_catalog_v1_to_v2_ddl(*, database: str, schema: str) -> str:
    """Render the locked, evidence-preserving v1-to-v2 catalog migration.

    The migration accepts an exact v1 catalog or an already exact v2 catalog.
    It never rewrites attempt keys or rows. Historical duplicate scheduler
    invocations are an explicit operator-repair boundary, not data to guess at.
    """

    attempt = _name(database, schema, ATTEMPT_TABLE)
    use_database = _quote(database)
    lock_resource = _sql_text(f"dpone:generic-transaction-catalog:{database}:{schema}:v2")
    common_proof = render_migration_common_proof(schema=schema)
    invocation_index_proof = render_invocation_index_proof(schema=schema)
    return f"""-- dpone generic MSSQL transaction catalog v1 -> v2
USE {use_database};
GO
SET NOCOUNT ON;
SET XACT_ABORT ON;
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
BEGIN TRY
    BEGIN TRANSACTION;
    DECLARE @dpone_catalog_lock_result int;
    EXEC @dpone_catalog_lock_result = sys.sp_getapplock
        @Resource = N'{lock_resource}',
        @LockMode = 'Exclusive',
        @LockOwner = 'Transaction',
        @LockTimeout = 0;
    IF @dpone_catalog_lock_result < 0
        THROW 51000, 'DPONE_GENERIC_TRANSACTION_V2_UPGRADE_LOCK_UNAVAILABLE', 1;

{common_proof}

    IF EXISTS (
        SELECT invocation_digest
        FROM {attempt} WITH (UPDLOCK, HOLDLOCK)
        GROUP BY invocation_digest
        HAVING COUNT_BIG(*) > 1
    )
        THROW 51000, 'DPONE_GENERIC_TRANSACTION_V2_DUPLICATE_INVOCATION', 1;

    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'{_sql_text(f"{_quote(schema)}.{_quote(ATTEMPT_TABLE)}")}')
          AND name = N'uq_dpone_load_attempt_invocation'
    )
        ALTER TABLE {attempt}
        ADD CONSTRAINT [uq_dpone_load_attempt_invocation]
            UNIQUE NONCLUSTERED (invocation_digest);

{invocation_index_proof}

    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
    THROW;
END CATCH;
GO
"""


def _name(database: str, schema: str, table: str) -> str:
    return MSSQLObjectName.from_parts(database=database, schema=schema, table=table, strict=True).quoted()


def _quote(value: str) -> str:
    return f"[{str(value).replace(']', ']]')}]"


def _sql_text(value: str) -> str:
    return str(value).replace("'", "''")


__all__ = ["render_generic_transaction_catalog_v1_to_v2_ddl"]
