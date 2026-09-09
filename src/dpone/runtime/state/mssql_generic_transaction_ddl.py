"""Versioned operator DDL for generic MSSQL fencing and receipts."""

from __future__ import annotations

from dpone.contracts.mssql_object_name import MSSQLObjectName
from dpone.runtime.state.mssql_generic_fence_trigger import render_fence_trigger_body
from dpone.runtime.state.mssql_generic_operation_trigger import render_operation_trigger_body
from dpone.runtime.state.mssql_generic_transaction_migration_ddl import (
    render_generic_transaction_catalog_v1_to_v2_ddl,
)
from dpone.runtime.state.mssql_generic_transaction_names import (
    ATTEMPT_TABLE,
    ATTEMPT_TRIGGER,
    FENCE_TABLE,
    FENCE_TRIGGER,
    GENERIC_TRANSACTION_CATALOG_VERSION,
    OPERATION_TABLE,
    OPERATION_TRIGGER,
    RECEIPT_TABLE,
    RECEIPT_TRIGGER,
)


def render_generic_transaction_catalog_ddl(*, database: str, schema: str) -> str:
    """Render exact, one-time external DDL; runtime never executes it."""

    fence = _name(database, schema, FENCE_TABLE)
    attempt = _name(database, schema, ATTEMPT_TABLE)
    operation = _name(database, schema, OPERATION_TABLE)
    receipt = _name(database, schema, RECEIPT_TABLE)
    use_database = _quote(database)
    quoted_schema = _quote(schema)
    return f"""-- dpone generic MSSQL transaction catalog v{GENERIC_TRANSACTION_CATALOG_VERSION}
USE {use_database};
GO
CREATE TABLE {fence} (
    target_identity binary(32) NOT NULL,
    current_generation bigint NOT NULL,
    current_attempt_key binary(32) NOT NULL,
    current_route_fingerprint binary(32) NOT NULL,
    updated_at_utc datetime2(7) NOT NULL
        CONSTRAINT [df_dpone_target_fence_updated] DEFAULT SYSUTCDATETIME(),
    CONSTRAINT [pk_dpone_target_fence] PRIMARY KEY CLUSTERED (target_identity),
    CONSTRAINT [ck_dpone_target_fence_generation] CHECK (current_generation >= 1)
) WITH (DATA_COMPRESSION = NONE);
GO
CREATE TABLE {attempt} (
    attempt_key binary(32) NOT NULL,
    target_identity binary(32) NOT NULL,
    generation bigint NOT NULL,
    invocation_digest binary(32) NOT NULL,
    route_fingerprint binary(32) NOT NULL,
    first_load_id nvarchar(128) NOT NULL,
    target_database nvarchar(256) NOT NULL,
    target_schema nvarchar(256) NOT NULL,
    target_table nvarchar(256) NOT NULL,
    strategy nvarchar(64) NOT NULL,
    created_at_utc datetime2(7) NOT NULL
        CONSTRAINT [df_dpone_load_attempt_created] DEFAULT SYSUTCDATETIME(),
    CONSTRAINT [pk_dpone_load_attempt] PRIMARY KEY CLUSTERED (attempt_key),
    CONSTRAINT [uq_dpone_load_attempt_invocation] UNIQUE NONCLUSTERED (invocation_digest),
    CONSTRAINT [uq_dpone_load_attempt_generation] UNIQUE NONCLUSTERED (target_identity, generation),
    CONSTRAINT [uq_dpone_load_attempt_authority] UNIQUE NONCLUSTERED
        (attempt_key, target_identity, generation, route_fingerprint),
    CONSTRAINT [ck_dpone_load_attempt_generation] CHECK (generation >= 1)
) WITH (DATA_COMPRESSION = NONE);
GO
ALTER TABLE {fence} ADD CONSTRAINT [fk_dpone_target_fence_attempt]
    FOREIGN KEY (current_attempt_key, target_identity, current_generation, current_route_fingerprint)
    REFERENCES {attempt} (attempt_key, target_identity, generation, route_fingerprint);
GO
CREATE TABLE {operation} (
    operation_key binary(32) NOT NULL,
    attempt_key binary(32) NOT NULL,
    scope_hash binary(32) NOT NULL,
    current_epoch bigint NOT NULL,
    current_owner_digest binary(32) NOT NULL,
    lease_expires_at_utc datetime2(7) NULL,
    updated_at_utc datetime2(7) NOT NULL
        CONSTRAINT [df_dpone_load_operation_updated] DEFAULT SYSUTCDATETIME(),
    CONSTRAINT [pk_dpone_load_operation] PRIMARY KEY CLUSTERED (operation_key),
    CONSTRAINT [uq_dpone_load_operation_scope] UNIQUE NONCLUSTERED (attempt_key, scope_hash),
    CONSTRAINT [uq_dpone_load_operation_attempt] UNIQUE NONCLUSTERED (operation_key, attempt_key),
    CONSTRAINT [uq_dpone_load_operation_receipt_authority] UNIQUE NONCLUSTERED
        (operation_key, attempt_key, scope_hash, current_epoch, current_owner_digest),
    CONSTRAINT [fk_dpone_load_operation_attempt] FOREIGN KEY (attempt_key)
        REFERENCES {attempt} (attempt_key),
    CONSTRAINT [ck_dpone_load_operation_epoch] CHECK (current_epoch >= 1)
) WITH (DATA_COMPRESSION = NONE);
GO
CREATE TABLE {receipt} (
    receipt_id nvarchar(128) NOT NULL,
    operation_key binary(32) NOT NULL,
    attempt_key binary(32) NOT NULL,
    scope_hash binary(32) NOT NULL,
    operation_epoch bigint NOT NULL,
    owner_digest binary(32) NOT NULL,
    load_id nvarchar(128) NOT NULL,
    payload_manifest_sha256 binary(32) NOT NULL,
    declared_rows bigint NOT NULL,
    actual_raw_rows bigint NOT NULL,
    actual_native_rows bigint NOT NULL,
    native_contract_sha256 binary(32) NOT NULL,
    mutation_plan_sha256 binary(32) NOT NULL,
    target_before_sha256 binary(32) NOT NULL,
    target_after_sha256 binary(32) NOT NULL,
    extraction_started_at_utc datetime2(7) NOT NULL,
    extraction_completed_at_utc datetime2(7) NOT NULL,
    extraction_clock_authority nvarchar(128) NOT NULL,
    snapshot_acquired_at_utc datetime2(7) NULL,
    snapshot_authority nvarchar(128) NULL,
    source_token_sha256 binary(32) NULL,
    loaded_at_utc datetime2(7) NOT NULL,
    inserted_rows bigint NOT NULL,
    updated_rows bigint NOT NULL,
    total_rows bigint NOT NULL,
    staging_rows bigint NULL,
    replaced_rows bigint NULL,
    soft_deleted_rows bigint NULL,
    reactivated_rows bigint NULL,
    unchanged_rows bigint NULL,
    hard_deleted_rows bigint NULL,
    active_rows bigint NULL,
    committed_at_utc datetime2(7) NOT NULL
        CONSTRAINT [df_dpone_load_receipt_committed] DEFAULT SYSUTCDATETIME(),
    CONSTRAINT [pk_dpone_load_receipt] PRIMARY KEY CLUSTERED (receipt_id),
    CONSTRAINT [uq_dpone_load_receipt_operation] UNIQUE NONCLUSTERED (operation_key),
    CONSTRAINT [fk_dpone_load_receipt_operation] FOREIGN KEY (
        operation_key, attempt_key, scope_hash, operation_epoch, owner_digest
    ) REFERENCES {operation} (
        operation_key, attempt_key, scope_hash, current_epoch, current_owner_digest
    ),
    CONSTRAINT [ck_dpone_load_receipt_operation_epoch] CHECK (operation_epoch >= 1),
    CONSTRAINT [ck_dpone_load_receipt_payload_rows] CHECK (
        declared_rows >= 0
        AND declared_rows = actual_raw_rows
        AND actual_raw_rows = actual_native_rows
    ),
    CONSTRAINT [ck_dpone_load_receipt_lifecycle] CHECK (
        extraction_started_at_utc <= extraction_completed_at_utc
        AND (
            (snapshot_acquired_at_utc IS NULL
                AND snapshot_authority IS NULL
                AND source_token_sha256 IS NULL)
            OR (snapshot_acquired_at_utc IS NOT NULL
                AND snapshot_authority IS NOT NULL
                AND extraction_started_at_utc <= snapshot_acquired_at_utc
                AND snapshot_acquired_at_utc <= extraction_completed_at_utc)
        )
        AND loaded_at_utc <= committed_at_utc
    ),
    CONSTRAINT [ck_dpone_load_receipt_metrics] CHECK ({_metric_check()})
) WITH (DATA_COMPRESSION = NONE);
GO
CREATE TRIGGER {quoted_schema}.{_quote(FENCE_TRIGGER)}
ON {quoted_schema}.{_quote(FENCE_TABLE)}
AFTER UPDATE, DELETE
AS
{render_fence_trigger_body()}
GO
CREATE TRIGGER {quoted_schema}.{_quote(ATTEMPT_TRIGGER)}
ON {quoted_schema}.{_quote(ATTEMPT_TABLE)}
INSTEAD OF UPDATE, DELETE
AS
    THROW 51000, 'DPONE_LOAD_ATTEMPT_IMMUTABLE', 1;
GO
CREATE TRIGGER {quoted_schema}.{_quote(OPERATION_TRIGGER)}
ON {quoted_schema}.{_quote(OPERATION_TABLE)}
AFTER UPDATE, DELETE
AS
{render_operation_trigger_body()}
GO
CREATE TRIGGER {quoted_schema}.{_quote(RECEIPT_TRIGGER)}
ON {quoted_schema}.{_quote(RECEIPT_TABLE)}
INSTEAD OF UPDATE, DELETE
AS
    THROW 51000, 'DPONE_LOAD_RECEIPT_IMMUTABLE', 1;
GO
"""


def _metric_check() -> str:
    required = " AND ".join(f"{name} >= 0" for name in ("inserted_rows", "updated_rows", "total_rows"))
    optional = " AND ".join(
        f"({name} IS NULL OR {name} >= 0)"
        for name in (
            "staging_rows",
            "replaced_rows",
            "soft_deleted_rows",
            "reactivated_rows",
            "unchanged_rows",
            "hard_deleted_rows",
            "active_rows",
        )
    )
    return f"{required} AND {optional}"


def _name(database: str, schema: str, table: str) -> str:
    return MSSQLObjectName.from_parts(database=database, schema=schema, table=table, strict=True).quoted()


def _quote(value: str) -> str:
    return f"[{str(value).replace(']', ']]')}]"


__all__ = [
    "GENERIC_TRANSACTION_CATALOG_VERSION",
    "render_generic_transaction_catalog_ddl",
    "render_generic_transaction_catalog_v1_to_v2_ddl",
]
