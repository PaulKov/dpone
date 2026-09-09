"""Versioned SQL Server control schema for semantic refresh V2."""

from __future__ import annotations

import re
from collections.abc import Callable
from typing import Protocol

from dpone.adapters.semantic_refresh_mssql_schema_authority import (
    render_authority_schema,
)
from dpone.adapters.semantic_refresh_mssql_schema_cleanup import (
    render_cleanup_ack_schema,
)
from dpone.adapters.semantic_refresh_mssql_schema_ddl_epoch import render_ddl_epoch_schema
from dpone.adapters.semantic_refresh_mssql_schema_publication import (
    render_publication_schema,
)
from dpone.adapters.semantic_refresh_mssql_schema_workspace import (
    render_workspace_activation_schema,
)

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
SEMANTIC_REFRESH_MSSQL_SCHEMA_VERSION = 24


class _Cursor(Protocol):
    def execute(self, sql: str, *parameters: object) -> _Cursor: ...

    def close(self) -> None: ...

    def nextset(self) -> bool: ...


class _Connection(Protocol):
    autocommit: bool

    def cursor(self) -> _Cursor: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...

    def close(self) -> None: ...


class SemanticRefreshMssqlSchemaError(RuntimeError):
    """Raised when the V2 control schema cannot be installed atomically."""


class MssqlSemanticRefreshSchemaMigration:
    """Install the additive V2 control schema through one idempotent migration."""

    def __init__(
        self,
        connection_factory: Callable[[], _Connection],
        *,
        control_schema: str = "dpone_control",
    ) -> None:
        self._connection_factory = connection_factory
        self._control_schema = _require_identifier(control_schema)

    def apply(self) -> None:
        """Create all V2 state tables or acknowledge the exact installed version."""

        connection: _Connection | None = None
        cursor: _Cursor | None = None
        try:
            connection = self._connection_factory()
            connection.autocommit = False
            cursor = connection.cursor()
            cursor.execute(self.render())
            while cursor.nextset():
                pass
            connection.commit()
        except Exception as exc:
            if connection is not None:
                try:
                    connection.rollback()
                except Exception:
                    pass
            raise SemanticRefreshMssqlSchemaError("semantic refresh SQL Server schema migration failed") from exc
        finally:
            if cursor is not None:
                cursor.close()
            if connection is not None:
                connection.close()

    def render(self) -> str:
        """Return the credential-free, transaction-scoped migration batch."""

        schema = self._control_schema

        def table(name: str) -> str:
            return f"[{schema}].[{name}]"

        return f"""{render_authority_schema(schema, SEMANTIC_REFRESH_MSSQL_SCHEMA_VERSION)}

IF OBJECT_ID(N'{table("semantic_refresh_reservations")}', N'U') IS NULL
CREATE TABLE {table("semantic_refresh_reservations")} (
    reservation_id nvarchar(512) NOT NULL PRIMARY KEY,
    workflow_id nvarchar(512) NOT NULL,
    workflow_execution_binding_sha256 varchar(71) NOT NULL,
    status nvarchar(32) NOT NULL,
    max_prepared_models bigint NOT NULL,
    max_sealed_extract_bytes bigint NOT NULL,
    max_clickhouse_staging_bytes bigint NOT NULL,
    max_shadow_bytes bigint NOT NULL,
    max_peak_bytes bigint NOT NULL,
    reserved_prepared_models bigint NOT NULL CONSTRAINT [df_{schema}_sr_rpm] DEFAULT 0,
    reserved_sealed_extract_bytes bigint NOT NULL CONSTRAINT [df_{schema}_sr_rse] DEFAULT 0,
    reserved_clickhouse_staging_bytes bigint NOT NULL CONSTRAINT [df_{schema}_sr_rcs] DEFAULT 0,
    reserved_shadow_bytes bigint NOT NULL CONSTRAINT [df_{schema}_sr_rsh] DEFAULT 0,
    reserved_retained_generation_bytes bigint NOT NULL CONSTRAINT [df_{schema}_sr_rrg] DEFAULT 0,
    reserved_peak_bytes bigint NOT NULL CONSTRAINT [df_{schema}_sr_rpk] DEFAULT 0
);

IF COL_LENGTH(N'{table("semantic_refresh_reservations")}', N'max_prepared_models') IS NULL
ALTER TABLE {table("semantic_refresh_reservations")} ADD max_prepared_models bigint NULL;
IF COL_LENGTH(N'{table("semantic_refresh_reservations")}', N'max_sealed_extract_bytes') IS NULL
ALTER TABLE {table("semantic_refresh_reservations")} ADD max_sealed_extract_bytes bigint NULL;
IF COL_LENGTH(N'{table("semantic_refresh_reservations")}', N'max_clickhouse_staging_bytes') IS NULL
ALTER TABLE {table("semantic_refresh_reservations")} ADD max_clickhouse_staging_bytes bigint NULL;
IF COL_LENGTH(N'{table("semantic_refresh_reservations")}', N'max_shadow_bytes') IS NULL
ALTER TABLE {table("semantic_refresh_reservations")} ADD max_shadow_bytes bigint NULL;
IF COL_LENGTH(N'{table("semantic_refresh_reservations")}', N'max_peak_bytes') IS NULL
ALTER TABLE {table("semantic_refresh_reservations")} ADD max_peak_bytes bigint NULL;
IF COL_LENGTH(N'{table("semantic_refresh_reservations")}', N'reserved_prepared_models') IS NULL
ALTER TABLE {table("semantic_refresh_reservations")} ADD reserved_prepared_models bigint NOT NULL DEFAULT 0;
IF COL_LENGTH(N'{table("semantic_refresh_reservations")}', N'reserved_sealed_extract_bytes') IS NULL
ALTER TABLE {table("semantic_refresh_reservations")} ADD reserved_sealed_extract_bytes bigint NOT NULL DEFAULT 0;
IF COL_LENGTH(N'{table("semantic_refresh_reservations")}', N'reserved_clickhouse_staging_bytes') IS NULL
ALTER TABLE {table("semantic_refresh_reservations")} ADD reserved_clickhouse_staging_bytes bigint NOT NULL DEFAULT 0;
IF COL_LENGTH(N'{table("semantic_refresh_reservations")}', N'reserved_shadow_bytes') IS NULL
ALTER TABLE {table("semantic_refresh_reservations")} ADD reserved_shadow_bytes bigint NOT NULL DEFAULT 0;
IF COL_LENGTH(N'{table("semantic_refresh_reservations")}', N'reserved_peak_bytes') IS NULL
ALTER TABLE {table("semantic_refresh_reservations")} ADD reserved_peak_bytes bigint NOT NULL DEFAULT 0;
IF COL_LENGTH(N'{table("semantic_refresh_reservations")}', N'reserved_retained_generation_bytes') IS NULL
ALTER TABLE {table("semantic_refresh_reservations")}
ADD reserved_retained_generation_bytes bigint NOT NULL DEFAULT 0;

IF OBJECT_ID(N'{table("semantic_refresh_resource_allocations")}', N'U') IS NULL
CREATE TABLE {table("semantic_refresh_resource_allocations")} (
    allocation_id nvarchar(512) NOT NULL PRIMARY KEY,
    reservation_id nvarchar(512) NOT NULL,
    resource_kind nvarchar(64) NOT NULL,
    amount bigint NOT NULL,
    status nvarchar(32) NOT NULL,
    created_at_utc datetime2(6) NOT NULL
        CONSTRAINT [df_{schema}_sr_allocation_created] DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID(N'{table("semantic_refresh_journals")}', N'U') IS NULL
CREATE TABLE {table("semantic_refresh_journals")} (
    model_unique_id nvarchar(512) NOT NULL,
    operation_id nvarchar(512) NOT NULL PRIMARY KEY,
    operation_plan_sha256 varchar(71) NOT NULL,
    attempt_binding_sha256 varchar(71) NOT NULL,
    strategy_authority_json nvarchar(max) NOT NULL,
    strategy_authority_sha256 varchar(71) NOT NULL,
    baseline_receipt_sha256 varchar(71) NOT NULL,
    baseline_kind nvarchar(64) NOT NULL,
    baseline_receipt_json nvarchar(max) NOT NULL,
    baseline_status nvarchar(32) NOT NULL,
    image_key_columns_json nvarchar(max) NOT NULL,
    workflow_id nvarchar(512) NOT NULL,
    target_resource_id nvarchar(512) NOT NULL,
    publication_database nvarchar(128) NOT NULL,
    publication_target_table nvarchar(128) NOT NULL,
    publication_scope_id nvarchar(512) NOT NULL,
    target_predecessor_generation_id varchar(71) NOT NULL,
    scope_predecessor_operation_id nvarchar(512) NULL,
    predecessor_target_generation bigint NOT NULL,
    predecessor_target_uuid uniqueidentifier NOT NULL,
    predecessor_target_operation_id nvarchar(512) NOT NULL,
    predecessor_scope_revision bigint NULL,
    predecessor_checkpoint_sha256 varchar(71) NULL,
    predecessor_checkpoint_operation_id nvarchar(512) NULL,
    predecessor_checkpoint_version bigint NULL,
    fencing_epoch bigint NOT NULL,
    replaces_failed_operation_id nvarchar(512) NULL,
    owner_id nvarchar(512) NOT NULL,
    journal_version bigint NOT NULL,
    status nvarchar(32) NOT NULL,
    prepare_receipt_sha256 varchar(71) NULL,
    clickhouse_commit_receipt_sha256 varchar(71) NULL,
    terminal_receipt_sha256 varchar(71) NULL,
    mssql_outcome nvarchar(32) NULL,
    mssql_evidence_sha256 varchar(71) NULL,
    target_uuid uniqueidentifier NULL
);

IF COL_LENGTH(N'{table("semantic_refresh_journals")}', N'mssql_outcome') IS NULL
ALTER TABLE {table("semantic_refresh_journals")}
ADD mssql_outcome nvarchar(32) NULL;

IF COL_LENGTH(N'{table("semantic_refresh_journals")}', N'mssql_evidence_sha256') IS NULL
ALTER TABLE {table("semantic_refresh_journals")}
ADD mssql_evidence_sha256 varchar(71) NULL;

IF COL_LENGTH(N'{table("semantic_refresh_journals")}', N'baseline_receipt_sha256') IS NULL
ALTER TABLE {table("semantic_refresh_journals")}
ADD baseline_receipt_sha256 varchar(71) NULL;

IF COL_LENGTH(N'{table("semantic_refresh_journals")}', N'baseline_status') IS NULL
ALTER TABLE {table("semantic_refresh_journals")}
ADD baseline_status nvarchar(32) NULL;

IF COL_LENGTH(N'{table("semantic_refresh_journals")}', N'strategy_authority_json') IS NULL
BEGIN
IF EXISTS (SELECT 1 FROM {table("semantic_refresh_journals")} WITH (UPDLOCK, HOLDLOCK))
    THROW 51111, 'DPONE_SEMANTIC_REFRESH_LEGACY_STRATEGY_AUTHORITY_REQUIRES_EXPLICIT_MIGRATION', 1;
ALTER TABLE {table("semantic_refresh_journals")}
ADD strategy_authority_json nvarchar(max) NULL;
END;
ALTER TABLE {table("semantic_refresh_journals")}
ALTER COLUMN strategy_authority_json nvarchar(max) NOT NULL;

IF COL_LENGTH(N'{table("semantic_refresh_journals")}', N'journal_version') IS NULL
BEGIN
IF EXISTS (SELECT 1 FROM {table("semantic_refresh_journals")} WITH (UPDLOCK, HOLDLOCK))
    THROW 51112, 'DPONE_SEMANTIC_REFRESH_LEGACY_JOURNAL_VERSION_REQUIRES_EXPLICIT_MIGRATION', 1;
ALTER TABLE {table("semantic_refresh_journals")} ADD journal_version bigint NULL;
END;
ALTER TABLE {table("semantic_refresh_journals")} ALTER COLUMN journal_version bigint NOT NULL;

IF (
    COL_LENGTH(N'{table("semantic_refresh_journals")}', N'baseline_kind') IS NULL
    OR COL_LENGTH(N'{table("semantic_refresh_journals")}', N'baseline_receipt_json') IS NULL
) AND EXISTS (SELECT 1 FROM {table("semantic_refresh_journals")} WITH (UPDLOCK, HOLDLOCK))
    THROW 51110, 'DPONE_SEMANTIC_REFRESH_LEGACY_JOURNAL_BASELINE_REQUIRES_EXPLICIT_MIGRATION', 1;

IF COL_LENGTH(N'{table("semantic_refresh_journals")}', N'baseline_kind') IS NULL
ALTER TABLE {table("semantic_refresh_journals")} ADD baseline_kind nvarchar(64) NULL;
IF COL_LENGTH(N'{table("semantic_refresh_journals")}', N'baseline_receipt_json') IS NULL
ALTER TABLE {table("semantic_refresh_journals")} ADD baseline_receipt_json nvarchar(max) NULL;
ALTER TABLE {table("semantic_refresh_journals")} ALTER COLUMN baseline_kind nvarchar(64) NOT NULL;
ALTER TABLE {table("semantic_refresh_journals")} ALTER COLUMN baseline_receipt_json nvarchar(max) NOT NULL;

IF COL_LENGTH(N'{table("semantic_refresh_journals")}', N'image_key_columns_json') IS NULL
BEGIN
IF EXISTS (SELECT 1 FROM {table("semantic_refresh_journals")} WITH (UPDLOCK, HOLDLOCK))
    THROW 51104, 'DPONE_SEMANTIC_REFRESH_LEGACY_JOURNAL_STATE_REQUIRES_EXPLICIT_MIGRATION', 1;
ALTER TABLE {table("semantic_refresh_journals")}
ADD image_key_columns_json nvarchar(max) NULL;
END;

ALTER TABLE {table("semantic_refresh_journals")}
ALTER COLUMN image_key_columns_json nvarchar(max) NOT NULL;

IF (
    COL_LENGTH(N'{table("semantic_refresh_journals")}', N'publication_database') IS NULL
    OR COL_LENGTH(N'{table("semantic_refresh_journals")}', N'predecessor_target_generation') IS NULL
    OR COL_LENGTH(N'{table("semantic_refresh_journals")}', N'predecessor_checkpoint_version') IS NULL
) AND EXISTS (SELECT 1 FROM {table("semantic_refresh_journals")} WITH (UPDLOCK, HOLDLOCK))
    THROW 51106, 'DPONE_SEMANTIC_REFRESH_LEGACY_PREDECESSOR_STATE_REQUIRES_EXPLICIT_MIGRATION', 1;

IF COL_LENGTH(N'{table("semantic_refresh_journals")}', N'publication_database') IS NULL
ALTER TABLE {table("semantic_refresh_journals")} ADD publication_database nvarchar(128) NULL;
IF COL_LENGTH(N'{table("semantic_refresh_journals")}', N'publication_target_table') IS NULL
ALTER TABLE {table("semantic_refresh_journals")} ADD publication_target_table nvarchar(128) NULL;
IF COL_LENGTH(N'{table("semantic_refresh_journals")}', N'publication_scope_id') IS NULL
ALTER TABLE {table("semantic_refresh_journals")} ADD publication_scope_id nvarchar(512) NULL;
IF COL_LENGTH(N'{table("semantic_refresh_journals")}', N'target_predecessor_generation_id') IS NULL
ALTER TABLE {table("semantic_refresh_journals")} ADD target_predecessor_generation_id varchar(71) NULL;
IF COL_LENGTH(N'{table("semantic_refresh_journals")}', N'scope_predecessor_operation_id') IS NULL
ALTER TABLE {table("semantic_refresh_journals")} ADD scope_predecessor_operation_id nvarchar(512) NULL;
IF COL_LENGTH(N'{table("semantic_refresh_journals")}', N'predecessor_target_generation') IS NULL
ALTER TABLE {table("semantic_refresh_journals")} ADD predecessor_target_generation bigint NULL;
IF COL_LENGTH(N'{table("semantic_refresh_journals")}', N'predecessor_target_uuid') IS NULL
ALTER TABLE {table("semantic_refresh_journals")} ADD predecessor_target_uuid uniqueidentifier NULL;
IF COL_LENGTH(N'{table("semantic_refresh_journals")}', N'predecessor_target_operation_id') IS NULL
ALTER TABLE {table("semantic_refresh_journals")} ADD predecessor_target_operation_id nvarchar(512) NULL;
IF COL_LENGTH(N'{table("semantic_refresh_journals")}', N'predecessor_scope_revision') IS NULL
ALTER TABLE {table("semantic_refresh_journals")} ADD predecessor_scope_revision bigint NULL;
IF COL_LENGTH(N'{table("semantic_refresh_journals")}', N'predecessor_checkpoint_sha256') IS NULL
ALTER TABLE {table("semantic_refresh_journals")} ADD predecessor_checkpoint_sha256 varchar(71) NULL;
IF COL_LENGTH(N'{table("semantic_refresh_journals")}', N'predecessor_checkpoint_operation_id') IS NULL
ALTER TABLE {table("semantic_refresh_journals")} ADD predecessor_checkpoint_operation_id nvarchar(512) NULL;
IF COL_LENGTH(N'{table("semantic_refresh_journals")}', N'predecessor_checkpoint_version') IS NULL
ALTER TABLE {table("semantic_refresh_journals")} ADD predecessor_checkpoint_version bigint NULL;

ALTER TABLE {table("semantic_refresh_journals")} ALTER COLUMN publication_database nvarchar(128) NOT NULL;
ALTER TABLE {table("semantic_refresh_journals")} ALTER COLUMN publication_target_table nvarchar(128) NOT NULL;
ALTER TABLE {table("semantic_refresh_journals")} ALTER COLUMN publication_scope_id nvarchar(512) NOT NULL;
ALTER TABLE {table("semantic_refresh_journals")} ALTER COLUMN target_predecessor_generation_id varchar(71) NOT NULL;
ALTER TABLE {table("semantic_refresh_journals")} ALTER COLUMN predecessor_target_generation bigint NOT NULL;
ALTER TABLE {table("semantic_refresh_journals")} ALTER COLUMN predecessor_target_uuid uniqueidentifier NOT NULL;
ALTER TABLE {table("semantic_refresh_journals")} ALTER COLUMN predecessor_target_operation_id nvarchar(512) NOT NULL;

IF COL_LENGTH(N'{table("semantic_refresh_journals")}', N'replaces_failed_operation_id') IS NULL
ALTER TABLE {table("semantic_refresh_journals")}
ADD replaces_failed_operation_id nvarchar(512) NULL;

IF OBJECT_ID(N'{table("semantic_refresh_baselines")}', N'U') IS NULL
CREATE TABLE {table("semantic_refresh_baselines")} (
    target_resource_id nvarchar(512) NOT NULL PRIMARY KEY,
    model_unique_id nvarchar(512) NOT NULL,
    baseline_kind nvarchar(64) NOT NULL,
    baseline_receipt_sha256 varchar(71) NOT NULL,
    baseline_receipt_json nvarchar(max) NOT NULL,
    status nvarchar(32) NOT NULL,
    is_current bit NOT NULL,
    created_at_utc datetime2(6) NOT NULL
        CONSTRAINT [df_{schema}_sr_baseline_created] DEFAULT SYSUTCDATETIME()
);

IF (
    COL_LENGTH(N'{table("semantic_refresh_baselines")}', N'baseline_kind') IS NULL
    OR COL_LENGTH(N'{table("semantic_refresh_baselines")}', N'baseline_receipt_json') IS NULL
) AND EXISTS (SELECT 1 FROM {table("semantic_refresh_baselines")} WITH (UPDLOCK, HOLDLOCK))
    THROW 51109, 'DPONE_SEMANTIC_REFRESH_LEGACY_BASELINE_REQUIRES_EXPLICIT_MIGRATION', 1;

IF COL_LENGTH(N'{table("semantic_refresh_baselines")}', N'baseline_kind') IS NULL
ALTER TABLE {table("semantic_refresh_baselines")} ADD baseline_kind nvarchar(64) NULL;
IF COL_LENGTH(N'{table("semantic_refresh_baselines")}', N'baseline_receipt_json') IS NULL
ALTER TABLE {table("semantic_refresh_baselines")} ADD baseline_receipt_json nvarchar(max) NULL;
ALTER TABLE {table("semantic_refresh_baselines")} ALTER COLUMN baseline_kind nvarchar(64) NOT NULL;
ALTER TABLE {table("semantic_refresh_baselines")} ALTER COLUMN baseline_receipt_json nvarchar(max) NOT NULL;

{render_cleanup_ack_schema(schema)}

{render_workspace_activation_schema(schema)}

{render_ddl_epoch_schema(schema)}

{render_publication_schema(schema, SEMANTIC_REFRESH_MSSQL_SCHEMA_VERSION)}
""".strip()


def _require_identifier(value: str) -> str:
    if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
        raise ValueError("control_schema must be a simple SQL identifier")
    return value


__all__ = [
    "MssqlSemanticRefreshSchemaMigration",
    "SEMANTIC_REFRESH_MSSQL_SCHEMA_VERSION",
    "SemanticRefreshMssqlSchemaError",
]
