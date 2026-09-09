"""Authority and workflow-guard DDL for the MSSQL semantic-refresh schema."""

from __future__ import annotations


def render_authority_schema(control_schema: str, schema_version: int) -> str:
    """Render the schema/version, guard, execution, and authority table batch."""

    schema = control_schema

    def table(name: str) -> str:
        return f"[{schema}].[{name}]"

    return f"""
SET XACT_ABORT ON;
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;
DECLARE @dpone_schema_lock int;
EXEC @dpone_schema_lock = sys.sp_getapplock
    @Resource = N'dpone:semantic-refresh:schema:{schema}',
    @LockMode = N'Exclusive', @LockOwner = N'Transaction', @LockTimeout = 0;
IF @dpone_schema_lock < 0
    THROW 51100, 'DPONE_SEMANTIC_REFRESH_SCHEMA_BUSY', 1;
DECLARE @dpone_ddl_freeze_lock int;
EXEC @dpone_ddl_freeze_lock = sys.sp_getapplock
    @Resource = N'dpone:semantic-refresh:ddl-freeze',
    @LockMode = N'Exclusive', @LockOwner = N'Transaction', @LockTimeout = 0;
IF @dpone_ddl_freeze_lock < 0
    THROW 51117, 'DPONE_SEMANTIC_REFRESH_DDL_FREEZE_ACTIVE', 1;

IF SCHEMA_ID(N'{schema}') IS NULL EXEC(N'CREATE SCHEMA [{schema}]');

IF OBJECT_ID(N'{table("semantic_refresh_schema_versions")}', N'U') IS NULL
CREATE TABLE {table("semantic_refresh_schema_versions")} (
    component nvarchar(128) NOT NULL PRIMARY KEY,
    schema_version int NOT NULL,
    installed_at_utc datetime2(6) NOT NULL
        CONSTRAINT [df_{schema}_sr_schema_installed] DEFAULT SYSUTCDATETIME()
);

IF EXISTS (
    SELECT 1 FROM {table("semantic_refresh_schema_versions")} WITH (UPDLOCK, HOLDLOCK)
    WHERE component = N'semantic_refresh_v2' AND schema_version > {schema_version}
)
    THROW 51101, 'DPONE_SEMANTIC_REFRESH_SCHEMA_VERSION_CONFLICT', 1;

IF OBJECT_ID(N'{table("semantic_refresh_guards")}', N'U') IS NULL
CREATE TABLE {table("semantic_refresh_guards")} (
    resource_id nvarchar(512) NOT NULL PRIMARY KEY,
    fencing_epoch bigint NOT NULL,
    owner_id nvarchar(512) NULL,
    workflow_id nvarchar(512) NULL,
    operation_id nvarchar(512) NULL,
    operation_plan_sha256 varchar(71) NULL,
    attempt_binding_sha256 varchar(71) NULL,
    strategy_authority_sha256 varchar(71) NULL,
    status nvarchar(32) NOT NULL
);

IF OBJECT_ID(N'{table("semantic_refresh_workflow_executions")}', N'U') IS NULL
CREATE TABLE {table("semantic_refresh_workflow_executions")} (
    workflow_id nvarchar(512) NOT NULL PRIMARY KEY,
    workflow_execution_id nvarchar(512) NOT NULL,
    workflow_plan_sha256 varchar(71) NULL,
    workflow_execution_binding_sha256 varchar(71) NULL,
    canonical_authority_sha256 varchar(71) NOT NULL,
    guard_set_sha256 varchar(71) NULL,
    journal_set_sha256 varchar(71) NULL,
    workflow_guard_resource_id nvarchar(512) NOT NULL,
    guard_count bigint NOT NULL,
    controller_id nvarchar(512) NULL,
    owner_id nvarchar(512) NULL,
    status nvarchar(32) NOT NULL,
    successor_workflow_id nvarchar(512) NULL,
    replacement_plan_sha256 varchar(71) NULL,
    terminal_summary_sha256 varchar(71) NULL,
    terminal_summary_json nvarchar(max) NULL,
    completed_at_utc datetime2(6) NULL
);

IF COL_LENGTH(N'{table("semantic_refresh_workflow_executions")}', N'terminal_summary_sha256') IS NULL
ALTER TABLE {table("semantic_refresh_workflow_executions")}
ADD terminal_summary_sha256 varchar(71) NULL;

IF COL_LENGTH(N'{table("semantic_refresh_workflow_executions")}', N'terminal_summary_json') IS NULL
BEGIN
IF EXISTS (
    SELECT 1 FROM {table("semantic_refresh_workflow_executions")} WITH (UPDLOCK, HOLDLOCK)
    WHERE status IN (N'COMPLETE', N'FAILED_PRE_COMMIT')
)
    THROW 51114, 'DPONE_SEMANTIC_REFRESH_TERMINAL_SUMMARY_REQUIRES_EXPLICIT_MIGRATION', 1;
ALTER TABLE {table("semantic_refresh_workflow_executions")}
ADD terminal_summary_json nvarchar(max) NULL;
END;

IF COL_LENGTH(N'{table("semantic_refresh_workflow_executions")}', N'completed_at_utc') IS NULL
ALTER TABLE {table("semantic_refresh_workflow_executions")}
ADD completed_at_utc datetime2(6) NULL;

IF (
    COL_LENGTH(N'{table("semantic_refresh_workflow_executions")}', N'workflow_execution_id') IS NULL
    OR COL_LENGTH(N'{table("semantic_refresh_workflow_executions")}', N'canonical_authority_sha256') IS NULL
    OR COL_LENGTH(N'{table("semantic_refresh_workflow_executions")}', N'workflow_guard_resource_id') IS NULL
    OR COL_LENGTH(N'{table("semantic_refresh_workflow_executions")}', N'guard_count') IS NULL
) AND EXISTS (SELECT 1 FROM {table("semantic_refresh_workflow_executions")} WITH (UPDLOCK, HOLDLOCK))
    THROW 51103, 'DPONE_SEMANTIC_REFRESH_LEGACY_EXECUTION_STATE_REQUIRES_EXPLICIT_MIGRATION', 1;

IF COL_LENGTH(N'{table("semantic_refresh_workflow_executions")}', N'workflow_execution_id') IS NULL
ALTER TABLE {table("semantic_refresh_workflow_executions")}
ADD workflow_execution_id nvarchar(512) NULL;

IF COL_LENGTH(N'{table("semantic_refresh_workflow_executions")}', N'canonical_authority_sha256') IS NULL
ALTER TABLE {table("semantic_refresh_workflow_executions")}
ADD canonical_authority_sha256 varchar(71) NULL;

IF COL_LENGTH(N'{table("semantic_refresh_workflow_executions")}', N'workflow_guard_resource_id') IS NULL
ALTER TABLE {table("semantic_refresh_workflow_executions")}
ADD workflow_guard_resource_id nvarchar(512) NULL;

IF COL_LENGTH(N'{table("semantic_refresh_workflow_executions")}', N'guard_count') IS NULL
ALTER TABLE {table("semantic_refresh_workflow_executions")}
ADD guard_count bigint NULL;

ALTER TABLE {table("semantic_refresh_workflow_executions")}
ALTER COLUMN workflow_execution_id nvarchar(512) NOT NULL;
ALTER TABLE {table("semantic_refresh_workflow_executions")}
ALTER COLUMN canonical_authority_sha256 varchar(71) NOT NULL;
ALTER TABLE {table("semantic_refresh_workflow_executions")}
ALTER COLUMN workflow_guard_resource_id nvarchar(512) NOT NULL;
ALTER TABLE {table("semantic_refresh_workflow_executions")}
ALTER COLUMN guard_count bigint NOT NULL;

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'{table("semantic_refresh_workflow_executions")}', N'U')
      AND name = N'ux_{schema}_sr_workflow_execution_id'
)
CREATE UNIQUE INDEX [ux_{schema}_sr_workflow_execution_id]
ON {table("semantic_refresh_workflow_executions")} (workflow_execution_id);

IF OBJECT_ID(N'{table("semantic_refresh_canonical_authorities")}', N'U') IS NULL
CREATE TABLE {table("semantic_refresh_canonical_authorities")} (
    workflow_execution_binding_sha256 varchar(71) NOT NULL PRIMARY KEY,
    workflow_execution_id nvarchar(512) NOT NULL UNIQUE,
    authority_sha256 varchar(71) NOT NULL UNIQUE,
    authority_json nvarchar(max) NOT NULL,
    status nvarchar(32) NOT NULL,
    created_at_utc datetime2(6) NOT NULL
        CONSTRAINT [df_{schema}_sr_authority_created] DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID(N'{table("semantic_refresh_deployment_activations")}', N'U') IS NULL
CREATE TABLE {table("semantic_refresh_deployment_activations")} (
    deployment_id varchar(71) NOT NULL PRIMARY KEY,
    deployment_subject_sha256 varchar(71) NOT NULL UNIQUE,
    status nvarchar(32) NOT NULL,
    created_at_utc datetime2(6) NOT NULL
        CONSTRAINT [df_{schema}_sr_deployment_activation_created] DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID(N'{table("semantic_refresh_activated_packs")}', N'U') IS NULL
CREATE TABLE {table("semantic_refresh_activated_packs")} (
    pack_fingerprint varchar(71) NOT NULL PRIMARY KEY,
    activation_authority_receipt_sha256 varchar(71) NOT NULL,
    authority_store_ref nvarchar(1024) NOT NULL,
    workflow_execution_id nvarchar(512) NOT NULL,
    workflow_execution_binding_sha256 varchar(71) NOT NULL UNIQUE,
    workflow_plan_sha256 varchar(71) NOT NULL,
    plan_bundle_sha256 varchar(71) NOT NULL,
    run_execution_bundle_sha256 varchar(71) NOT NULL,
    run_guard_closure_sha256 varchar(71) NOT NULL,
    workflow_guard_resource_id nvarchar(512) NOT NULL,
    resource_guard_ids_json nvarchar(max) NOT NULL,
    static_projection_identity_json nvarchar(max) NOT NULL,
    status nvarchar(16) NOT NULL
        CONSTRAINT [ck_{schema}_sr_activated_pack_status] CHECK (status = N'ACTIVE')
);

IF COL_LENGTH(N'{table("semantic_refresh_activated_packs")}', N'run_guard_closure_sha256') IS NULL
BEGIN
    IF EXISTS (SELECT 1 FROM {table("semantic_refresh_activated_packs")})
        THROW 51071, 'active legacy pack lacks run guard closure authority', 1;
    ALTER TABLE {table("semantic_refresh_activated_packs")}
    ADD run_guard_closure_sha256 varchar(71) NOT NULL,
        workflow_guard_resource_id nvarchar(512) NOT NULL,
        resource_guard_ids_json nvarchar(max) NOT NULL;
END;

IF COL_LENGTH(N'{table("semantic_refresh_activated_packs")}', N'static_projection_identity_json') IS NULL
BEGIN
    IF EXISTS (SELECT 1 FROM {table("semantic_refresh_activated_packs")})
        THROW 51072, 'active legacy pack lacks static projection authority', 1;
    ALTER TABLE {table("semantic_refresh_activated_packs")}
    ADD static_projection_identity_json nvarchar(max) NOT NULL;
END;
""".strip()


__all__ = ["render_authority_schema"]
