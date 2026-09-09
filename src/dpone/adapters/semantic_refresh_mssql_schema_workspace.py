"""Workspace activation tables sharing semantic-refresh physical guards."""

from __future__ import annotations


def render_workspace_activation_schema(control_schema: str) -> str:
    """Render additive occurrence and exact guard-epoch ownership tables."""

    schema = control_schema

    def table(name: str) -> str:
        return f"[{schema}].[{name}]"

    return f"""
IF OBJECT_ID(N'{table("dbt_workspace_activations")}', N'U') IS NULL
CREATE TABLE {table("dbt_workspace_activations")} (
    activation_id uniqueidentifier NOT NULL PRIMARY KEY,
    request_sha256 varchar(71) NOT NULL,
    environment nvarchar(63) NOT NULL,
    release_id varchar(71) NOT NULL,
    deployment_id varchar(71) NOT NULL,
    previous_deployment_id varchar(71) NULL,
    source_inventory_sha256 varchar(71) NOT NULL,
    runtime_context_sha256 varchar(71) NOT NULL,
    state nvarchar(32) NOT NULL,
    created_at_utc datetime2(6) NOT NULL
        CONSTRAINT [df_{schema}_dbt_workspace_activation_created] DEFAULT SYSUTCDATETIME(),
    updated_at_utc datetime2(6) NOT NULL
        CONSTRAINT [df_{schema}_dbt_workspace_activation_updated] DEFAULT SYSUTCDATETIME(),
    CONSTRAINT [ck_{schema}_dbt_workspace_activation_state]
        CHECK (state IN (N'PREPARED', N'ACTIVE', N'RETIRING', N'RETIRED'))
);

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'{table("dbt_workspace_activations")}', N'U')
      AND name = N'ux_{schema}_dbt_workspace_activation_request'
)
CREATE UNIQUE INDEX [ux_{schema}_dbt_workspace_activation_request]
ON {table("dbt_workspace_activations")} (request_sha256);

IF OBJECT_ID(N'{table("dbt_workspace_activation_guards")}', N'U') IS NULL
CREATE TABLE {table("dbt_workspace_activation_guards")} (
    activation_id uniqueidentifier NOT NULL,
    guard_id nvarchar(512) NOT NULL,
    resource_sha256 varchar(71) NOT NULL,
    fencing_epoch bigint NOT NULL,
    CONSTRAINT [pk_{schema}_dbt_workspace_activation_guards]
        PRIMARY KEY (activation_id, guard_id),
    CONSTRAINT [fk_{schema}_dbt_workspace_activation_guards]
        FOREIGN KEY (activation_id)
        REFERENCES {table("dbt_workspace_activations")} (activation_id),
    CONSTRAINT [ck_{schema}_dbt_workspace_activation_epoch]
        CHECK (fencing_epoch > 0)
);

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'{table("dbt_workspace_activation_guards")}', N'U')
      AND name = N'ix_{schema}_dbt_workspace_activation_guard'
)
CREATE INDEX [ix_{schema}_dbt_workspace_activation_guard]
ON {table("dbt_workspace_activation_guards")} (guard_id, activation_id);

IF OBJECT_ID(N'{table("dbt_workspace_activation_write_subjects")}', N'U') IS NULL
CREATE TABLE {table("dbt_workspace_activation_write_subjects")} (
    activation_id uniqueidentifier NOT NULL,
    write_subject_sha256 varchar(71) NOT NULL,
    guard_id nvarchar(512) NOT NULL,
    CONSTRAINT [pk_{schema}_dbt_workspace_activation_subjects]
        PRIMARY KEY (activation_id, write_subject_sha256),
    CONSTRAINT [fk_{schema}_dbt_workspace_activation_subject_activation]
        FOREIGN KEY (activation_id)
        REFERENCES {table("dbt_workspace_activations")} (activation_id),
    CONSTRAINT [fk_{schema}_dbt_workspace_activation_subject_guard]
        FOREIGN KEY (activation_id, guard_id)
        REFERENCES {table("dbt_workspace_activation_guards")} (activation_id, guard_id)
);

IF OBJECT_ID(N'{table("dbt_workspace_attempts")}', N'U') IS NULL
CREATE TABLE {table("dbt_workspace_attempts")} (
    attempt_id varchar(71) NOT NULL PRIMARY KEY,
    activation_id uniqueidentifier NOT NULL,
    request_sha256 varchar(71) NOT NULL,
    workflow_id nvarchar(64) NOT NULL,
    state nvarchar(32) NOT NULL,
    terminal_receipt_sha256 varchar(71) NULL,
    created_at_utc datetime2(6) NOT NULL
        CONSTRAINT [df_{schema}_dbt_workspace_attempt_created] DEFAULT SYSUTCDATETIME(),
    updated_at_utc datetime2(6) NOT NULL
        CONSTRAINT [df_{schema}_dbt_workspace_attempt_updated] DEFAULT SYSUTCDATETIME(),
    CONSTRAINT [fk_{schema}_dbt_workspace_attempt_activation]
        FOREIGN KEY (activation_id)
        REFERENCES {table("dbt_workspace_activations")} (activation_id),
    CONSTRAINT [ck_{schema}_dbt_workspace_attempt_state]
        CHECK (state IN (N'RUNNING', N'SUCCEEDED', N'FAILED', N'COMMIT_UNKNOWN')),
    CONSTRAINT [ck_{schema}_dbt_workspace_attempt_terminal]
        CHECK ((state = N'RUNNING' AND terminal_receipt_sha256 IS NULL)
            OR (state <> N'RUNNING' AND terminal_receipt_sha256 IS NOT NULL))
);

IF OBJECT_ID(N'{table("dbt_workspace_attempt_guards")}', N'U') IS NULL
CREATE TABLE {table("dbt_workspace_attempt_guards")} (
    attempt_id varchar(71) NOT NULL,
    guard_id nvarchar(512) NOT NULL,
    fencing_epoch bigint NOT NULL,
    CONSTRAINT [pk_{schema}_dbt_workspace_attempt_guards]
        PRIMARY KEY (attempt_id, guard_id),
    CONSTRAINT [fk_{schema}_dbt_workspace_attempt_guard_attempt]
        FOREIGN KEY (attempt_id)
        REFERENCES {table("dbt_workspace_attempts")} (attempt_id),
    CONSTRAINT [ck_{schema}_dbt_workspace_attempt_epoch]
        CHECK (fencing_epoch > 0)
);

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE object_id = OBJECT_ID(N'{table("dbt_workspace_attempt_guards")}', N'U')
      AND name = N'ix_{schema}_dbt_workspace_attempt_guard'
)
CREATE INDEX [ix_{schema}_dbt_workspace_attempt_guard]
ON {table("dbt_workspace_attempt_guards")} (guard_id, attempt_id);
""".strip()


__all__ = ["render_workspace_activation_schema"]
