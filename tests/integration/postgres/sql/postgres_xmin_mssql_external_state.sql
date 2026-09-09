CREATE TABLE [__DATABASE__].[system].[dpone_source_state] (
    [state_key] binary(32) NOT NULL,
    [contract_version] nvarchar(64) NOT NULL,
    [environment] nvarchar(128) NOT NULL,
    [process_name] nvarchar(512) NOT NULL,
    [source_connection] nvarchar(512) NOT NULL,
    [source_database] nvarchar(256) NOT NULL,
    [source_schema] nvarchar(256) NOT NULL,
    [source_table] nvarchar(256) NOT NULL,
    [target_database] nvarchar(256) NOT NULL,
    [target_schema] nvarchar(256) NOT NULL,
    [target_table] nvarchar(256) NOT NULL,
    [target_identity] binary(32) NOT NULL,
    [unique_key_json] nvarchar(max) NOT NULL,
    [schema_hash] char(71) NOT NULL,
    [scope_hash] char(71) NOT NULL,
    [xmin_value] bigint NOT NULL,
    [state_revision] bigint NOT NULL DEFAULT (0),
    [is_initial] bit NOT NULL,
    [wraparound_detected] bit NOT NULL,
    [frozen_xid] bigint NULL,
    [source_snapshot_token] char(71) NOT NULL,
    [last_load_id] nvarchar(128) NOT NULL,
    [superseded_at_utc] datetime2(7) NULL,
    [superseded_by_state_key] binary(32) NULL,
    [__dpone__loaded_at] datetime2(7) NOT NULL,
    [__dpone__updated_at] datetime2(7) NOT NULL,
    CONSTRAINT [pk_dpone_source_state] PRIMARY KEY ([state_key]),
    CONSTRAINT [ck_dpone_source_state_supersession] CHECK (
        ([superseded_at_utc] IS NULL AND [superseded_by_state_key] IS NULL)
        OR ([superseded_at_utc] IS NOT NULL AND [superseded_by_state_key] IS NOT NULL
            AND [superseded_by_state_key] <> [state_key])
    )
)

-- dpone:statement
CREATE UNIQUE NONCLUSTERED INDEX [uq_dpone_source_state_active_target]
ON [__DATABASE__].[system].[dpone_source_state] (
    [target_identity]
)
WHERE [superseded_at_utc] IS NULL

-- dpone:statement
CREATE TABLE [__DATABASE__].[system].[dpone_commit_receipt] (
    [receipt_id] nvarchar(128) NOT NULL,
    [state_key] binary(32) NOT NULL,
    [load_id] nvarchar(128) NOT NULL,
    [previous_xmin] bigint NULL,
    [candidate_xmin] bigint NOT NULL,
    [previous_revision] bigint NULL,
    [candidate_revision] bigint NOT NULL,
    [source_snapshot_token] char(71) NOT NULL,
    [publication_receipt_id] nvarchar(128) NULL,
    [committed_at] datetime2(7) NOT NULL,
    CONSTRAINT [pk_dpone_commit_receipt] PRIMARY KEY ([receipt_id]),
    CONSTRAINT [uq_dpone_commit_receipt_state_load] UNIQUE ([state_key], [load_id]),
    CONSTRAINT [fk_dpone_commit_receipt_state] FOREIGN KEY ([state_key])
        REFERENCES [system].[dpone_source_state] ([state_key])
)

-- dpone:statement
CREATE TABLE [__DATABASE__].[system].[dpone_repair_authority] (
    [authority_id] nvarchar(128) NOT NULL,
    [authority_digest] char(71) NOT NULL,
    [state_key] binary(32) NOT NULL,
    [transfer_from_state_key] binary(32) NULL,
    [transfer_from_xmin] bigint NULL,
    [transfer_from_revision] bigint NULL,
    [expected_checkpoint_absent] bit NOT NULL,
    [expected_xmin] bigint NULL,
    [expected_revision] bigint NULL,
    [scope_hash] char(71) NOT NULL,
    [reason] nvarchar(2048) NOT NULL,
    [expires_at_utc] datetime2(7) NOT NULL,
    [allow_full_baseline] bit NOT NULL,
    [allow_max_delete_rows] bigint NULL,
    [allow_max_delete_ratio] float(53) NULL,
    [created_at_utc] datetime2(7) NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT [pk_dpone_repair_authority] PRIMARY KEY ([authority_id]),
    CONSTRAINT [uq_dpone_repair_authority_digest] UNIQUE ([authority_digest]),
    CONSTRAINT [ck_dpone_repair_authority_checkpoint] CHECK (
        ([expected_checkpoint_absent] = 1 AND [expected_xmin] IS NULL AND [expected_revision] IS NULL)
        OR ([expected_checkpoint_absent] = 0 AND [expected_xmin] IS NOT NULL AND [expected_revision] IS NOT NULL)
    ),
    CONSTRAINT [ck_dpone_repair_authority_transfer] CHECK (
        ([transfer_from_state_key] IS NULL AND [transfer_from_xmin] IS NULL
            AND [transfer_from_revision] IS NULL)
        OR ([transfer_from_state_key] IS NOT NULL AND [transfer_from_xmin] IS NOT NULL
            AND [transfer_from_revision] IS NOT NULL AND [transfer_from_state_key] <> [state_key]
            AND [expected_checkpoint_absent] = 1 AND [allow_full_baseline] = 1)
    ),
    CONSTRAINT [ck_dpone_repair_authority_delete_rows] CHECK (
        [allow_max_delete_rows] IS NULL OR [allow_max_delete_rows] >= 0
    ),
    CONSTRAINT [ck_dpone_repair_authority_delete_ratio] CHECK (
        [allow_max_delete_ratio] IS NULL OR [allow_max_delete_ratio] BETWEEN 0.0 AND 1.0
    )
)

-- dpone:statement
CREATE TRIGGER [system].[trg_dpone_repair_authority_immutable]
ON [system].[dpone_repair_authority]
INSTEAD OF UPDATE, DELETE
AS
    THROW 51000, 'DPONE_REPAIR_AUTHORITY_IMMUTABLE', 1;

-- dpone:statement
CREATE TABLE [__DATABASE__].[system].[dpone_repair_authority_consumption] (
    [authority_id] nvarchar(128) NOT NULL,
    [authority_digest] char(71) NOT NULL,
    [state_key] binary(32) NOT NULL,
    [load_id] nvarchar(128) NOT NULL,
    [receipt_id] nvarchar(128) NOT NULL,
    [used_full_baseline] bit NOT NULL,
    [observed_delete_rows] bigint NOT NULL,
    [observed_delete_ratio] float(53) NOT NULL,
    [consumed_at_utc] datetime2(7) NOT NULL,
    CONSTRAINT [pk_dpone_repair_authority_consumption] PRIMARY KEY ([authority_id]),
    CONSTRAINT [uq_dpone_repair_authority_consumption_receipt] UNIQUE ([receipt_id]),
    CONSTRAINT [fk_dpone_repair_authority_consumption_authority] FOREIGN KEY ([authority_id])
        REFERENCES [system].[dpone_repair_authority] ([authority_id]),
    CONSTRAINT [fk_dpone_repair_authority_consumption_state] FOREIGN KEY ([state_key])
        REFERENCES [system].[dpone_source_state] ([state_key]),
    CONSTRAINT [fk_dpone_repair_authority_consumption_receipt] FOREIGN KEY ([receipt_id])
        REFERENCES [system].[dpone_commit_receipt] ([receipt_id]),
    CONSTRAINT [ck_dpone_repair_authority_consumption_rows] CHECK ([observed_delete_rows] >= 0),
    CONSTRAINT [ck_dpone_repair_authority_consumption_ratio] CHECK (
        [observed_delete_ratio] BETWEEN 0.0 AND 1.0
    )
)

-- dpone:statement
CREATE TABLE [__DATABASE__].[system].[dpone_run_state] (
    [id] bigint IDENTITY(1,1) NOT NULL,
    [run_state_key] binary(32) NOT NULL,
    [dag_id] nvarchar(512) NOT NULL,
    [process_name] nvarchar(512) NOT NULL,
    [source_schema] nvarchar(256) NOT NULL,
    [source_table] nvarchar(256) NOT NULL,
    [target_schema] nvarchar(256) NOT NULL,
    [target_table] nvarchar(256) NOT NULL,
    [load_strategy] nvarchar(64) NOT NULL,
    [execution_date] datetime2 NOT NULL,
    [state] nvarchar(32) NOT NULL,
    [started_at] datetime2 NOT NULL,
    [ended_at] datetime2 NULL,
    [duration_min] float NULL,
    [error_message] nvarchar(max) NULL,
    [rows_read] bigint NULL,
    [rows_written] bigint NULL,
    [rows_updated] bigint NULL,
    [rows_deleted] bigint NULL,
    [__dpone__loaded_at] datetime2 NOT NULL DEFAULT SYSUTCDATETIME(),
    [__dpone__updated_at] datetime2 NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT [pk_dpone_run_state] PRIMARY KEY CLUSTERED ([id]),
    CONSTRAINT [uq_dpone_run_state_key] UNIQUE NONCLUSTERED ([run_state_key])
)

-- dpone:statement
CREATE TABLE [__DATABASE__].[system].[dpone_load_audit] (
    [run_id] char(26) NOT NULL,
    [load_id] char(26) NOT NULL,
    [status] nvarchar(32) NOT NULL,
    [process_name] nvarchar(512) NULL,
    [source_schema] nvarchar(256) NOT NULL,
    [source_table] nvarchar(256) NOT NULL,
    [target_schema] nvarchar(256) NOT NULL,
    [target_table] nvarchar(256) NOT NULL,
    [strategy] nvarchar(64) NOT NULL,
    [started_at] datetime2 NOT NULL,
    [staged_at] datetime2 NULL,
    [committed_at] datetime2 NULL,
    [failed_at] datetime2 NULL,
    [extracted_rows] bigint NULL,
    [staged_rows] bigint NULL,
    [inserted_rows] bigint NULL,
    [updated_rows] bigint NULL,
    [loaded_rows] bigint NULL,
    [deleted_rows] bigint NULL,
    [reactivated_rows] bigint NULL,
    [unchanged_rows] bigint NULL,
    [soft_deleted_rows] bigint NULL,
    [hard_deleted_rows] bigint NULL,
    [active_rows] bigint NULL,
    [total_rows] bigint NULL,
    [commit_receipt_id] nvarchar(128) NULL,
    [commit_outcome] nvarchar(64) NULL,
    [error_message] nvarchar(max) NULL,
    [artifact_uri] nvarchar(max) NULL,
    [__dpone__loaded_at] datetime2 NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT [pk_dpone_load_audit] PRIMARY KEY ([load_id])
)
