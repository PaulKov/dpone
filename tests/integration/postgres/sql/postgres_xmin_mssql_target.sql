CREATE TABLE [__DATABASE__].[dbo].[dpone_target_identity] (
    [binding_id] uniqueidentifier NOT NULL,
    [schema_name] nvarchar(128) COLLATE DATABASE_DEFAULT NOT NULL,
    [table_name] nvarchar(128) COLLATE DATABASE_DEFAULT NOT NULL,
    [created_at_utc] datetime2(7) NOT NULL
        CONSTRAINT [df_dpone_target_identity_created] DEFAULT SYSUTCDATETIME(),
    CONSTRAINT [pk_dpone_target_identity] PRIMARY KEY CLUSTERED ([binding_id]),
    CONSTRAINT [uq_dpone_target_identity_name]
        UNIQUE NONCLUSTERED ([schema_name], [table_name])
)

-- dpone:statement
CREATE TRIGGER [dbo].[trg_dpone_target_identity_immutable]
ON [dbo].[dpone_target_identity]
INSTEAD OF UPDATE, DELETE
AS
    THROW 51000, 'DPONE_TARGET_IDENTITY_IMMUTABLE', 1;

-- dpone:statement
INSERT INTO [__DATABASE__].[dbo].[dpone_target_identity]
    ([binding_id], [schema_name], [table_name])
VALUES
    ('ad6fbf8f-e8bd-47f7-b004-7e70f42da3f2', N'sample_metrics', N'metric_values')

-- dpone:statement
CREATE TABLE [__DATABASE__].[sample_metrics].[metric_values] (
    [metric_code] nvarchar(450) COLLATE Latin1_General_100_BIN2 NOT NULL,
    [metric_value] float(53) NOT NULL,
    [note] nvarchar(max) NULL,
    [occurred_at] datetimeoffset(6) NOT NULL,
    [__dpone__run_id] char(26) NOT NULL,
    [__dpone__load_id] char(26) NOT NULL,
    [__dpone__row_hash] char(64) NOT NULL,
    [__dpone__extracted_at] datetime2(7) NOT NULL,
    [__dpone__loaded_at] datetime2(7) NOT NULL,
    [__dpone__deleted_at] datetime2(7) NULL
)

-- dpone:statement
CREATE UNIQUE NONCLUSTERED INDEX [ux_metric_values_metric_code]
    ON [__DATABASE__].[sample_metrics].[metric_values] ([metric_code])
    WITH (DATA_COMPRESSION = NONE)
