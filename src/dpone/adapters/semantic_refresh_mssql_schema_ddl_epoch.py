"""Machine-enforced SQL Server DDL epoch used by semantic-refresh execution."""

from __future__ import annotations


def render_ddl_epoch_schema(schema: str) -> str:
    """Render the singleton epoch and database DDL trigger without widening trusted inputs."""

    table = f"[{schema}].[semantic_refresh_ddl_epoch]"
    trigger_name = f"dpone_semantic_refresh_ddl_epoch_guard_{schema}"
    trigger_sql = f"""
CREATE OR ALTER TRIGGER [{trigger_name}]
ON DATABASE
FOR DDL_DATABASE_LEVEL_EVENTS
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @dpone_event xml = EVENTDATA();
    DECLARE @dpone_schema sysname = @dpone_event.value(
        '(/EVENT_INSTANCE/SchemaName)[1]', 'sysname'
    );
    DECLARE @dpone_object sysname = @dpone_event.value(
        '(/EVENT_INSTANCE/ObjectName)[1]', 'sysname'
    );
    IF COALESCE(@dpone_schema, N'') COLLATE Latin1_General_100_BIN2 <> N'{schema}'
       AND COALESCE(@dpone_object, N'') NOT LIKE N'%[_][_]dbt[_]tmp'
       AND COALESCE(@dpone_object, N'') NOT LIKE N'dpone[_]sr[_]before[_]%'
       AND COALESCE(@dpone_object, N'') NOT LIKE N'dpone[_]sr[_]after[_]%'
    BEGIN
        DECLARE @dpone_ddl_lock int;
        EXEC @dpone_ddl_lock = sys.sp_getapplock
            @Resource = N'dpone:semantic-refresh:ddl-freeze',
            @LockMode = N'Exclusive', @LockOwner = N'Transaction', @LockTimeout = 0;
        IF @dpone_ddl_lock < 0
            THROW 51117, 'DPONE_SEMANTIC_REFRESH_DDL_FREEZE_ACTIVE', 1;
        UPDATE {table} WITH (UPDLOCK, HOLDLOCK)
        SET current_epoch = current_epoch + 1,
            last_event_type = @dpone_event.value(
                '(/EVENT_INSTANCE/EventType)[1]', 'nvarchar(128)'
            ),
            last_changed_at = SYSUTCDATETIME()
        WHERE singleton_id = 1;
        IF @@ROWCOUNT <> 1
            THROW 51118, 'DPONE_SEMANTIC_REFRESH_DDL_EPOCH_UNAVAILABLE', 1;
    END;
END;
""".strip()
    escaped_trigger_sql = trigger_sql.replace("'", "''")
    return f"""
IF OBJECT_ID(N'{table}', N'U') IS NULL
CREATE TABLE {table} (
    singleton_id tinyint NOT NULL
        CONSTRAINT [pk_{schema}_sr_ddl_epoch] PRIMARY KEY
        CONSTRAINT [ck_{schema}_sr_ddl_epoch_singleton] CHECK (singleton_id = 1),
    current_epoch bigint NOT NULL,
    last_event_type nvarchar(128) NOT NULL,
    last_changed_at datetime2(6) NOT NULL
);

IF NOT EXISTS (SELECT 1 FROM {table} WITH (UPDLOCK, HOLDLOCK) WHERE singleton_id = 1)
INSERT INTO {table} (singleton_id, current_epoch, last_event_type, last_changed_at)
VALUES (1, 1, N'SCHEMA_INSTALL', SYSUTCDATETIME());

EXEC(N'{escaped_trigger_sql}');
""".strip()


__all__ = ["render_ddl_epoch_schema"]
