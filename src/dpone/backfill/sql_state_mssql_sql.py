"""Pure SQL rendering for the MSSQL resumable backfill journal."""

from __future__ import annotations

from dpone.backfill.sql_state_rendering import sql_string


def campaign_ddl(*, schema: str, table: str, qualified_table: str) -> str:
    """Render the append-only campaign journal and latest-row index."""

    return f"""
    IF OBJECT_ID(N'{schema}.{table}', N'U') IS NULL
    CREATE TABLE {qualified_table} (
        journal_id bigint IDENTITY(1,1) NOT NULL,
        run_key nvarchar(128) NOT NULL,
        dataset nvarchar(512) NOT NULL,
        inner_mode nvarchar(64) NOT NULL,
        status nvarchar(64) NOT NULL,
        plan_hash nvarchar(128) NOT NULL,
        config_hash nvarchar(128) NOT NULL,
        chunk_config_json nvarchar(max) NOT NULL,
        details_json nvarchar(max) NOT NULL,
        __dpone__loaded_at datetime2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'{schema}.{table}')
          AND name = N'ix_dpone_backfill_campaign_run_key_journal'
    )
    CREATE INDEX [ix_dpone_backfill_campaign_run_key_journal]
        ON {qualified_table} ([run_key], [journal_id] DESC)
    """


def chunk_ddl(*, schema: str, table: str, qualified_table: str) -> str:
    """Render the append-only chunk journal and point-read index."""

    return f"""
    IF OBJECT_ID(N'{schema}.{table}', N'U') IS NULL
    CREATE TABLE {qualified_table} (
        journal_id bigint IDENTITY(1,1) NOT NULL,
        run_key nvarchar(128) NOT NULL,
        chunk_index int NOT NULL,
        status nvarchar(64) NOT NULL,
        start_value nvarchar(128) NOT NULL,
        end_value nvarchar(128) NOT NULL,
        idempotency_key nvarchar(512) NOT NULL,
        run_id nvarchar(128) NOT NULL,
        load_id nvarchar(128) NOT NULL,
        error nvarchar(max) NOT NULL,
        details_json nvarchar(max) NOT NULL,
        __dpone__loaded_at datetime2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE object_id = OBJECT_ID(N'{schema}.{table}')
          AND name = N'ix_dpone_backfill_chunk_run_key_index_journal'
    )
    CREATE INDEX [ix_dpone_backfill_chunk_run_key_index_journal]
        ON {qualified_table} ([run_key], [chunk_index], [journal_id] DESC)
    """


def select_campaign(qualified_table: str, run_key: str) -> str:
    """Render a literalized latest campaign read for connector compatibility."""

    return (
        f"SELECT TOP (1) journal_id, details_json FROM {qualified_table} "
        f"WHERE run_key = {sql_string(run_key)} ORDER BY journal_id DESC"
    )


def select_chunks(qualified_table: str, run_key: str) -> str:
    """Render a literalized latest revision read for every campaign chunk."""

    return (
        "WITH latest_chunk AS ("
        "SELECT chunk_index, journal_id, details_json, "
        "ROW_NUMBER() OVER (PARTITION BY chunk_index ORDER BY journal_id DESC) AS revision_rank "
        f"FROM {qualified_table} WHERE run_key = {sql_string(run_key)}"
        ") SELECT chunk_index, journal_id, details_json FROM latest_chunk "
        "WHERE revision_rank = 1 ORDER BY chunk_index ASC, journal_id DESC"
    )


def select_chunk_revisions(qualified_table: str, indexes: tuple[int, ...]) -> str:
    """Render an indexed latest-revision query for only the requested chunks."""

    placeholders = ", ".join("?" for _ in indexes)
    return (
        "WITH latest_chunk AS ("
        "SELECT chunk_index, journal_id, details_json, "
        "ROW_NUMBER() OVER (PARTITION BY chunk_index ORDER BY journal_id DESC) AS revision_rank "
        f"FROM {qualified_table} WITH (UPDLOCK, HOLDLOCK) "
        f"WHERE run_key = ? AND chunk_index IN ({placeholders})"
        ") SELECT chunk_index, journal_id, details_json FROM latest_chunk "
        "WHERE revision_rank = 1 ORDER BY chunk_index ASC"
    )


def select_running_chunks(qualified_table: str) -> str:
    """Render the takeover-only latest RUNNING chunk projection."""

    return (
        "WITH latest_chunk AS ("
        "SELECT chunk_index, journal_id, status, details_json, "
        "ROW_NUMBER() OVER (PARTITION BY chunk_index ORDER BY journal_id DESC) AS revision_rank "
        f"FROM {qualified_table} WITH (UPDLOCK, HOLDLOCK) WHERE run_key = ?"
        ") SELECT chunk_index, journal_id, details_json FROM latest_chunk "
        "WHERE revision_rank = 1 AND status = N'running' ORDER BY chunk_index ASC"
    )


__all__ = [
    "campaign_ddl",
    "chunk_ddl",
    "select_campaign",
    "select_chunk_revisions",
    "select_chunks",
    "select_running_chunks",
]
