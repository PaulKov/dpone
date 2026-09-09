"""DDL renderers for SQL Server operational state tables."""

from __future__ import annotations


def render_load_audit_create_sql(*, fq_table: str, object_id: str, constraint_token: str) -> str:
    """Render the canonical load-audit table create statement."""

    return f"""
        IF OBJECT_ID(N'{object_id}', N'U') IS NULL
        CREATE TABLE {fq_table} (
            run_id char(26) NOT NULL,
            load_id char(26) NOT NULL,
            status nvarchar(32) NOT NULL,
            process_name nvarchar(512) NULL,
            source_schema nvarchar(256) NOT NULL,
            source_table nvarchar(256) NOT NULL,
            target_schema nvarchar(256) NOT NULL,
            target_table nvarchar(256) NOT NULL,
            strategy nvarchar(64) NOT NULL,
            started_at datetime2 NOT NULL,
            staged_at datetime2 NULL,
            committed_at datetime2 NULL,
            failed_at datetime2 NULL,
            extracted_rows bigint NULL,
            staged_rows bigint NULL,
            inserted_rows bigint NULL,
            updated_rows bigint NULL,
            loaded_rows bigint NULL,
            deleted_rows bigint NULL,
            reactivated_rows bigint NULL,
            unchanged_rows bigint NULL,
            soft_deleted_rows bigint NULL,
            hard_deleted_rows bigint NULL,
            active_rows bigint NULL,
            total_rows bigint NULL,
            commit_receipt_id nvarchar(128) NULL,
            commit_outcome nvarchar(64) NULL,
            error_message nvarchar(max) NULL,
            artifact_uri nvarchar(max) NULL,
            __dpone__loaded_at datetime2 NOT NULL DEFAULT SYSUTCDATETIME(),
            CONSTRAINT pk_{constraint_token}_load_id PRIMARY KEY (load_id)
        )
        """


__all__ = ["render_load_audit_create_sql"]
