"""FAILED_PRE_COMMIT cleanup acknowledgement DDL."""

from __future__ import annotations


def render_cleanup_ack_schema(control_schema: str) -> str:
    """Render the create-only failed cleanup acknowledgement table."""

    table = f"[{control_schema}].[semantic_refresh_failed_cleanup_acks]"
    return f"""
IF OBJECT_ID(N'{table}', N'U') IS NULL
CREATE TABLE {table} (
    workflow_execution_binding_sha256 varchar(71) NOT NULL,
    operation_id varchar(71) NOT NULL,
    workflow_execution_id nvarchar(512) NOT NULL,
    operation_plan_sha256 varchar(71) NOT NULL,
    attempt_binding_sha256 varchar(71) NOT NULL,
    fencing_epoch bigint NOT NULL,
    target_uuid uniqueidentifier NOT NULL,
    scratch_absence_evidence_sha256 varchar(71) NOT NULL,
    scratch_receipt_json nvarchar(max) NOT NULL,
    resource_allocation_closure_sha256 varchar(71) NOT NULL,
    resource_closure_json nvarchar(max) NOT NULL,
    cleanup_receipt_sha256 varchar(71) NOT NULL UNIQUE,
    status nvarchar(16) NOT NULL
        CONSTRAINT [ck_{control_schema}_sr_failed_cleanup_ack_status]
        CHECK (status = N'COMPLETE'),
    created_at_utc datetime2(6) NOT NULL
        CONSTRAINT [df_{control_schema}_sr_failed_cleanup_ack_created]
        DEFAULT SYSUTCDATETIME(),
    CONSTRAINT [pk_{control_schema}_sr_failed_cleanup_ack]
        PRIMARY KEY (workflow_execution_binding_sha256, operation_id)
);
""".strip()


__all__ = ["render_cleanup_ack_schema"]
