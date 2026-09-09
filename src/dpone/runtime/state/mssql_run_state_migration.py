"""SQL Server run-state capability detection and identity policy."""

from __future__ import annotations

import re
from collections.abc import Collection

from dpone._compat import StrEnum

RUN_STATE_V2_IDENTITY_COLUMNS = frozenset({"id", "run_state_key", "process_name"})
RUN_STATE_V1_IDENTITY_COLUMNS = frozenset({"dag_id", "execution_date"})
RUN_STATE_V1_COLUMNS = frozenset(
    {
        "id",
        "dag_id",
        "source_schema",
        "source_table",
        "target_schema",
        "target_table",
        "load_strategy",
        "execution_date",
        "state",
        "started_at",
        "ended_at",
        "duration_min",
        "error_message",
        "rows_read",
        "rows_written",
        "rows_updated",
        "rows_deleted",
        "__dpone__loaded_at",
        "__dpone__updated_at",
    }
)


class MssqlRunStateIdentityPolicy(StrEnum):
    """Select which durable identity contract a route is allowed to use."""

    LEGACY_COMPATIBLE = "legacy_compatible"
    PROCESS_SCOPED_V2_REQUIRED = "process_scoped_v2_required"


class MssqlRunStateTableCapability(StrEnum):
    """Detected identity capability of an existing run-state table."""

    LEGACY_V1 = "legacy_v1"
    PROCESS_SCOPED_V2 = "process_scoped_v2"


def resolve_run_state_table_capability(
    columns: Collection[str],
    *,
    identity_policy: MssqlRunStateIdentityPolicy,
    table_label: str,
) -> MssqlRunStateTableCapability:
    """Route a compatible table without weakening process-scoped workloads."""

    normalized = {str(column).lower() for column in columns}
    if RUN_STATE_V2_IDENTITY_COLUMNS.issubset(normalized):
        return MssqlRunStateTableCapability.PROCESS_SCOPED_V2
    if RUN_STATE_V1_COLUMNS.issubset(normalized):
        if identity_policy is MssqlRunStateIdentityPolicy.LEGACY_COMPATIBLE:
            return MssqlRunStateTableCapability.LEGACY_V1
        raise RuntimeError(f"mssql_run_state_v1_to_v2_migration_required:{table_label}")
    if RUN_STATE_V1_IDENTITY_COLUMNS.issubset(normalized) and (
        identity_policy is MssqlRunStateIdentityPolicy.PROCESS_SCOPED_V2_REQUIRED
    ):
        raise RuntimeError(f"mssql_run_state_v1_to_v2_migration_required:{table_label}")
    raise RuntimeError(f"mssql_run_state_contract_invalid:{table_label}")


def require_run_state_v2(columns: Collection[str], *, table_label: str) -> None:
    """Reject a legacy/partial table instead of silently reusing v1 identity."""

    resolve_run_state_table_capability(
        columns,
        identity_policy=MssqlRunStateIdentityPolicy.PROCESS_SCOPED_V2_REQUIRED,
        table_label=table_label,
    )


def render_run_state_v1_create_sql(fq_table: str, *, constraint_token: str) -> str:
    """Render the frozen runtime-managed v1 table for legacy manifests."""

    token = _constraint_token(constraint_token)
    return f"""
CREATE TABLE {fq_table} (
    id bigint IDENTITY(1,1) NOT NULL,
    dag_id nvarchar(512) NOT NULL,
    source_schema nvarchar(256) NOT NULL,
    source_table nvarchar(256) NOT NULL,
    target_schema nvarchar(256) NOT NULL,
    target_table nvarchar(256) NOT NULL,
    load_strategy nvarchar(64) NOT NULL,
    execution_date datetime2 NOT NULL,
    state nvarchar(32) NOT NULL,
    started_at datetime2 NOT NULL,
    ended_at datetime2 NULL,
    duration_min float NULL,
    error_message nvarchar(max) NULL,
    rows_read bigint NULL,
    rows_written bigint NULL,
    rows_updated bigint NULL,
    rows_deleted bigint NULL,
    __dpone__loaded_at datetime2 NOT NULL DEFAULT SYSUTCDATETIME(),
    __dpone__updated_at datetime2 NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT pk_{token}_dag_execution PRIMARY KEY (dag_id, execution_date)
)
""".strip()


def render_run_state_v2_create_sql(fq_table: str, *, constraint_token: str) -> str:
    """Render a versioned v2 table; callers choose a new explicit table name.

    The helper intentionally does not copy v1 rows: historical records do not
    contain ``process_name``, so guessing it would create silent identity
    collisions.  Operators retain v1 read-only and bind ``state.run_table`` to
    the freshly provisioned v2 object.
    """

    token = _constraint_token(constraint_token)
    return f"""
CREATE TABLE {fq_table} (
    id bigint IDENTITY(1,1) NOT NULL,
    run_state_key binary(32) NOT NULL,
    dag_id nvarchar(512) NOT NULL,
    process_name nvarchar(512) NOT NULL,
    source_schema nvarchar(256) NOT NULL,
    source_table nvarchar(256) NOT NULL,
    target_schema nvarchar(256) NOT NULL,
    target_table nvarchar(256) NOT NULL,
    load_strategy nvarchar(64) NOT NULL,
    execution_date datetime2(7) NOT NULL,
    state nvarchar(32) NOT NULL,
    started_at datetime2(7) NOT NULL,
    ended_at datetime2(7) NULL,
    duration_min float NULL,
    error_message nvarchar(max) NULL,
    rows_read bigint NULL,
    rows_written bigint NULL,
    rows_updated bigint NULL,
    rows_deleted bigint NULL,
    __dpone__loaded_at datetime2(7) NOT NULL DEFAULT SYSUTCDATETIME(),
    __dpone__updated_at datetime2(7) NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT pk_{token}_id PRIMARY KEY CLUSTERED (id),
    CONSTRAINT uq_{token}_run_state_key UNIQUE NONCLUSTERED (run_state_key)
)
""".strip()


def _constraint_token(value: str) -> str:
    token = re.sub(r"[^A-Za-z0-9_]", "_", value)[:100]
    if not token:
        raise ValueError("mssql_run_state_constraint_token_required")
    return token


__all__ = [
    "MssqlRunStateIdentityPolicy",
    "MssqlRunStateTableCapability",
    "render_run_state_v1_create_sql",
    "render_run_state_v2_create_sql",
    "require_run_state_v2",
    "resolve_run_state_table_capability",
]
