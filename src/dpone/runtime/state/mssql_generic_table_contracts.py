"""Exact column contracts for generic SQL Server transaction governance."""

from __future__ import annotations

from dpone.runtime.state.mssql_contract import (
    MssqlColumnShape,
    exact_external_table_contract,
)

FENCE_CONTRACT = exact_external_table_contract(
    columns=frozenset(
        {
            "target_identity",
            "current_generation",
            "current_attempt_key",
            "current_route_fingerprint",
            "updated_at_utc",
        }
    ),
    unique_indexes=(("target_identity",),),
    shapes=(
        MssqlColumnShape("target_identity", "binary", 32, None, None, False),
        MssqlColumnShape("current_generation", "bigint", 8, 19, 0, False),
        MssqlColumnShape("current_attempt_key", "binary", 32, None, None, False),
        MssqlColumnShape("current_route_fingerprint", "binary", 32, None, None, False),
        MssqlColumnShape("updated_at_utc", "datetime2", 8, None, 7, False),
    ),
)

ATTEMPT_CONTRACT = exact_external_table_contract(
    columns=frozenset(
        {
            "attempt_key",
            "target_identity",
            "generation",
            "invocation_digest",
            "route_fingerprint",
            "first_load_id",
            "target_database",
            "target_schema",
            "target_table",
            "strategy",
            "created_at_utc",
        }
    ),
    unique_indexes=(
        ("attempt_key",),
        ("invocation_digest",),
        ("target_identity", "generation"),
        ("attempt_key", "target_identity", "generation", "route_fingerprint"),
    ),
    shapes=(
        MssqlColumnShape("attempt_key", "binary", 32, None, None, False),
        MssqlColumnShape("target_identity", "binary", 32, None, None, False),
        MssqlColumnShape("generation", "bigint", 8, 19, 0, False),
        MssqlColumnShape("invocation_digest", "binary", 32, None, None, False),
        MssqlColumnShape("route_fingerprint", "binary", 32, None, None, False),
        MssqlColumnShape("first_load_id", "nvarchar", 256, None, None, False),
        MssqlColumnShape("target_database", "nvarchar", 512, None, None, False),
        MssqlColumnShape("target_schema", "nvarchar", 512, None, None, False),
        MssqlColumnShape("target_table", "nvarchar", 512, None, None, False),
        MssqlColumnShape("strategy", "nvarchar", 128, None, None, False),
        MssqlColumnShape("created_at_utc", "datetime2", 8, None, 7, False),
    ),
)

OPERATION_CONTRACT = exact_external_table_contract(
    columns=frozenset(
        {
            "operation_key",
            "attempt_key",
            "scope_hash",
            "current_epoch",
            "current_owner_digest",
            "lease_expires_at_utc",
            "updated_at_utc",
        }
    ),
    unique_indexes=(
        ("operation_key",),
        ("attempt_key", "scope_hash"),
        ("operation_key", "attempt_key"),
        ("operation_key", "attempt_key", "scope_hash", "current_epoch", "current_owner_digest"),
    ),
    shapes=(
        MssqlColumnShape("operation_key", "binary", 32, None, None, False),
        MssqlColumnShape("attempt_key", "binary", 32, None, None, False),
        MssqlColumnShape("scope_hash", "binary", 32, None, None, False),
        MssqlColumnShape("current_epoch", "bigint", 8, 19, 0, False),
        MssqlColumnShape("current_owner_digest", "binary", 32, None, None, False),
        MssqlColumnShape("lease_expires_at_utc", "datetime2", 8, None, 7, True),
        MssqlColumnShape("updated_at_utc", "datetime2", 8, None, 7, False),
    ),
)

REQUIRED_METRICS = ("inserted_rows", "updated_rows", "total_rows")
OPTIONAL_METRICS = (
    "staging_rows",
    "replaced_rows",
    "soft_deleted_rows",
    "reactivated_rows",
    "unchanged_rows",
    "hard_deleted_rows",
    "active_rows",
)
RECEIPT_CONTRACT = exact_external_table_contract(
    columns=frozenset(
        {
            "receipt_id",
            "operation_key",
            "attempt_key",
            "scope_hash",
            "operation_epoch",
            "owner_digest",
            "load_id",
            "payload_manifest_sha256",
            "declared_rows",
            "actual_raw_rows",
            "actual_native_rows",
            "native_contract_sha256",
            "mutation_plan_sha256",
            "target_before_sha256",
            "target_after_sha256",
            "extraction_started_at_utc",
            "extraction_completed_at_utc",
            "extraction_clock_authority",
            "snapshot_acquired_at_utc",
            "snapshot_authority",
            "source_token_sha256",
            "loaded_at_utc",
            *REQUIRED_METRICS,
            *OPTIONAL_METRICS,
            "committed_at_utc",
        }
    ),
    unique_indexes=(("receipt_id",), ("operation_key",)),
    shapes=(
        MssqlColumnShape("receipt_id", "nvarchar", 256, None, None, False),
        *(
            MssqlColumnShape(column, "binary", 32, None, None, False)
            for column in (
                "operation_key",
                "attempt_key",
                "scope_hash",
                "owner_digest",
                "payload_manifest_sha256",
                "native_contract_sha256",
                "mutation_plan_sha256",
                "target_before_sha256",
                "target_after_sha256",
            )
        ),
        MssqlColumnShape("source_token_sha256", "binary", 32, None, None, True),
        MssqlColumnShape("operation_epoch", "bigint", 8, 19, 0, False),
        MssqlColumnShape("load_id", "nvarchar", 256, None, None, False),
        MssqlColumnShape("extraction_clock_authority", "nvarchar", 256, None, None, False),
        MssqlColumnShape("snapshot_authority", "nvarchar", 256, None, None, True),
        *(
            MssqlColumnShape(column, "bigint", 8, 19, 0, False)
            for column in ("declared_rows", "actual_raw_rows", "actual_native_rows")
        ),
        *(
            MssqlColumnShape(column, "datetime2", 8, None, 7, False)
            for column in (
                "extraction_started_at_utc",
                "extraction_completed_at_utc",
                "loaded_at_utc",
            )
        ),
        MssqlColumnShape("snapshot_acquired_at_utc", "datetime2", 8, None, 7, True),
        *(MssqlColumnShape(column, "bigint", 8, 19, 0, False) for column in REQUIRED_METRICS),
        *(MssqlColumnShape(column, "bigint", 8, 19, 0, True) for column in OPTIONAL_METRICS),
        MssqlColumnShape("committed_at_utc", "datetime2", 8, None, 7, False),
    ),
)


__all__ = [
    "ATTEMPT_CONTRACT",
    "FENCE_CONTRACT",
    "OPERATION_CONTRACT",
    "OPTIONAL_METRICS",
    "RECEIPT_CONTRACT",
    "REQUIRED_METRICS",
]
