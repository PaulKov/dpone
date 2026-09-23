"""Narrow facade for canonical SqlClient chunk receipt consumers."""

from dpone.contracts import mssql_tds_api as native
from dpone.contracts.mssql_sqlclient_native_chunk_receipt import (
    SCHEMA,
    SqlClientFailedAttemptSettlement,
    SqlClientFailedEligibility,
    SqlClientInputCustody,
    SqlClientInputCustodyReceipt,
    SqlClientNativeChunkEvidence,
    bind_native_chunk_receipt,
    canonical_stage_id,
    plan_sha256,
    settlement_operation_key,
    sqlclient_physical_stage,
    validate_native_chunk_receipt,
)

__all__ = (
    "SCHEMA",
    "SqlClientFailedAttemptSettlement",
    "SqlClientFailedEligibility",
    "SqlClientInputCustody",
    "SqlClientInputCustodyReceipt",
    "SqlClientNativeChunkEvidence",
    "bind_native_chunk_receipt",
    "canonical_stage_id",
    "native",
    "plan_sha256",
    "settlement_operation_key",
    "sqlclient_physical_stage",
    "validate_native_chunk_receipt",
)
