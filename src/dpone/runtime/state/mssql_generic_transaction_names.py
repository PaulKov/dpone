"""Neutral object-name contract for generic MSSQL transaction governance."""

from __future__ import annotations

GENERIC_TRANSACTION_CATALOG_VERSION = 2
FENCE_TABLE = "dpone_target_fence"
ATTEMPT_TABLE = "dpone_load_attempt"
OPERATION_TABLE = "dpone_load_operation"
RECEIPT_TABLE = "dpone_load_receipt"
FENCE_TRIGGER = "trg_dpone_target_fence_monotonic"
ATTEMPT_TRIGGER = "trg_dpone_load_attempt_immutable"
OPERATION_TRIGGER = "trg_dpone_load_operation_fence"
RECEIPT_TRIGGER = "trg_dpone_load_receipt_immutable"
GENERIC_TRANSACTION_TABLES = (
    FENCE_TABLE,
    ATTEMPT_TABLE,
    OPERATION_TABLE,
    RECEIPT_TABLE,
)
GENERIC_TRANSACTION_TRIGGERS = (
    FENCE_TRIGGER,
    ATTEMPT_TRIGGER,
    OPERATION_TRIGGER,
    RECEIPT_TRIGGER,
)

__all__ = [
    "ATTEMPT_TABLE",
    "ATTEMPT_TRIGGER",
    "FENCE_TABLE",
    "FENCE_TRIGGER",
    "GENERIC_TRANSACTION_TABLES",
    "GENERIC_TRANSACTION_TRIGGERS",
    "GENERIC_TRANSACTION_CATALOG_VERSION",
    "OPERATION_TABLE",
    "OPERATION_TRIGGER",
    "RECEIPT_TABLE",
    "RECEIPT_TRIGGER",
]
