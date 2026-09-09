"""Typed source-checkpoint capability for generic MSSQL transactions.

The generic MSSQL receipt can make a target mutation and its receipt atomic.
It cannot turn a target-derived single-column ``MAX`` into a complete source
snapshot boundary.  Built-in sources therefore distinguish genuinely
stateless extraction from unsafe or separately-owned checkpoint models.
"""

from __future__ import annotations

from typing import Any

from dpone._compat import StrEnum


class MssqlTransactionCheckpointMode(StrEnum):
    """Checkpoint relationship to the generic target transaction."""

    STATELESS = "stateless"
    SNAPSHOT_ENVELOPE_TARGET_ATOMIC = "snapshot_envelope_target_atomic"
    TARGET_DERIVED_SINGLE_COLUMN_UNSAFE = "target_derived_single_column_unsafe"
    EXTERNAL_NONATOMIC = "external_nonatomic"
    UNKNOWN = "unknown"
    LEGACY_AMBIGUOUS = "target_derived_or_stateless"


def normalize_mssql_transaction_checkpoint_mode(value: Any) -> MssqlTransactionCheckpointMode:
    """Normalize one source declaration without treating unknown values as safe."""

    try:
        return MssqlTransactionCheckpointMode(str(value))
    except ValueError:
        return MssqlTransactionCheckpointMode.UNKNOWN


def require_generic_mssql_checkpoint_safety(value: Any) -> MssqlTransactionCheckpointMode:
    """Return the typed mode only when generic receipt atomicity is sufficient."""

    mode = normalize_mssql_transaction_checkpoint_mode(value)
    if mode is not MssqlTransactionCheckpointMode.STATELESS:
        raise RuntimeError(f"mssql_transaction.source_checkpoint_not_atomic:{mode.value}")
    return mode


def require_snapshot_envelope_mssql_checkpoint_safety(value: Any) -> MssqlTransactionCheckpointMode:
    """Require the source-owned envelope that joins the MSSQL target transaction."""

    mode = normalize_mssql_transaction_checkpoint_mode(value)
    if mode is not MssqlTransactionCheckpointMode.SNAPSHOT_ENVELOPE_TARGET_ATOMIC:
        raise RuntimeError(f"mssql_transaction.snapshot_envelope_checkpoint_required:{mode.value}")
    return mode


__all__ = [
    "MssqlTransactionCheckpointMode",
    "normalize_mssql_transaction_checkpoint_mode",
    "require_generic_mssql_checkpoint_safety",
    "require_snapshot_envelope_mssql_checkpoint_safety",
]
