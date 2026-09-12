"""Runtime boundary for the public incremental-snapshot contracts.

Runtime components import the shared immutable values through this module so
the contracts package remains one explicit dependency boundary instead of a
web of repeated cross-layer imports.
"""

from dpone.contracts.incremental_snapshot import (
    DELTA_HASH_COLUMN,
    KEY_HASH_COLUMN,
    MSSQL_TEXT_KEY_COLLATION,
    DeltaSnapshotReceipt,
    IncrementalSnapshotEnvelope,
    KeySnapshotReceipt,
    KeySnapshotReconciliationPolicy,
    schema_identity_hash,
    snapshot_scope_hash,
    snapshot_token_digest,
)


class SnapshotReconciliationError(RuntimeError):
    """Fail-closed snapshot reconciliation error with a stable diagnostic code."""

    def __init__(self, code: str, *, full_repair_required: bool = False) -> None:
        super().__init__(code)
        self.code = code
        self.full_repair_required = full_repair_required


__all__ = [
    "DELTA_HASH_COLUMN",
    "DeltaSnapshotReceipt",
    "IncrementalSnapshotEnvelope",
    "KEY_HASH_COLUMN",
    "MSSQL_TEXT_KEY_COLLATION",
    "KeySnapshotReceipt",
    "KeySnapshotReconciliationPolicy",
    "SnapshotReconciliationError",
    "schema_identity_hash",
    "snapshot_scope_hash",
    "snapshot_token_digest",
]
