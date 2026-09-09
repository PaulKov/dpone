"""Frozen target authority and ambiguous-commit outcome for MSSQL snapshots."""

from __future__ import annotations

from copy import copy
from dataclasses import is_dataclass, replace
from typing import Any, cast

from dpone.contracts.mssql_transaction_governance import MssqlCleanupDisposition
from dpone.runtime.extraction_lifecycle import ArtifactTerminalOutcome
from dpone.runtime.incremental_snapshot import IncrementalSnapshotEnvelope
from dpone.runtime.sinks.strategies.mssql.mssql_snapshot_reconciliation import SnapshotReconciliationError


class MssqlCommitOutcomeUnknown(RuntimeError):
    code = "mssql_snapshot_reconciliation.commit_outcome_unknown"
    cleanup_disposition = MssqlCleanupDisposition.PRESERVE_STAGING_EVIDENCE
    artifact_terminal_outcome = ArtifactTerminalOutcome.RETAIN_COMMIT_UNKNOWN

    def __init__(self) -> None:
        super().__init__(self.code)


def frozen_target_config(load_config: Any, envelope: IncrementalSnapshotEnvelope[Any, Any]) -> Any:
    """Snapshot canonical target coordinates carried by the source envelope."""

    require_frozen_target_coordinates(load_config, envelope)
    coordinates = {
        "target_database": envelope.state_key.target_database,
        "target_schema": envelope.state_key.target_schema,
        "target_table": envelope.state_key.target_table,
    }
    if is_dataclass(load_config):
        return replace(cast(Any, load_config), **coordinates)
    frozen = copy(load_config)
    for field, value in coordinates.items():
        setattr(frozen, field, value)
    return frozen


def require_frozen_target_coordinates(
    load_config: Any,
    envelope: IncrementalSnapshotEnvelope[Any, Any],
) -> None:
    """Reject a mutable route that no longer matches its extracted envelope."""

    actual = tuple(getattr(load_config, field, None) for field in ("target_database", "target_schema", "target_table"))
    expected = tuple(
        getattr(envelope.state_key, field, None) for field in ("target_database", "target_schema", "target_table")
    )
    if actual != expected or any(not isinstance(value, str) or not value for value in expected):
        raise SnapshotReconciliationError("mssql_snapshot_reconciliation.target_coordinates_changed")
