"""Pure SQL and parameter construction for MSSQL checkpoint CAS writes."""

from __future__ import annotations

from typing import Any

from dpone.ports.source_state_storage import MssqlStateLocation, SourceStateKey
from dpone.runtime.state.xmin_storage import XMinState


def checkpoint_cas_sql(location: MssqlStateLocation, *, initial: bool) -> str:
    """Build the atomic checkpoint-and-receipt statement for one state location."""

    state = location.state_table_name
    receipt = location.receipt_table_name
    if initial:
        mutation = f"""
            INSERT INTO {state} (
                state_key, contract_version, environment, process_name,
                source_connection, source_database, source_schema, source_table,
                target_database, target_schema, target_table, target_identity, unique_key_json,
                schema_hash, scope_hash, xmin_value, state_revision, is_initial,
                wraparound_detected, frozen_xid, source_snapshot_token,
                last_load_id, __dpone__loaded_at, __dpone__updated_at
            )
            SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                   SYSUTCDATETIME(), SYSUTCDATETIME()
            WHERE NOT EXISTS (SELECT 1 FROM {state} WITH (UPDLOCK, HOLDLOCK) WHERE state_key = ?);
            SET @cas_rows = @@ROWCOUNT;
        """
    else:
        mutation = f"""
            UPDATE {state} WITH (UPDLOCK, HOLDLOCK)
            SET xmin_value = ?, state_revision = ?, is_initial = ?, wraparound_detected = ?,
                frozen_xid = ?, source_snapshot_token = ?, last_load_id = ?,
                __dpone__updated_at = SYSUTCDATETIME()
            WHERE state_key = ? AND target_identity = ?
              AND xmin_value = ? AND state_revision = ? AND ? >= xmin_value
              AND superseded_at_utc IS NULL;
            SET @cas_rows = @@ROWCOUNT;
        """
    return f"""
        SET NOCOUNT ON;
        DECLARE @cas_rows int = 0;
        {mutation}
        IF @cas_rows <> 1
            THROW 51000, 'DPONE_XMIN_CHECKPOINT_CAS_MISMATCH', 1;

        INSERT INTO {receipt} (
            receipt_id, state_key, load_id, previous_xmin, candidate_xmin,
            previous_revision, candidate_revision, source_snapshot_token,
            publication_receipt_id, committed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME());

        SELECT r.receipt_id, r.candidate_xmin, r.candidate_revision,
            r.publication_receipt_id
        FROM {receipt} AS r
        INNER JOIN {state} AS s
            ON s.state_key = r.state_key AND s.target_identity = ?
        WHERE r.state_key = ? AND r.load_id = ?;
    """


def checkpoint_cas_params(
    *,
    key: SourceStateKey,
    expected: XMinState | None,
    candidate: XMinState,
    load_id: str,
    snapshot_token: str,
    publication_receipt_id: str | None,
) -> tuple[Any, ...]:
    """Bind checkpoint mutation and receipt values in statement placeholder order."""

    next_revision = (expected.revision if expected is not None else 0) + 1
    if expected is None:
        mutation_params: tuple[Any, ...] = (
            key.digest,
            key.contract_version,
            key.environment,
            key.process,
            key.source_connection,
            key.source_database,
            key.source_schema,
            key.source_table,
            key.target_database,
            key.target_schema,
            key.target_table,
            key.target_identity,
            key.unique_key_json,
            key.schema_hash,
            key.scope_hash,
            candidate.xmin_value,
            next_revision,
            int(candidate.is_initial),
            int(candidate.wraparound_detected),
            candidate.frozen_xid,
            snapshot_token,
            load_id,
            key.digest,
        )
    else:
        mutation_params = (
            candidate.xmin_value,
            next_revision,
            int(candidate.is_initial),
            int(candidate.wraparound_detected),
            candidate.frozen_xid,
            snapshot_token,
            load_id,
            key.digest,
            key.target_identity,
            expected.xmin_value,
            expected.revision,
            candidate.xmin_value,
        )
    receipt_params = (
        load_id,
        key.digest,
        load_id,
        expected.xmin_value if expected is not None else None,
        candidate.xmin_value,
        expected.revision if expected is not None else None,
        next_revision,
        snapshot_token,
        publication_receipt_id,
        key.target_identity,
        key.digest,
        load_id,
    )
    return (*mutation_params, *receipt_params)


__all__ = ["checkpoint_cas_params", "checkpoint_cas_sql"]
