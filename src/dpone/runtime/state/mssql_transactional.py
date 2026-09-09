"""Caller-transaction MSSQL checkpoint and commit-receipt operations."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dpone.ports.source_state_storage import (
    CheckpointCommitOutcome,
    MssqlStateLocation,
    SourceStateKey,
)
from dpone.runtime.state.mssql_checkpoint_cas import (
    checkpoint_cas_params,
    checkpoint_cas_sql,
)
from dpone.runtime.state.mssql_contract import (
    COMMIT_RECEIPT_CONTRACT,
    SOURCE_STATE_CONTRACT,
    require_external_table_shape,
)
from dpone.runtime.state.mssql_repair_contract import (
    REPAIR_AUTHORITY_CONTRACT,
    REPAIR_CONSUMPTION_CONTRACT,
    require_immutable_authority_trigger,
)
from dpone.runtime.state.xmin_storage import XMinState

if TYPE_CHECKING:
    from dpone.contracts.repair_authority import RepairAuthority


def preflight_external_state_tables(
    connector: Any,
    *,
    database: str | None,
    schema: str,
    state_table: str,
    receipt_table: str,
    repair_authority_table: str,
    repair_consumption_table: str,
) -> None:
    """Validate externally provisioned checkpoint and receipt tables without DDL."""

    require_external_table_shape(
        connector,
        database=database,
        schema=schema,
        table=state_table,
        contract=SOURCE_STATE_CONTRACT,
    )
    from dpone.runtime.state.mssql_target_authority import require_active_target_authority_index

    require_active_target_authority_index(
        connector,
        database=database,
        schema=schema,
        table=state_table,
    )
    require_external_table_shape(
        connector,
        database=database,
        schema=schema,
        table=receipt_table,
        contract=COMMIT_RECEIPT_CONTRACT,
    )
    require_external_table_shape(
        connector,
        database=database,
        schema=schema,
        table=repair_authority_table,
        contract=REPAIR_AUTHORITY_CONTRACT,
    )
    require_external_table_shape(
        connector,
        database=database,
        schema=schema,
        table=repair_consumption_table,
        contract=REPAIR_CONSUMPTION_CONTRACT,
    )
    require_immutable_authority_trigger(
        connector,
        database=database,
        schema=schema,
        table=repair_authority_table,
    )


class MssqlTransactionalStateService:
    """Execute checkpoint CAS without owning begin, commit, or rollback."""

    def __init__(self, connector: Any, location: MssqlStateLocation) -> None:
        self._connector = connector
        self.location = location

    def load_state_by_key(self, key: SourceStateKey) -> XMinState | None:
        rows = self._connector.get_records(
            f"""
            SELECT TOP (1)
                xmin_value, state_revision, __dpone__updated_at, is_initial,
                wraparound_detected, frozen_xid
            FROM {self.location.state_table_name}
            WHERE state_key = ? AND target_identity = ? AND superseded_at_utc IS NULL
            """,
            (key.digest, key.target_identity),
            as_dict=True,
        )
        if not rows:
            return None
        row = rows[0]
        return XMinState(
            xmin_value=int(row["xmin_value"]),
            timestamp=row["__dpone__updated_at"],
            is_initial=bool(row["is_initial"]),
            wraparound_detected=bool(row["wraparound_detected"]),
            frozen_xid=row.get("frozen_xid"),
            revision=int(row["state_revision"]),
        )

    def assert_target_authority(self, *, executor: Any, key: SourceStateKey) -> None:
        """Reject a second checkpoint authority for the same physical target."""

        from dpone.runtime.state.mssql_target_authority import MssqlTargetAuthorityService

        MssqlTargetAuthorityService(self.location).assert_current(executor=executor, key=key)

    def assert_or_transfer_target_authority(
        self,
        *,
        executor: Any,
        key: SourceStateKey,
        authority: RepairAuthority | None,
    ) -> None:
        """Assert current ownership or consume the authority's transfer binding."""

        from dpone.runtime.state.mssql_target_authority import MssqlTargetAuthorityService

        MssqlTargetAuthorityService(self.location).assert_or_transfer(
            executor=executor,
            key=key,
            authority=authority,
        )

    def admit_repair_authority(self, **kwargs: Any) -> Any | None:
        """Admit an exceptional run under this caller-owned transaction."""

        from dpone.runtime.state.mssql_repair_authority import MssqlRepairAuthorityService

        return MssqlRepairAuthorityService(self.location).admit(**kwargs)

    def consume_repair_authority(self, **kwargs: Any) -> None:
        """Write unique consumption evidence under this caller-owned transaction."""

        from dpone.runtime.state.mssql_repair_authority import MssqlRepairAuthorityService

        MssqlRepairAuthorityService(self.location).consume(**kwargs)

    def preview_repair_authority(self, **kwargs: Any) -> Any:
        """Read and verify authority without consuming it before source I/O."""

        from dpone.runtime.state.mssql_repair_authority import MssqlRepairAuthorityService

        return MssqlRepairAuthorityService(self.location).preview(**kwargs)

    def compare_and_set_with_receipt(
        self,
        *,
        executor: Any,
        key: SourceStateKey,
        expected: XMinState | None,
        candidate: XMinState,
        load_id: str,
        snapshot_token: str,
        publication_receipt_id: str | None = None,
    ) -> CheckpointCommitOutcome:
        if not load_id or not snapshot_token.startswith("sha256:"):
            raise ValueError("checkpoint receipt requires load_id and snapshot_token")
        if publication_receipt_id is not None and not publication_receipt_id.strip():
            raise ValueError("checkpoint publication receipt is empty")
        if expected is not None and candidate.xmin_value < expected.xmin_value:
            raise ValueError("postgres_xmin_checkpoint_regression")
        rows = executor.get_records(
            self._cas_sql(expected is None),
            self._cas_params(
                key=key,
                expected=expected,
                candidate=candidate,
                load_id=load_id,
                snapshot_token=snapshot_token,
                publication_receipt_id=publication_receipt_id,
            ),
            as_dict=True,
        )
        if not rows:
            raise RuntimeError("DPONE_XMIN_CHECKPOINT_RECEIPT_MISSING")
        row = rows[0]
        candidate_revision = int(row["candidate_revision"])
        expected_revision = (expected.revision if expected is not None else 0) + 1
        if int(row["candidate_xmin"]) != candidate.xmin_value or candidate_revision != expected_revision:
            raise RuntimeError("DPONE_XMIN_CHECKPOINT_RECEIPT_MISMATCH")
        return CheckpointCommitOutcome(
            receipt_id=str(row["receipt_id"]),
            candidate_xmin=int(row["candidate_xmin"]),
            candidate_revision=candidate_revision,
            publication_receipt_id=_optional_text(row.get("publication_receipt_id")),
        )

    def probe_receipt(
        self,
        *,
        key: SourceStateKey,
        load_id: str,
        executor: Any | None = None,
    ) -> CheckpointCommitOutcome | None:
        connector = executor or self._connector
        rows = connector.get_records(
            f"""
            SELECT TOP (1) r.receipt_id, r.candidate_xmin, r.candidate_revision,
                r.publication_receipt_id
            FROM {self.location.receipt_table_name} AS r
            INNER JOIN {self.location.state_table_name} AS s
                ON s.state_key = r.state_key AND s.target_identity = ?
            WHERE r.state_key = ? AND r.load_id = ?
            """,
            (key.target_identity, key.digest, load_id),
            as_dict=True,
        )
        if not rows:
            return None
        return CheckpointCommitOutcome(
            receipt_id=str(rows[0]["receipt_id"]),
            candidate_xmin=int(rows[0]["candidate_xmin"]),
            candidate_revision=int(rows[0]["candidate_revision"]),
            publication_receipt_id=_optional_text(rows[0].get("publication_receipt_id")),
        )

    def bind_seed_publication_receipt(
        self,
        *,
        executor: Any,
        key: SourceStateKey,
        load_id: str,
        receipt_id: str,
        candidate_xmin: int,
        snapshot_token: str,
        publication_receipt_id: str,
    ) -> CheckpointCommitOutcome:
        """Perform the only legal legacy NULL-to-publication seed enrichment."""

        if (
            not load_id
            or not receipt_id
            or candidate_xmin < 1
            or not snapshot_token.startswith("sha256:")
            or not publication_receipt_id.strip()
        ):
            raise ValueError("postgres_xmin_handoff.seed_publication_binding_invalid")
        rows = executor.get_records(
            f"""
            SELECT r.receipt_id, r.candidate_xmin, r.candidate_revision,
                   r.previous_revision, r.source_snapshot_token,
                   r.publication_receipt_id,
                   s.xmin_value, s.state_revision
            FROM {self.location.receipt_table_name} AS r WITH (UPDLOCK, HOLDLOCK)
            INNER JOIN {self.location.state_table_name} AS s WITH (HOLDLOCK)
                ON s.state_key = r.state_key
            WHERE r.state_key = ? AND r.load_id = ?
              AND s.target_identity = ? AND s.superseded_at_utc IS NULL
            """,
            (key.digest, load_id, key.target_identity),
            as_dict=True,
        )
        if len(rows or ()) != 1:
            raise RuntimeError("postgres_xmin_handoff.seed_state_conflict")
        row = rows[0]
        persisted_publication = _optional_text(row.get("publication_receipt_id"))
        exact = (
            str(row.get("receipt_id") or "") == receipt_id
            and row.get("previous_revision") is None
            and int(row.get("candidate_revision") or 0) == 1
            and int(row.get("candidate_xmin") or 0) == candidate_xmin
            and str(row.get("source_snapshot_token") or "") == snapshot_token
            and int(row.get("state_revision") or 0) >= 1
            and int(row.get("xmin_value") or 0) >= int(row.get("candidate_xmin") or 0)
            and persisted_publication in (None, publication_receipt_id)
        )
        if not exact:
            raise RuntimeError("postgres_xmin_handoff.seed_state_conflict")
        if persisted_publication is None:
            executor.execute_query(
                f"""
                UPDATE {self.location.receipt_table_name} WITH (UPDLOCK, HOLDLOCK)
                SET publication_receipt_id = ?
                WHERE state_key = ? AND load_id = ? AND receipt_id = ?
                  AND candidate_revision = 1 AND previous_revision IS NULL
                  AND candidate_xmin = ? AND source_snapshot_token = ?
                  AND publication_receipt_id IS NULL
                """,
                (
                    publication_receipt_id,
                    key.digest,
                    load_id,
                    receipt_id,
                    candidate_xmin,
                    snapshot_token,
                ),
            )
        rebound = self.probe_receipt(key=key, load_id=load_id, executor=executor)
        if (
            rebound is None
            or rebound.receipt_id != receipt_id
            or rebound.publication_receipt_id != publication_receipt_id
        ):
            raise RuntimeError("postgres_xmin_handoff.seed_publication_binding_not_durable")
        return rebound

    def _cas_sql(self, initial: bool) -> str:
        return checkpoint_cas_sql(self.location, initial=initial)

    def _cas_params(
        self,
        *,
        key: SourceStateKey,
        expected: XMinState | None,
        candidate: XMinState,
        load_id: str,
        snapshot_token: str,
        publication_receipt_id: str | None,
    ) -> tuple[Any, ...]:
        return checkpoint_cas_params(
            key=key,
            expected=expected,
            candidate=candidate,
            load_id=load_id,
            snapshot_token=snapshot_token,
            publication_receipt_id=publication_receipt_id,
        )


def _optional_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


__all__ = ["MssqlTransactionalStateService", "preflight_external_state_tables"]
