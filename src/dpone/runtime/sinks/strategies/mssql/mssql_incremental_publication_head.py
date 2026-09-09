"""Durable publication-head fence for PostgreSQL XMin incrementals."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from dpone.runtime.postgres_xmin_execution import (
    PostgresXminExecutionMode,
    postgres_xmin_execution_policy,
    xmin_handoff_seed_load_id,
)
from dpone.runtime.sinks.mssql_backfill_publication_catalog import (
    MssqlPublicationTarget,
    publication_target,
    read_shadow_owner,
)
from dpone.runtime.sinks.mssql_backfill_publication_generation import (
    acquire_publication_lock,
    require_xmin_publication_head,
)


@dataclass(frozen=True, slots=True)
class MssqlIncrementalPublicationAuthority:
    """Target and seed identity protected by the held publication lock."""

    target: MssqlPublicationTarget
    seed_load_id: str


class MssqlIncrementalPublicationHeadGuard:
    """Require the exact initial generation before any incremental DML."""

    def __init__(self, *, connector: Any, state_storage: Any) -> None:
        self._connector = connector
        self._state = state_storage

    def acquire(
        self,
        load_config: Any,
        envelope: Any,
        *,
        timeout_ms: int,
    ) -> MssqlIncrementalPublicationAuthority | None:
        """Acquire target-global publication order for an explicit incremental."""

        policy = postgres_xmin_execution_policy(getattr(load_config, "options", None))
        if policy.mode is not PostgresXminExecutionMode.INCREMENTAL:
            return None
        if policy.handoff_id is None:
            raise RuntimeError("postgres_xmin_handoff.handoff_id_required")
        key = envelope.state_key
        target = publication_target(
            database=key.target_database,
            schema=key.target_schema,
            table=key.target_table,
        )
        acquire_publication_lock(
            self._connector,
            target,
            phase="incremental",
            timeout_ms=timeout_ms,
        )
        return MssqlIncrementalPublicationAuthority(
            target=target,
            seed_load_id=xmin_handoff_seed_load_id(
                handoff_id=policy.handoff_id,
                state_key_digest=key.digest,
            ),
        )

    def require_locked(
        self,
        authority: MssqlIncrementalPublicationAuthority | None,
        envelope: Any,
    ) -> None:
        """Verify durable seed authority and, when bound, the exact live head."""

        if authority is None:
            return
        key = envelope.state_key
        receipt = self._state.probe_receipt(
            key=key,
            load_id=authority.seed_load_id,
            executor=self._connector,
        )
        previous = envelope.previous_checkpoint
        exact_seed = (
            receipt is not None
            and receipt.candidate_revision == 1
            and previous is not None
            and previous.revision >= 1
            and previous.xmin_value >= receipt.candidate_xmin
        )
        if not exact_seed:
            raise RuntimeError("postgres_xmin_handoff.not_committed")
        publication_receipt_id = receipt.publication_receipt_id
        if publication_receipt_id is None:
            if read_shadow_owner(self._connector, authority.target) is not None:
                raise RuntimeError("postgres_xmin_handoff.legacy_publication_migration_required")
            return
        require_xmin_publication_head(
            self._connector,
            authority.target,
            publication_receipt_id=publication_receipt_id,
            state_key_sha256=key.digest.hex(),
            seed_load_id=authority.seed_load_id,
            xmin_receipt_id=receipt.receipt_id,
        )


__all__ = [
    "MssqlIncrementalPublicationAuthority",
    "MssqlIncrementalPublicationHeadGuard",
]
