"""PostgreSQL-side authority for one initial-to-XMin handoff."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from dpone.backfill.xmin_handoff import PostgresXminHandoffProof
from dpone.backfill.xmin_handoff_models import BackfillXminHandoffRecord
from dpone.runtime.postgres_xmin_execution import xmin_handoff_seed_load_id
from dpone.runtime.sources.strategies.postgres.postgres_snapshot_failure import (
    rollback_preserving_primary,
)
from dpone.runtime.sources.strategies.postgres.postgres_xmin_freeze_horizon import (
    require_checkpoint_above_freeze_horizon,
)
from dpone.runtime.state.xmin_storage import XMinState


class PostgresXminHandoffSource:
    """Capture and revalidate a signed XMin anchor on branded RR sessions."""

    def __init__(self, strategy: Any) -> None:
        self._strategy = strategy

    def capture_anchor(
        self,
        load_config: Any,
        *,
        handoff_id: str,
        plan_hash: str,
    ) -> PostgresXminHandoffProof:
        proof = self._read_proof(load_config)
        authority_sha256 = _authority_digest(proof.source_identity)
        record = BackfillXminHandoffRecord(
            handoff_id=handoff_id,
            status="anchored",
            anchor_xmin=proof.anchor_xmin,
            snapshot_token=proof.snapshot_token,
            state_key_sha256=proof.state_key.digest.hex(),
            source_authority_sha256=authority_sha256,
            plan_hash=plan_hash,
            seed_load_id=xmin_handoff_seed_load_id(
                handoff_id=handoff_id,
                state_key_digest=proof.state_key.digest,
            ),
        )
        return PostgresXminHandoffProof(
            record=record,
            state_key=proof.state_key,
            candidate=_candidate(proof.anchor_xmin),
        )

    def revalidate_anchor(
        self,
        load_config: Any,
        record: BackfillXminHandoffRecord,
    ) -> PostgresXminHandoffProof:
        proof = self._read_proof(load_config, checkpoint_xmin=record.anchor_xmin)
        exact = (
            proof.state_key.digest.hex() == record.state_key_sha256
            and _authority_digest(proof.source_identity) == record.source_authority_sha256
        )
        if not exact:
            raise RuntimeError("postgres_xmin_handoff.source_identity_changed")
        return PostgresXminHandoffProof(
            record=record,
            state_key=proof.state_key,
            candidate=_candidate(record.anchor_xmin),
        )

    def _read_proof(self, load_config: Any, *, checkpoint_xmin: int | None = None) -> _SnapshotProof:
        strategy = self._strategy
        strategy._preflight_atomic_route(load_config)
        lifecycle = strategy._new_extraction_lifecycle()
        lease = strategy._begin_repeatable_read_snapshot(lifecycle)
        try:
            source_identity = strategy._verify_postgres_source_authority(lease, load_config)
            projection = strategy.fetch_schema_projection(load_config)
            state_key = strategy._snapshot_extractor.state_key(
                load_config,
                list(projection.projected_schema),
                relation_schema=list(projection.relation_schema),
                relation_metadata=projection.relation_metadata,
            )
            anchor_xmin = strategy.xmin_manager.get_snapshot_xmin_anchor()
            if checkpoint_xmin is not None:
                require_checkpoint_above_freeze_horizon(
                    strategy.connector,
                    load_config,
                    checkpoint_xmin=checkpoint_xmin,
                )
            lifecycle.complete()
            strategy.connector.commit_transaction()
        except BaseException as primary:
            rollback_preserving_primary(strategy.connector, primary)
            raise
        return _SnapshotProof(
            anchor_xmin=anchor_xmin,
            snapshot_token=lease.snapshot_token_digest,
            state_key=state_key,
            source_identity=source_identity,
        )


class _SnapshotProof:
    __slots__ = ("anchor_xmin", "snapshot_token", "source_identity", "state_key")

    def __init__(self, *, anchor_xmin: int, snapshot_token: str, state_key: Any, source_identity: Any) -> None:
        self.anchor_xmin = anchor_xmin
        self.snapshot_token = snapshot_token
        self.state_key = state_key
        self.source_identity = source_identity


def _authority_digest(identity: Any) -> str:
    value = str(getattr(identity, "authority_sha256", "") or "")
    if not value.startswith("sha256:") or len(value) != 71:
        raise RuntimeError("postgres_xmin_handoff.source_authority_digest_required")
    return value.removeprefix("sha256:")


def _candidate(anchor_xmin: int) -> XMinState:
    return XMinState(
        xmin_value=anchor_xmin,
        timestamp=datetime.now(UTC),
        is_initial=False,
        wraparound_detected=False,
        revision=1,
    )


__all__ = ["PostgresXminHandoffSource"]
