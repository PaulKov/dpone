"""Campaign lifecycle that bridges resumable backfill to XMin state."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from dpone.backfill.state import CHUNK_STATUS_SUCCESS, BackfillLedger, BackfillStateStore
from dpone.backfill.xmin_handoff_models import BackfillXminHandoffRecord
from dpone.config.postgres_xmin_execution import require_postgres_xmin_execution_route
from dpone.contracts.postgres_xmin_execution import PostgresXminExecutionMode
from dpone.ports.source_state_storage import CheckpointCommitOutcome, SourceStateKey
from dpone.runtime.state.xmin_storage import XMinState


@dataclass(frozen=True, slots=True)
class PostgresXminHandoffProof:
    """Runtime-only verified checkpoint seed request."""

    record: BackfillXminHandoffRecord
    state_key: SourceStateKey
    candidate: XMinState

    def __post_init__(self) -> None:
        if self.record.state_key_sha256 != self.state_key.digest.hex():
            raise ValueError("postgres_xmin_handoff.state_key_mismatch")
        if self.record.anchor_xmin != self.candidate.xmin_value or self.candidate.is_initial:
            raise ValueError("postgres_xmin_handoff.candidate_mismatch")


class PostgresXminHandoffSourcePort(Protocol):
    def capture_anchor(
        self,
        load_config: Any,
        *,
        handoff_id: str,
        plan_hash: str,
    ) -> PostgresXminHandoffProof: ...

    def revalidate_anchor(
        self,
        load_config: Any,
        record: BackfillXminHandoffRecord,
    ) -> PostgresXminHandoffProof: ...


class PostgresXminHandoffCommitPort(Protocol):
    def probe_seed(self, proof: PostgresXminHandoffProof) -> CheckpointCommitOutcome | None: ...

    def commit_seed(self, proof: PostgresXminHandoffProof) -> CheckpointCommitOutcome: ...


class PostgresXminInitialHandoffLifecycle:
    """Order one durable anchor before chunks and one atomic seed after them."""

    def __init__(
        self,
        *,
        source: PostgresXminHandoffSourcePort,
        committer: PostgresXminHandoffCommitPort,
    ) -> None:
        self._source = source
        self._committer = committer

    def require_unmapped(self, *, selection: Any) -> None:
        if selection is not None:
            raise RuntimeError("postgres_xmin_handoff.airflow_mapping_unsupported")

    @staticmethod
    def campaign_contract(load_config: Any) -> dict[str, Any]:
        """Bind the handoff identity into plan/config hashes before ledger load."""

        policy = require_postgres_xmin_execution_route(load_config)
        return {"postgres_xmin_execution": policy.to_contract()}

    def before_chunks(
        self,
        load_config: Any,
        ledger: BackfillLedger,
        store: BackfillStateStore,
    ) -> BackfillLedger:
        policy = require_postgres_xmin_execution_route(load_config)
        if policy.mode is not PostgresXminExecutionMode.INITIAL or policy.handoff_id is None:
            raise RuntimeError("postgres_xmin_handoff.initial_contract_invalid")
        if not ledger.plan_hash:
            raise RuntimeError("postgres_xmin_handoff.plan_hash_missing")
        record = ledger.xmin_handoff
        if record is None:
            proof = self._source.capture_anchor(
                load_config,
                handoff_id=policy.handoff_id,
                plan_hash=ledger.plan_hash,
            )
            record = proof.record
            if record.handoff_id != policy.handoff_id or record.plan_hash != ledger.plan_hash:
                raise RuntimeError("postgres_xmin_handoff.identity_changed")
            ledger.xmin_handoff = record
            outcome = self._committer.probe_seed(proof)
            if outcome is not None:
                raise RuntimeError("postgres_xmin_handoff.seed_exists_before_campaign")
            store.save(ledger)
            return ledger
        if record.handoff_id != policy.handoff_id or record.plan_hash != ledger.plan_hash:
            raise RuntimeError("postgres_xmin_handoff.identity_changed")
        proof = self._source.revalidate_anchor(load_config, record)
        outcome = self._committer.probe_seed(proof)
        if outcome is not None:
            self._apply_outcome(record, outcome)
            store.save(ledger)
        elif record.status == "committed":
            raise RuntimeError("postgres_xmin_handoff.committed_receipt_missing")
        return ledger

    def after_chunks(
        self,
        load_config: Any,
        ledger: BackfillLedger,
        store: BackfillStateStore,
    ) -> dict[str, Any]:
        if len(ledger.committed_indexes()) != len(ledger.chunks) or any(
            chunk.status != CHUNK_STATUS_SUCCESS for chunk in ledger.chunks
        ):
            raise RuntimeError("postgres_xmin_handoff.chunks_incomplete")
        record = ledger.xmin_handoff
        if record is None:
            raise RuntimeError("postgres_xmin_handoff.anchor_missing")
        self._bind_publication(record, ledger)
        proof = self._source.revalidate_anchor(load_config, record)
        outcome = self._committer.probe_seed(proof)
        if outcome is None:
            record.status = "committing"
            record.receipt_id = None
            record.candidate_revision = None
            store.save(ledger)
            outcome = self._committer.commit_seed(proof)
        self._apply_outcome(record, outcome)
        store.save(ledger)
        return self._evidence(ledger, outcome)

    @staticmethod
    def _bind_publication(record: BackfillXminHandoffRecord, ledger: BackfillLedger) -> None:
        publication = ledger.publication
        if publication is None:
            return
        if publication.phase != "published" or not publication.receipt_id:
            raise RuntimeError("postgres_xmin_handoff.publication_receipt_missing")
        if record.publication_receipt_id not in (None, publication.receipt_id):
            raise RuntimeError("postgres_xmin_handoff.publication_authority_changed")
        record.publication_receipt_id = publication.receipt_id

    @staticmethod
    def _apply_outcome(record: BackfillXminHandoffRecord, outcome: CheckpointCommitOutcome) -> None:
        if outcome.candidate_xmin != record.anchor_xmin or outcome.candidate_revision != 1:
            raise RuntimeError("postgres_xmin_handoff.receipt_mismatch")
        record.status = "committed"
        record.receipt_id = outcome.receipt_id
        record.candidate_revision = outcome.candidate_revision

    @staticmethod
    def _evidence(ledger: BackfillLedger, outcome: CheckpointCommitOutcome) -> dict[str, Any]:
        record = ledger.xmin_handoff
        assert record is not None
        return {
            "status": "committed",
            "handoff_id": record.handoff_id,
            "anchor_xmin": record.anchor_xmin,
            "state_key_sha256": record.state_key_sha256,
            "chunks_committed": len(ledger.committed_indexes()),
            "receipt_id": outcome.receipt_id,
            "candidate_revision": outcome.candidate_revision,
        }


__all__ = [
    "BackfillXminHandoffRecord",
    "PostgresXminHandoffCommitPort",
    "PostgresXminHandoffProof",
    "PostgresXminHandoffSourcePort",
    "PostgresXminInitialHandoffLifecycle",
]
