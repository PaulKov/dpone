"""Atomic SQL Server commit of an initial PostgreSQL XMin handoff."""

from __future__ import annotations

from contextlib import suppress
from typing import TYPE_CHECKING, Any

from dpone.backfill.execution_policy import execution_policy_from_load_config
from dpone.runtime.sinks.mssql_backfill_publication_catalog import publication_target
from dpone.runtime.sinks.mssql_backfill_publication_generation import (
    acquire_publication_lock,
    complete_xmin_publication_head,
    require_recoverable_xmin_publication_head,
    require_xmin_publication_head,
)
from dpone.runtime.sinks.strategies.mssql.mssql_transaction_lock import (
    acquire_target_lock,
    target_lock_resource,
    transaction_lock_timeout_ms,
)
from dpone.runtime.state.mssql_route_preflight import require_atomic_mssql_route

if TYPE_CHECKING:
    from dpone.backfill.xmin_handoff import PostgresXminHandoffProof
    from dpone.ports.source_state_storage import CheckpointCommitOutcome


class MssqlXminHandoffCommitOutcomeUnknown(RuntimeError):
    """The server may have committed, but no exact receipt was observable."""

    code = "postgres_xmin_handoff.commit_outcome_unknown"

    def __init__(self) -> None:
        super().__init__(self.code)


class MssqlXminHandoffCommitter:
    """Seed checkpoint and receipt without owning business-table DML."""

    def __init__(self, *, target_connector: Any, state_storage: Any, load_config: Any) -> None:
        self._target = target_connector
        self._state = state_storage
        self._load_config = load_config

    def probe_seed(self, proof: PostgresXminHandoffProof) -> CheckpointCommitOutcome | None:
        require_atomic_mssql_route(self._target, self._state)
        receipt = self._state.probe_receipt(
            key=proof.state_key,
            load_id=proof.record.seed_load_id,
        )
        state = self._state.load_state_by_key(proof.state_key)
        if receipt is None and state is None:
            self._require_publication_head(proof, xmin_receipt_id=None)
            return None
        if receipt is None or state is None or not _matches_checkpoint(proof, receipt):
            raise RuntimeError("postgres_xmin_handoff.seed_state_conflict")
        if state.xmin_value < proof.candidate.xmin_value or state.revision < receipt.candidate_revision:
            raise RuntimeError("postgres_xmin_handoff.seed_state_conflict")
        if receipt.publication_receipt_id is None and proof.record.publication_receipt_id is not None:
            receipt = self._bind_legacy_publication_receipt(proof, receipt)
        if receipt.publication_receipt_id != proof.record.publication_receipt_id:
            raise RuntimeError("postgres_xmin_handoff.seed_state_conflict")
        self._require_publication_head(proof, xmin_receipt_id=receipt.receipt_id)
        return receipt

    def commit_seed(self, proof: PostgresXminHandoffProof) -> CheckpointCommitOutcome:
        existing = self.probe_seed(proof)
        if existing is not None:
            return existing
        publication_target_value = self._publication_target(proof, required=True)
        transaction_started = False
        commit_attempted = False
        outcome: CheckpointCommitOutcome | None = None
        try:
            self._target.begin()
            transaction_started = True
            self._target.execute_query("SET XACT_ABORT ON")
            self._target.execute_query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
            acquire_target_lock(
                self._target,
                target_lock_resource(proof.state_key.target_identity),
                database=proof.state_key.target_database,
                timeout_ms=transaction_lock_timeout_ms(self._load_config),
            )
            if publication_target_value is not None:
                acquire_publication_lock(self._target, publication_target_value, phase="publish")
                self._require_publication_head(proof, xmin_receipt_id=None)
            self._state.assert_physical_target_identity(executor=self._target, key=proof.state_key)
            self._state.assert_target_authority(executor=self._target, key=proof.state_key)
            outcome = self._state.compare_and_set_with_receipt(
                executor=self._target,
                key=proof.state_key,
                expected=None,
                candidate=proof.candidate,
                load_id=proof.record.seed_load_id,
                snapshot_token=proof.record.snapshot_token,
                publication_receipt_id=proof.record.publication_receipt_id,
            )
            if publication_target_value is not None:
                assert proof.record.publication_receipt_id is not None
                complete_xmin_publication_head(
                    self._target,
                    publication_target_value,
                    publication_receipt_id=proof.record.publication_receipt_id,
                    state_key_sha256=proof.record.state_key_sha256,
                    seed_load_id=proof.record.seed_load_id,
                    xmin_receipt_id=outcome.receipt_id,
                )
            commit_attempted = True
            self._target.commit_transaction()
            transaction_started = False
            return outcome
        except BaseException as exc:
            if commit_attempted and outcome is not None:
                transaction_started = False
                closer = getattr(self._target, "close", None)
                if callable(closer):
                    with suppress(Exception):
                        closer()
                try:
                    probed = self.probe_seed(proof)
                except BaseException:
                    probed = None
                if probed is not None and _matches(proof, probed):
                    return probed
                raise MssqlXminHandoffCommitOutcomeUnknown() from exc
            if transaction_started:
                self._target.rollback()
            raise

    def _publication_target(
        self,
        proof: PostgresXminHandoffProof,
        *,
        required: bool = False,
    ) -> Any | None:
        policy = execution_policy_from_load_config(self._load_config)
        if policy.publication.mode != "shadow_swap":
            return None
        if proof.record.publication_receipt_id is None:
            if required:
                raise RuntimeError("postgres_xmin_handoff.publication_receipt_missing")
            return None
        key = proof.state_key
        return publication_target(
            database=key.target_database,
            schema=key.target_schema,
            table=key.target_table,
        )

    def _require_publication_head(
        self,
        proof: PostgresXminHandoffProof,
        *,
        xmin_receipt_id: str | None,
    ) -> None:
        target = self._publication_target(proof)
        if target is None:
            return
        assert proof.record.publication_receipt_id is not None
        require_xmin_publication_head(
            self._target,
            target,
            publication_receipt_id=proof.record.publication_receipt_id,
            state_key_sha256=proof.record.state_key_sha256,
            seed_load_id=proof.record.seed_load_id,
            xmin_receipt_id=xmin_receipt_id,
        )

    def _bind_legacy_publication_receipt(
        self,
        proof: PostgresXminHandoffProof,
        receipt: CheckpointCommitOutcome,
    ) -> CheckpointCommitOutcome:
        """Migrate the only legal legacy NULL binding under both target fences."""

        target = self._publication_target(proof, required=True)
        assert target is not None
        publication_receipt_id = proof.record.publication_receipt_id
        assert publication_receipt_id is not None
        binder = getattr(self._state, "bind_seed_publication_receipt", None)
        if not callable(binder):
            raise RuntimeError("postgres_xmin_handoff.seed_publication_binding_required")
        transaction_started = False
        commit_attempted = False
        try:
            self._target.begin()
            transaction_started = True
            self._target.execute_query("SET XACT_ABORT ON")
            self._target.execute_query("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
            acquire_target_lock(
                self._target,
                target_lock_resource(proof.state_key.target_identity),
                database=proof.state_key.target_database,
                timeout_ms=transaction_lock_timeout_ms(self._load_config),
            )
            acquire_publication_lock(self._target, target, phase="legacy-seed-bind")
            self._state.assert_physical_target_identity(executor=self._target, key=proof.state_key)
            self._state.assert_target_authority(executor=self._target, key=proof.state_key)
            head = require_recoverable_xmin_publication_head(
                self._target,
                target,
                publication_receipt_id=publication_receipt_id,
                state_key_sha256=proof.record.state_key_sha256,
                seed_load_id=proof.record.seed_load_id,
                xmin_receipt_id=receipt.receipt_id,
            )
            rebound = binder(
                executor=self._target,
                key=proof.state_key,
                load_id=proof.record.seed_load_id,
                receipt_id=receipt.receipt_id,
                candidate_xmin=proof.candidate.xmin_value,
                snapshot_token=proof.record.snapshot_token,
                publication_receipt_id=publication_receipt_id,
            )
            if not _matches(proof, rebound):
                raise RuntimeError("postgres_xmin_handoff.seed_state_conflict")
            if head.xmin_receipt_id is None:
                complete_xmin_publication_head(
                    self._target,
                    target,
                    publication_receipt_id=publication_receipt_id,
                    state_key_sha256=proof.record.state_key_sha256,
                    seed_load_id=proof.record.seed_load_id,
                    xmin_receipt_id=receipt.receipt_id,
                )
            commit_attempted = True
            self._target.commit_transaction()
            transaction_started = False
            return rebound
        except BaseException as exc:
            if commit_attempted:
                transaction_started = False
                closer = getattr(self._target, "close", None)
                if callable(closer):
                    with suppress(Exception):
                        closer()
                try:
                    recovered = self.probe_seed(proof)
                except BaseException:
                    recovered = None
                if recovered is not None and _matches(proof, recovered):
                    return recovered
                raise MssqlXminHandoffCommitOutcomeUnknown() from exc
            if transaction_started:
                self._target.rollback()
            raise


def _matches(proof: PostgresXminHandoffProof, outcome: CheckpointCommitOutcome) -> bool:
    return _matches_checkpoint(proof, outcome) and outcome.publication_receipt_id == proof.record.publication_receipt_id


def _matches_checkpoint(proof: PostgresXminHandoffProof, outcome: CheckpointCommitOutcome) -> bool:
    return outcome.candidate_xmin == proof.candidate.xmin_value and outcome.candidate_revision == 1


__all__ = ["MssqlXminHandoffCommitOutcomeUnknown", "MssqlXminHandoffCommitter"]
