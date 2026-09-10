"""At-most-one EXCHANGE policy with explicit, protected UUID reconciliation.

The composition root supplies real authority/catalog/store/executor adapters.
This service never issues principals, creates tables, deletes retained data,
commits checkpoints or releases parent fences. A caller cannot make it safe by
constructing a record or providing terminal hashes instead of those adapters.
"""

from __future__ import annotations

from dpone.contracts.composition_activation import CompositionAdmissionError, require_digest
from dpone.contracts.composition_attempt import CompositionAttemptIdentity, require_composition_attempt_scope
from dpone.contracts.composition_persistence import encode_attempt_identity
from dpone.contracts.composition_snapshot import (
    SnapshotCatalogObservation,
    SnapshotPublicationIntent,
    SnapshotPublicationRecord,
    SnapshotPublisherClosure,
    classify_snapshot,
)
from dpone.ports.composition_snapshot import (
    ClickHouseSnapshotCatalog,
    ClickHouseSnapshotExecutor,
    SnapshotPublicationAuthority,
    SnapshotPublicationStore,
)


class ClickHouseAtomicSnapshotPublisher:
    """Publish a complete sealed generation, preserving vanished/empty snapshots.

    A concurrent losing publish does not close the winner's principal. Operators
    explicitly call reconcile to close an existing publisher and resolve its
    retained intent. No recovery path can dispatch or reopen writer authority.
    """

    def __init__(
        self,
        *,
        authority: SnapshotPublicationAuthority,
        store: SnapshotPublicationStore,
        catalog: ClickHouseSnapshotCatalog,
        executor: ClickHouseSnapshotExecutor,
    ) -> None:
        self._authority, self._store, self._catalog, self._executor = authority, store, catalog, executor

    def prepare(self, attempt: CompositionAttemptIdentity, generation_ref: str) -> SnapshotPublicationRecord:
        """Reopen verified producer evidence before persisting exact PREPARED bytes."""
        try:
            require_digest(generation_ref)
            value = self._authority.load_prepared(attempt, generation_ref)
            if type(value) is not SnapshotPublicationIntent:
                raise CompositionAdmissionError("snapshot_prepared_shape")
            value.__post_init__()
            if (
                encode_attempt_identity(value.attempt) != encode_attempt_identity(attempt)
                or value.generation.record_sha256 != generation_ref
            ):
                raise CompositionAdmissionError("snapshot_prepared_subject")
            self._require_current(value, recovery=False)
            self._require_unpublished(value)
            expected = SnapshotPublicationRecord(value)
            return self._confirm(self._store.prepare(value), expected)
        except CompositionAdmissionError:
            raise
        except Exception:
            raise CompositionAdmissionError("snapshot_prepare_unavailable") from None

    def publish(self, intent_sha256: str) -> SnapshotPublicationRecord:
        """Only an acknowledged fresh exact claim permits one EXCHANGE invocation.

        Commit/readback uncertainty grants no dispatch and asserts no persistence.
        The original parent/attempt/gate remain blocking until explicit recovery.
        Transport acknowledgement does not decide publication outcome.
        An existing terminal record is replayed as history without requiring
        current ownership or granting authority to execute or reconcile again.
        """
        try:
            record = self._read(intent_sha256)
            if record.state in {"PUBLISHED", "NOT_PUBLISHED"}:
                return record
            if record.state != "PREPARED":
                raise CompositionAdmissionError("snapshot_dispatch_already_claimed")
            self._require_current(record.intent, recovery=False)
            self._require_unpublished(record.intent)
            claimed = self._store.claim_exchange(record)
            if claimed is None:
                raise CompositionAdmissionError("snapshot_dispatch_conflict")
            self._confirm(claimed, record.transition("EXCHANGE_INTENT"))
        except CompositionAdmissionError:
            raise
        except Exception:
            raise CompositionAdmissionError("snapshot_claim_unknown") from None
        try:
            # The actual executor/gate must also serialize dispatch with closure.
            # A Python check before the call cannot fence a paused old process.
            self._require_current(record.intent, recovery=False)
            self._require_unpublished(record.intent)
            self._executor.exchange_once(record.intent)
        except Exception:
            # Even an apparent pre-execution failure cannot prove the SQL outcome.
            # Closure + independent observation below decide it, without replay.
            pass
        return self.reconcile(intent_sha256)

    def reconcile(self, intent_sha256: str) -> SnapshotPublicationRecord:
        """Require retained ACTIVE/RETIRING ownership, even for terminal records.

        Close existing authority and classify B/A; never grant another dispatch.
        """
        try:
            record = self._read(intent_sha256)
            self._require_current(record.intent, recovery=True)
            if record.state in {"PUBLISHED", "NOT_PUBLISHED"}:
                return record
            closure: SnapshotPublisherClosure | None = None
            observation: SnapshotCatalogObservation | None = None
            state = "COMMIT_UNKNOWN"
            try:
                candidate = self._authority.close_publisher(record.intent)
                if type(candidate) is not SnapshotPublisherClosure:
                    raise CompositionAdmissionError("snapshot_closure_shape")
                candidate.__post_init__()
                if candidate.intent_sha256 != record.intent.intent_sha256:
                    raise CompositionAdmissionError("snapshot_closure_subject")
                closure = candidate
                self._require_current(record.intent, recovery=True)
                observation = self._inspect(record.intent)
                state = classify_snapshot(record.intent, observation)
            except Exception:
                # An unavailable closure or read is neither false success nor zero.
                # Preserve any valid observed evidence; the outcome stays unknown.
                pass
            self._require_current(record.intent, recovery=True)
            if record.state == "PREPARED" and state != "NOT_PUBLISHED":
                raise CompositionAdmissionError("snapshot_prepared_reconciliation_unknown")
            expected = record.transition(state, closure=closure, observation=observation)
            result = self._store.resolve(record, state=state, closure=closure, observation=observation)
            return self._confirm(result, expected)
        except CompositionAdmissionError:
            raise
        except Exception:
            raise CompositionAdmissionError("snapshot_resolution_unknown") from None

    def _require_current(self, intent: SnapshotPublicationIntent, *, recovery: bool) -> None:
        occurrence = self._authority.require_current(intent, recovery=recovery)
        if occurrence.receipt.state not in ({"ACTIVE", "RETIRING"} if recovery else {"ACTIVE"}):
            raise CompositionAdmissionError("snapshot_occurrence_state")
        guards = require_composition_attempt_scope(occurrence, intent.attempt)
        target = intent.target
        workload = next(row for row in occurrence.request.workloads if row.workload_id == intent.attempt.workload_id)
        resource = next((row for row in occurrence.request.resources if row.guard_id == target.guard_id), None)
        if (
            target.guard_id not in guards
            or resource is None
            or (resource.connector, resource.service_id, resource.physical_subject_sha256)
            != ("clickhouse", target.service_id, target.physical_subject_sha256)
            or target.write_subject_sha256 not in resource.write_subjects
            or target.write_subject_sha256 not in workload.write_subjects
            or workload.execution_cell != "mssql_clickhouse_full_refresh_v1"
        ):
            raise CompositionAdmissionError("snapshot_parent_scope")

    def _inspect(self, intent: SnapshotPublicationIntent) -> SnapshotCatalogObservation:
        observation = self._catalog.inspect(intent)
        if type(observation) is not SnapshotCatalogObservation:
            raise CompositionAdmissionError("snapshot_catalog_shape")
        observation.__post_init__()
        return observation

    def _require_unpublished(self, intent: SnapshotPublicationIntent) -> None:
        if classify_snapshot(intent, self._inspect(intent)) != "NOT_PUBLISHED":
            raise CompositionAdmissionError("snapshot_prepublication_observation")

    def _read(self, intent_sha256: str) -> SnapshotPublicationRecord:
        require_digest(intent_sha256)
        result = self._store.read(intent_sha256)
        if type(result) is not SnapshotPublicationRecord or result.intent.intent_sha256 != intent_sha256:
            raise CompositionAdmissionError("snapshot_record_missing_or_changed")
        return SnapshotPublicationRecord.from_bytes(result.to_bytes(), result.record_sha256)

    def _confirm(
        self, result: SnapshotPublicationRecord, expected: SnapshotPublicationRecord
    ) -> SnapshotPublicationRecord:
        if type(result) is not SnapshotPublicationRecord or result.to_bytes() != expected.to_bytes():
            raise CompositionAdmissionError("snapshot_commit_readback")
        observed = self._read(expected.intent.intent_sha256)
        if observed.to_bytes() != expected.to_bytes():
            raise CompositionAdmissionError("snapshot_commit_readback")
        return observed
