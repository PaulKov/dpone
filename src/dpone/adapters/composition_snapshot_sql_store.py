"""Protected snapshot publication journal under the composition SQL ledger lock.

The SQL commit acknowledges only this invocation. A driver exception, including
lost commit acknowledgement, propagates as uncertainty; readback never repairs
it into an exchange permit. History and retained ownership survive recovery.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, Protocol
from uuid import UUID

from dpone.adapters.composition_mssql_attempts import ConnectionFactory, composition_control_transaction
from dpone.adapters.composition_mssql_existing_operation import require_existing_execution_in
from dpone.adapters.composition_mssql_schema import require_control_schema
from dpone.adapters.composition_mssql_store_queries import CompositionMssqlLedger
from dpone.adapters.composition_snapshot_sql_schema import require_snapshot_publication_schema
from dpone.contracts.composition_identity import CompositionAdmissionError, require_digest
from dpone.contracts.composition_persistence import CompositionAttemptIdentity
from dpone.contracts.composition_snapshot import (
    SnapshotCatalogObservation,
    SnapshotPublicationIntent,
    SnapshotPublicationRecord,
    SnapshotPublisherClosure,
)
from dpone.ports.composition_sql import CompositionSqlContext


class SnapshotJournalAuthority(Protocol):
    """Trusted producers reopen originals on the supplied protected transaction.

    Neither callback may commit, change cursor/session, or substitute the ledger.
    Ready requires exact issued UUIDs, ready publisher and closed ingest/source
    originals. Closure requires exact CLOSED_GATES/QUIESCENCE originals for the
    publisher, including the irreversible dispatch barrier. DTO equality alone
    is not evidence. These callbacks never issue or re-enable a principal.
    """

    def require_ready(self, context: CompositionSqlContext, intent: SnapshotPublicationIntent) -> None:
        """Reopen protected enrollment, generation and ready publisher originals."""

    def require_closure(
        self, context: CompositionSqlContext, intent: SnapshotPublicationIntent, closure: SnapshotPublisherClosure
    ) -> None:
        """Verify complete protected closure originals against the supplied subject."""


def _require(condition: bool, reason: str) -> None:
    if not condition:
        raise CompositionAdmissionError("snapshot_journal_" + reason)


class _Queries:
    """Exact canonical reads and append-only revisions in one pinned transaction."""

    def __init__(self, ledger: CompositionMssqlLedger) -> None:
        self.ledger = ledger
        self._cursor, self._schema, self._service = ledger.cursor, ledger.schema, ledger.expected_service_id
        self._transaction = ledger.require_transaction()
        require_snapshot_publication_schema(ledger.cursor, ledger.schema)
        self.check()

    def check(self) -> None:
        _require(
            self.ledger.cursor is self._cursor
            and (self.ledger.schema, self.ledger.expected_service_id) == (self._schema, self._service),
            "context",
        )
        self.ledger.require_transaction(self._transaction)

    def rows(self, sql: str, *parameters: object) -> tuple[tuple[Any, ...], ...]:
        self.check()
        self._cursor.execute(sql, *parameters)
        result = tuple(tuple(row) for row in self._cursor.fetchall())
        self.check()
        return result

    def scope(self, intent: SnapshotPublicationIntent, *, recovery: bool) -> object:
        self.check()
        occurrence, receipt = require_existing_execution_in(
            self.ledger,
            intent.attempt,
            expected_service_id=self._service,
            terminal_validator=self.ledger.terminal_validator,
        )
        self.check()
        intent.require_parent_scope(occurrence, recovery=recovery)
        _require(receipt.state in ({"RUNNING", "COMMIT_UNKNOWN"} if recovery else {"RUNNING"}), "attempt_state")
        return occurrence, receipt

    def intents(self, where: str, *parameters: object) -> tuple[SnapshotPublicationIntent, ...]:
        rows = self.rows(
            "SELECT TOP (8193) intent_sha256,operation_key,write_subject_sha256,exchange_query_id,"
            "CASE WHEN DATALENGTH(intent_document) BETWEEN 1 AND 8388608 THEN intent_document END "
            f"FROM {self.ledger.table('snapshot_intents')} WITH (UPDLOCK,HOLDLOCK) {where};",
            *parameters,
        )
        _require(len(rows) <= 8192, "intent_budget")
        values = []
        for row in rows:
            _require(len(row) == 5, "intent_shape")
            value = SnapshotPublicationIntent.from_bytes(row[4], row[0])
            _require(
                row[:4]
                == (
                    value.intent_sha256,
                    value.attempt.attempt_sha256,
                    value.target.write_subject_sha256,
                    value.exchange_query_id,
                ),
                "intent_index",
            )
            values.append(value)
        return tuple(values)

    def read(self, key: str) -> SnapshotPublicationRecord | None:
        values = self.intents("WHERE intent_sha256=?", key)
        _require(len(values) <= 1, "intent_duplicate")
        if not values:
            return None
        intent = values[0]
        rows = self.rows(
            "SELECT TOP (8193) revision,state,record_sha256,"
            "CASE WHEN DATALENGTH(record_document) BETWEEN 1 AND 8388608 THEN record_document END "
            f"FROM {self.ledger.table('snapshot_history')} WITH (UPDLOCK,HOLDLOCK) "
            "WHERE intent_sha256=? ORDER BY revision;",
            key,
        )
        _require(0 < len(rows) <= 8192, "history_missing_or_budget")
        previous = None
        for row in rows:
            _require(len(row) == 4, "record_shape")
            current = SnapshotPublicationRecord.from_bytes(row[3], row[2])
            _require(
                current.intent.to_bytes() == intent.to_bytes() and (current.revision, current.state) == row[:2],
                "history_index",
            )
            expected = (
                SnapshotPublicationRecord(intent)
                if previous is None
                else previous.transition(
                    current.state,
                    closure=current.closure,
                    observation=current.observation,
                )
            )
            _require(current.to_bytes() == expected.to_bytes(), "history_transition")
            previous = current
        return previous

    def append(self, table: str, columns: str, *values: object) -> None:
        self.check()
        self._cursor.execute(
            f"INSERT INTO {self.ledger.table(table)} ({columns}) VALUES ({','.join('?' for _ in values)});",
            *values,
        )
        self.check()

    def record(self, record: SnapshotPublicationRecord) -> None:
        self.append(
            "snapshot_history",
            "intent_sha256,revision,state,record_sha256,record_document",
            record.intent.intent_sha256,
            record.revision,
            record.state,
            record.record_sha256,
            record.to_bytes(),
        )
        _require(self.read(record.intent.intent_sha256) == record, "append_readback")


class MssqlSnapshotPublicationStore:
    """One attempt's immutable publication originals and exact single-winner CAS.

    Construct with a trusted control connection and transaction-scoped authority.
    Runtime never installs tables, grants workers access, retries mutation, or
    releases parent/attempt ownership. ``records`` only lists this exact attempt.
    """

    def __init__(
        self,
        connection_factory: ConnectionFactory,
        *,
        expected_service_id: str,
        authority: SnapshotJournalAuthority,
        attempt: CompositionAttemptIdentity,
        control_schema: str = "dpone_control",
    ) -> None:
        try:
            valid = str(UUID(expected_service_id)) == expected_service_id
        except (ValueError, TypeError, AttributeError):
            valid = False
        _require(valid, "control_service")
        attempt.__post_init__()
        self._factory, self._service = connection_factory, expected_service_id
        self._schema, self._authority, self._attempt = require_control_schema(control_schema), authority, attempt

    @contextmanager
    def _transaction(self) -> Iterator[_Queries]:
        with composition_control_transaction(self._factory, self._schema, self._service) as ledger:
            q = _Queries(ledger)
            yield q
            q.check()

    def _bound(self, intent: SnapshotPublicationIntent) -> None:
        intent.__post_init__()
        _require(intent.attempt == self._attempt, "attempt_binding")

    def prepare(self, intent: SnapshotPublicationIntent) -> SnapshotPublicationRecord:
        """Insert PREPARED or acknowledge identical existing PREPARED only."""
        self._bound(intent)
        result = SnapshotPublicationRecord(intent)
        with self._transaction() as q:
            original = q.scope(intent, recovery=False)
            existing = q.intents(
                "WHERE intent_sha256=? OR (operation_key=? AND write_subject_sha256=?) OR exchange_query_id=?",
                intent.intent_sha256,
                intent.attempt.attempt_sha256,
                intent.target.write_subject_sha256,
                intent.exchange_query_id,
            )
            _require(not existing or existing == (intent,), "prepare_conflict")
            self._authority.require_ready(q.ledger, intent)
            q.check()
            _require(q.scope(intent, recovery=False) == original, "authority_changed")
            if existing:
                _require(q.read(intent.intent_sha256) == result, "prepare_conflict")
            else:
                q.append(
                    "snapshot_intents",
                    "intent_sha256,operation_key,write_subject_sha256,exchange_query_id,intent_document",
                    intent.intent_sha256,
                    intent.attempt.attempt_sha256,
                    intent.target.write_subject_sha256,
                    intent.exchange_query_id,
                    intent.to_bytes(),
                )
                q.record(result)
        _require(self.read(intent.intent_sha256) == result, "prepare_readback")
        return result

    def read(self, intent_sha256: str) -> SnapshotPublicationRecord | None:
        """Open a fresh connection and validate every complete immutable revision."""
        require_digest(intent_sha256)
        with self._transaction() as q:
            record = q.read(intent_sha256)
            if record is not None:
                self._bound(record.intent)
            return record

    def claim_exchange(self, expected: SnapshotPublicationRecord) -> SnapshotPublicationRecord | None:
        """ACK only this invocation's new PREPARED→EXCHANGE_INTENT claim."""
        self._bound(expected.intent)
        expected.__post_init__()
        with self._transaction() as q:
            if q.read(expected.intent.intent_sha256) != expected or expected.state != "PREPARED":
                return None
            original = q.scope(expected.intent, recovery=False)
            self._authority.require_ready(q.ledger, expected.intent)
            q.check()
            _require(q.scope(expected.intent, recovery=False) == original, "authority_changed")
            _require(q.read(expected.intent.intent_sha256) == expected, "claim_changed")
            result = expected.transition("EXCHANGE_INTENT")
            q.record(result)
        # Never reached after commit uncertainty; no winner may be resurrected.
        _require(self.read(expected.intent.intent_sha256) == result, "claim_readback")
        return result

    def resolve(
        self,
        expected: SnapshotPublicationRecord,
        *,
        state: str,
        closure: SnapshotPublisherClosure | None,
        observation: SnapshotCatalogObservation | None,
    ) -> SnapshotPublicationRecord:
        """Append exact recovery CAS with independently protected closure proof."""
        self._bound(expected.intent)
        result = expected.transition(state, closure=closure, observation=observation)
        _require(state != "EXCHANGE_INTENT", "resolve_state")
        with self._transaction() as q:
            _require(q.read(expected.intent.intent_sha256) == expected, "resolve_conflict")
            original = q.scope(expected.intent, recovery=True)
            if closure is not None:
                self._authority.require_closure(q.ledger, expected.intent, closure)
                q.check()
            _require(q.scope(expected.intent, recovery=True) == original, "authority_changed")
            _require(q.read(expected.intent.intent_sha256) == expected, "resolve_conflict")
            q.record(result)
        _require(self.read(expected.intent.intent_sha256) == result, "resolve_readback")
        return result

    def records(self) -> tuple[SnapshotPublicationRecord, ...]:
        """Reopen this exact attempt's complete immutable histories for recovery."""
        with self._transaction() as q:
            values = q.intents("WHERE operation_key=? ORDER BY intent_sha256", self._attempt.attempt_sha256)
            records = []
            for value in values:
                self._bound(value)
                record = q.read(value.intent_sha256)
                _require(record is not None, "history_missing")
                assert record is not None
                records.append(record)
            return tuple(records)
