"""Retained single-host publication ownership; no SQL client or implicit binding."""

from __future__ import annotations

import hashlib
import hmac
import json
import secrets
import sqlite3
from dataclasses import asdict, replace
from pathlib import Path

from dpone.adapters.clickhouse_authority_storage import AuthorityStorage
from dpone.adapters.clickhouse_publication_codec import decode_record, encode_record
from dpone.contracts.clickhouse_authority import (
    AuthorityConflict,
    AuthorityError,
    AuthoritySubject,
    DispatchGrant,
    JournalEntry,
    OperationBinding,
    TransportState,
)
from dpone.contracts.clickhouse_publication import PublicationIntent, PublicationRecord, PublicationState


class SQLitePublicationAuthority:
    """Explicitly provisioned local authority. No expiry, handoff or release API."""

    def __init__(self, path: Path, deployment_id: str) -> None:
        self._storage = AuthorityStorage(path, deployment_id)

    @staticmethod
    def provision(path: Path, deployment_id: str) -> None:
        AuthorityStorage.provision(path, deployment_id)

    def acquire(self, operation_id: str, subject: AuthoritySubject, candidate: str) -> OperationBinding:
        binding = OperationBinding(operation_id, subject, candidate, 1)
        if subject.deployment_id != self._storage.deployment_id:
            raise AuthorityError("Foreign deployment")
        payload = json.dumps(asdict(subject), sort_keys=True, separators=(",", ":"))
        with self._storage.transaction() as db:
            existing = db.execute("SELECT 1 FROM operations WHERE operation_id=?", (operation_id,)).fetchone()
            if existing:
                if self._binding(db, operation_id) != binding:
                    raise AuthorityConflict("Operation cannot be rebound")
                return binding
            if db.execute("SELECT 1 FROM subjects WHERE subject_key=?", (subject.key,)).fetchone():
                raise AuthorityConflict("Target has a retained owner")
            db.execute("INSERT INTO subjects VALUES (?,?,?,?)", (subject.key, payload, operation_id, 1))
            query_id = "dpone-publication-" + hashlib.sha256(operation_id.encode()).hexdigest()
            db.execute(
                "INSERT INTO operations(operation_id,subject_key,candidate,query_id) VALUES (?,?,?,?)",
                (operation_id, subject.key, candidate, query_id),
            )
            db.execute("INSERT INTO history VALUES (?,0,'acquire','not_started',NULL)", (operation_id,))
        return binding

    @staticmethod
    def _binding(db: sqlite3.Connection, operation_id: str) -> OperationBinding:
        row = db.execute(
            "SELECT s.payload,s.owner,s.epoch,o.candidate,s.subject_key FROM operations o JOIN subjects s USING(subject_key) WHERE o.operation_id=?",
            (operation_id,),
        ).fetchone()
        if row is None or row[1] != operation_id:
            raise AuthorityError("No original retained owner")
        try:
            subject = AuthoritySubject(**json.loads(row[0]))
            if subject.key != row[4]:
                raise AuthorityError("Corrupt subject identity")
            return OperationBinding(operation_id, subject, row[3], row[2])
        except (TypeError, ValueError) as error:
            raise AuthorityError("Invalid original binding") from error

    def binding(self, operation_id: str) -> OperationBinding:
        with self._storage.connection() as db:
            return self._binding(db, operation_id)

    @staticmethod
    def _row(db: sqlite3.Connection, operation_id: str) -> sqlite3.Row:
        cursor = db.cursor()
        cursor.row_factory = sqlite3.Row
        try:
            row = cursor.execute("SELECT * FROM operations WHERE operation_id=?", (operation_id,)).fetchone()
        finally:
            cursor.close()
        if row is None:
            raise AuthorityError("Unknown authority operation")
        return row

    def _entry(self, db: sqlite3.Connection, row: sqlite3.Row) -> JournalEntry | None:
        binding = self._binding(db, row["operation_id"])
        if row["record"] is None:
            return None
        record = decode_record(row["record"])
        expected = (binding.subject.server_id, binding.subject.database, binding.subject.target, binding.candidate)
        if (
            record.intent.operation_id != binding.operation_id
            or record.intent.before.subject != expected
            or record.intent.query_id != row["query_id"]
        ):
            raise AuthorityError("Stored intent does not match retained subject")
        intent = json.dumps(asdict(record.intent), sort_keys=True, separators=(",", ":"))
        if intent != row["intent"]:
            raise AuthorityError("Stored immutable intent changed")
        return JournalEntry(record, row["revision"])

    def prepare(self, binding: OperationBinding, intent: PublicationIntent) -> JournalEntry:
        """Create an exact immutable intent, never adopt a divergent retry."""
        payload = encode_record(PublicationRecord(intent, PublicationState.PREPARED))
        expected = (binding.subject.server_id, binding.subject.database, binding.subject.target, binding.candidate)
        if intent.operation_id != binding.operation_id or intent.before.subject != expected:
            raise AuthorityConflict("Intent does not belong to the retained subject")
        with self._storage.transaction() as db:
            if self._binding(db, binding.operation_id) != binding:
                raise AuthorityConflict("Stale binding")
            row = self._row(db, binding.operation_id)
            entry = self._entry(db, row)
            if entry is not None:
                if encode_record(entry.record) != payload:
                    raise AuthorityConflict("Preparation cannot replace an existing record")
                return entry
            self._update(
                db,
                row,
                "prepare",
                {"record": payload, "intent": json.dumps(asdict(intent), sort_keys=True, separators=(",", ":"))},
            )
            result = self._entry(db, self._row(db, binding.operation_id))
            assert result is not None
        return result

    def read(self, operation_id: str) -> JournalEntry | None:
        """Read a registered operation's record; no grant is exposed."""
        with self._storage.connection() as db:
            return self._entry(db, self._row(db, operation_id))

    @staticmethod
    def _update(db: sqlite3.Connection, row: sqlite3.Row, event: str, changes: dict[str, object]) -> None:
        revision = row["revision"] + 1
        assignments = ",".join(f"{key}=?" for key in changes)
        changed = db.execute(
            f"UPDATE operations SET {assignments},revision=? WHERE operation_id=? AND revision=?",
            (*changes.values(), revision, row["operation_id"], row["revision"]),
        )
        if changed.rowcount != 1:
            raise AuthorityConflict("Authority revision changed")
        payload = str(changes.get("record", row["record"]))
        digest = hashlib.sha256(payload.encode()).hexdigest()
        db.execute(
            "INSERT INTO history VALUES (?,?,?,?,?)",
            (row["operation_id"], revision, event, changes.get("transport", row["transport"]), digest),
        )

    def claim(self, entry: JournalEntry) -> DispatchGrant | None:
        """Only a positively acknowledged fresh CAS returns its volatile secret."""
        with self._storage.transaction() as db:
            row = self._row(db, entry.record.intent.operation_id)
            current = self._entry(db, row)
            if (
                current != entry
                or entry.record.state != PublicationState.PREPARED
                or row["transport"] != TransportState.NOT_STARTED
            ):
                return None
            binding = self._binding(db, row["operation_id"])
            grant = DispatchGrant(binding.operation_id, binding.epoch, secrets.token_hex(32))
            claimed = replace(entry.record, state=PublicationState.CLAIMED, claim_granted=True)
            self._update(
                db,
                row,
                "claim",
                {"record": encode_record(claimed), "grant_hash": hashlib.sha256(grant.secret.encode()).hexdigest()},
            )
        return grant

    def _grant_row(self, db: sqlite3.Connection, grant: DispatchGrant) -> sqlite3.Row:
        row = self._row(db, grant.operation_id)
        binding = self._binding(db, grant.operation_id)
        entry = self._entry(db, row)
        expected = hashlib.sha256(grant.secret.encode()).hexdigest()
        if (
            binding.epoch != grant.epoch
            or entry is None
            or not entry.record.claim_granted
            or not hmac.compare_digest(row["grant_hash"] or "", expected)
        ):
            raise AuthorityConflict("Invalid or revoked dispatch grant")
        return row

    def begin_send(self, grant: DispatchGrant) -> None:
        """Spend the sole send permission before any network bytes may be sent."""
        with self._storage.transaction() as db:
            row = self._grant_row(db, grant)
            if row["transport"] != TransportState.NOT_STARTED:
                raise AuthorityConflict("Send permission is already consumed or closed")
            self._update(db, row, "begin_send", {"transport": TransportState.MAY_HAVE_SENT})

    def close_without_send(self, operation_id: str) -> None:
        """Compete with send entry; never infer no send from a failed transport."""
        with self._storage.transaction() as db:
            row = self._row(db, operation_id)
            if self._entry(db, row) is None:
                raise AuthorityError("Prepare an intent before closing publication")
            if row["transport"] == TransportState.CLOSED_WITHOUT_SEND:
                return
            if row["transport"] != TransportState.NOT_STARTED:
                raise AuthorityConflict("Possibly sent request cannot close as unsent")
            self._update(
                db, row, "close_without_send", {"transport": TransportState.CLOSED_WITHOUT_SEND, "grant_hash": None}
            )

    def transport_state(self, operation_id: str) -> TransportState:
        with self._storage.connection() as db:
            row = self._row(db, operation_id)
            self._entry(db, row)
            return TransportState(row["transport"])
