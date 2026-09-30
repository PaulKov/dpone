"""Explicit v2 enrollment and lifecycle on the original private SQLite file.

Names and owners are retained forever in this increment. No public acquire or
prepare shortcut exists; no existing operation can yield another invocation.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import re
import secrets
import sqlite3
from dataclasses import asdict
from pathlib import Path

from dpone.adapters.clickhouse_authority_schema import AuthorityVersion
from dpone.adapters.clickhouse_authority_storage import AuthorityStorage
from dpone.adapters.clickhouse_candidate_diagnostics import status_diagnostics, write_status
from dpone.adapters.clickhouse_observation_profile import ProtectedObservationProfile
from dpone.adapters.clickhouse_publication_journal import SQLitePublicationJournal, _storage_errors
from dpone.contracts import clickhouse_candidate as values
from dpone.contracts.clickhouse_authority import (
    AuthorityConflict,
    AuthorityError,
    AuthorityStorageIdentity,
    AuthoritySubject,
    OperationBinding,
    TransportState,
    require_text,
)
from dpone.contracts.clickhouse_observation import CandidateColumn, CandidateDesign, MultisetState, ObservationLimits


def _json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _candidate_row(db: sqlite3.Connection, operation_id: str) -> sqlite3.Row:
    cursor = db.cursor()
    try:
        row = cursor.execute("SELECT * FROM candidate_operations WHERE operation_id=?", (operation_id,)).fetchone()
        if row is None:
            raise AuthorityError("Unknown protected candidate operation")
        return sqlite3.Row(cursor, row)
    finally:
        cursor.close()


def _definition(row: sqlite3.Row) -> values.ProtectedPublicationRequest:
    try:
        body = json.loads(row["request"])
        design = body["design"]
        result = values.ProtectedPublicationRequest(
            body["operation_id"],
            AuthoritySubject(**body["subject"]),
            body["candidate"],
            CandidateDesign(
                tuple(CandidateColumn(**column) for column in design["columns"]),
                tuple(design["sorting_key"]),
                tuple(design["primary_key"]),
                tuple(design["partition_key"]),
            ),
            ObservationLimits(**body["limits"]),
        )
        profile = ProtectedObservationProfile(result.design, result.limits)
        if _json(asdict(result)) != row["request"] or profile.profile_digest != row["profile_digest"]:
            raise AuthorityError("Original candidate profile changed")
        return result
    except (TypeError, ValueError, KeyError) as error:
        raise AuthorityError("Invalid original candidate definition") from error


def _expected(row: sqlite3.Row) -> MultisetState:
    try:
        body = json.loads(row["expected"])
        value = MultisetState(body["count"], body["total"], body["encoded_bytes"], tuple(body["nulls"]))
        if _json(asdict(value)) != row["expected"]:
            raise AuthorityError("Noncanonical candidate multiset")
        return value
    except (TypeError, ValueError, KeyError) as error:
        raise AuthorityError("Invalid candidate multiset") from error


def _invocation_row(
    storage: AuthorityStorage, db: sqlite3.Connection, invocation: values.CandidateInvocation
) -> sqlite3.Row:
    invocation.assert_current()
    if storage.execution_identity() != invocation.authority_identity:
        raise AuthorityConflict("Foreign candidate authority")
    binding = SQLitePublicationJournal._binding(db, invocation.binding.operation_id)
    row = _candidate_row(db, binding.operation_id)
    if binding != invocation.binding or not hmac.compare_digest(
        row["invocation_hash"], hashlib.sha256(invocation._secret.encode()).hexdigest()
    ):
        raise AuthorityConflict("Foreign candidate invocation")
    _definition(row)
    return row


def _update_candidate(
    db: sqlite3.Connection, row: sqlite3.Row, event: str, changes: dict[str, object], query_id: str | None = None
) -> None:
    revision = row["revision"] + 1
    assignments = ",".join(f"{name}=?" for name in changes)
    result = db.execute(
        f"UPDATE candidate_operations SET {assignments},revision=? WHERE operation_id=? AND revision=?",
        (*changes.values(), revision, row["operation_id"], row["revision"]),
    )
    if result.rowcount != 1:
        raise AuthorityConflict("Candidate lifecycle revision changed")
    db.execute(
        "INSERT INTO candidate_history VALUES (?,?,?,?,?)",
        (row["operation_id"], revision, event, query_id, _json(changes)),
    )


class SQLiteCandidateAuthority:
    """No migration, re-enrollment, release, expiry or automatic source retry."""

    def __init__(self, path: Path, deployment_id: str) -> None:
        require_text(deployment_id)
        with _storage_errors():
            self._storage = AuthorityStorage(path, deployment_id, version=AuthorityVersion.V2)
        self._journal = SQLitePublicationJournal(self._storage)

    @staticmethod
    def provision(path: Path, deployment_id: str) -> None:
        require_text(deployment_id)
        with _storage_errors():
            AuthorityStorage.provision(path, deployment_id, version=AuthorityVersion.V2)

    def execution_identity(self) -> AuthorityStorageIdentity:
        with _storage_errors():
            return self._storage.execution_identity()

    def binding(self, operation_id: str) -> OperationBinding:
        return self._journal.binding(operation_id)

    def enroll(
        self, request: values.ProtectedPublicationRequest, enrollment: values.VerifiedEnrollment
    ) -> values.CandidateInvocation:
        """Reserve both physical roles atomically; return capability only on ACK."""
        enrollment.assert_current(request)
        profile = ProtectedObservationProfile(request.design, request.limits)
        inventory = enrollment.inventory_identity
        if inventory[2] != profile.profile_digest or request.subject.deployment_id != self._storage.deployment_id:
            raise AuthorityConflict("Foreign enrollment profile or deployment")
        identity = self.execution_identity()
        binding = OperationBinding(request.operation_id, request.subject, request.candidate, 1)
        secret = secrets.token_hex(32)
        with _storage_errors(), self._storage.transaction() as db:
            if db.execute("SELECT 1 FROM operations WHERE operation_id=?", (binding.operation_id,)).fetchone():
                raise AuthorityConflict("Existing candidate operation cannot re-enroll")
            subject = binding.subject
            names = (subject.target, binding.candidate)
            occupied = db.execute(
                "SELECT 1 FROM name_reservations WHERE deployment=? AND server=? AND database_name=? AND physical_name IN (?,?)",
                (subject.deployment_id, subject.server_id, subject.database, *names),
            ).fetchone()
            if occupied:
                raise AuthorityConflict("candidate_namespace_occupied")
            db.execute(
                "INSERT INTO subjects VALUES (?,?,?,?)",
                (subject.key, _json(asdict(subject)), binding.operation_id, binding.epoch),
            )
            query_id = "dpone-publication-" + hashlib.sha256(binding.operation_id.encode()).hexdigest()
            db.execute(
                "INSERT INTO operations(operation_id,subject_key,candidate,query_id) VALUES (?,?,?,?)",
                (binding.operation_id, subject.key, binding.candidate, query_id),
            )
            db.execute(
                "INSERT INTO history VALUES (?,0,'acquire','not_started',NULL,NULL,NULL)", (binding.operation_id,)
            )
            for role, name in zip(("target", "candidate"), names, strict=True):
                db.execute(
                    "INSERT INTO name_reservations VALUES (?,?,?,?,?,?)",
                    (subject.deployment_id, subject.server_id, subject.database, name, binding.operation_id, role),
                )
            expected = MultisetState(0, 0, 0, (0,) * len(request.design.columns))
            db.execute(
                "INSERT INTO candidate_operations(operation_id,request,profile_digest,inventory,invocation_hash,expected) VALUES (?,?,?,?,?,?)",
                (
                    binding.operation_id,
                    _json(asdict(request)),
                    profile.profile_digest,
                    _json(inventory),
                    hashlib.sha256(secret.encode()).hexdigest(),
                    _json(asdict(expected)),
                ),
            )
            db.execute(
                "INSERT INTO candidate_history VALUES (?,0,'enroll',NULL,?)",
                (binding.operation_id, _json({"profile_digest": profile.profile_digest})),
            )
        return values.CandidateInvocation._issue(binding, identity, secret)

    def source_exhausted(self, invocation: values.CandidateInvocation) -> None:
        """Record only trusted normal exhaustion, never an exception or readback."""
        with _storage_errors(), self._storage.transaction() as db:
            row = _invocation_row(self._storage, db, invocation)
            if row["lifecycle"] in {"retained", "sealed"}:
                raise AuthorityConflict("Closed candidate cannot claim source exhaustion")
            if not row["source_exhausted"]:
                _update_candidate(db, row, "source_exhausted", {"source_exhausted": 1})

    def close_admission(self, operation_id: str) -> None:
        """Recovery may close admission but gains no request or source capability."""
        with _storage_errors(), self._storage.transaction() as db:
            row = _candidate_row(db, operation_id)
            if not row["admission_closed"]:
                _update_candidate(db, row, "close_admission", {"admission_closed": 1, "lifecycle": "admission_closed"})

    def retain(self, operation_id: str, reason: str) -> None:
        """Persist a safe phase/reason code, never a raw source or SDK exception."""
        if type(reason) is not str or re.fullmatch(r"[a-z][a-z0-9_]{0,63}", reason) is None:
            raise AuthorityError("Retained reason must be a safe code")
        with _storage_errors(), self._storage.transaction() as db:
            row = _candidate_row(db, operation_id)
            if row["lifecycle"] != "retained":
                _update_candidate(
                    db, row, "retain", {"admission_closed": 1, "lifecycle": "retained", "retained_reason": reason}
                )

    def inspect(self, operation_id: str) -> values.CandidateStatus:
        """Read one consistent status; never issue an invocation from the record."""
        with _storage_errors(), self._storage.transaction() as db:
            row = _candidate_row(db, operation_id)
            definition = _definition(row)
            binding = self._journal._binding(db, operation_id)
            if binding != OperationBinding(
                definition.operation_id, definition.subject, definition.candidate, binding.epoch
            ):
                raise AuthorityError("Candidate and publication bindings disagree")
            requests = db.execute(
                "SELECT query_id,state FROM candidate_requests WHERE operation_id=? ORDER BY sequence", (operation_id,)
            ).fetchall()
            entry = self._journal._entry(db, self._journal._row(db, operation_id))
            if db.execute("SELECT 1 FROM candidate_seals WHERE operation_id=?", (operation_id,)).fetchone():
                raise AuthorityError("Candidate seal producer is not available in this staged increment")
            expected = _expected(row)
            if len(expected.nulls) != len(definition.design.columns):
                raise AuthorityError("Candidate evidence width mismatch")
            return values.CandidateStatus(
                binding,
                definition,
                row["revision"],
                row["lifecycle"],
                bool(row["admission_closed"]),
                bool(row["source_exhausted"]),
                tuple(q for q, _ in requests),
                tuple(q for q, state in requests if state == TransportState.CLOSED_TERMINAL),
                tuple(q for q, state in requests if state == TransportState.MAY_HAVE_SENT),
                expected,
                None,
                entry.record.state if entry else None,
                row["retained_reason"],
                row["profile_digest"],
            )

    def diagnostics(self, operation_id: str) -> dict[str, object]:
        return status_diagnostics(self.inspect(operation_id))

    def write_diagnostics(self, operation_id: str, destination: Path) -> None:
        write_status(self.diagnostics(operation_id), destination, self._storage.path.parent)
