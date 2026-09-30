"""Retained single-host publication ownership; no SQL client or implicit binding."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import asdict
from pathlib import Path

from dpone.adapters.clickhouse_authority_storage import AuthorityStorage
from dpone.contracts.clickhouse_authority import AuthorityConflict, AuthorityError, AuthoritySubject, OperationBinding


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
