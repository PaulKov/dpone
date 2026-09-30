"""Retained single-host publication ownership; no SQL client or implicit binding."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from dataclasses import asdict
from pathlib import Path

from dpone.adapters.clickhouse_authority_storage import AuthorityStorage
from dpone.adapters.clickhouse_publication_journal import SQLitePublicationJournal, _storage_errors
from dpone.contracts.clickhouse_authority import (
    AuthorityConflict,
    AuthorityError,
    AuthorityStorageIdentity,
    AuthoritySubject,
    OperationBinding,
    require_text,
)


class SQLitePublicationAuthority(SQLitePublicationJournal):
    """Explicitly provisioned local authority. No expiry, handoff or release API."""

    def __init__(self, path: Path, deployment_id: str) -> None:
        require_text(deployment_id)
        with _storage_errors():
            super().__init__(AuthorityStorage(path, deployment_id))

    @staticmethod
    def provision(path: Path, deployment_id: str) -> None:
        require_text(deployment_id)
        with _storage_errors():
            AuthorityStorage.provision(path, deployment_id)

    def execution_identity(self) -> AuthorityStorageIdentity:
        """Return checked local identity without provisioning or granting dispatch."""
        with _storage_errors():
            return self._storage.execution_identity()

    def acquire(self, operation_id: str, subject: AuthoritySubject, candidate: str) -> OperationBinding:
        binding = OperationBinding(operation_id, subject, candidate, 1)
        if subject.deployment_id != self._storage.deployment_id:
            raise AuthorityError("Foreign deployment")
        payload = json.dumps(asdict(subject), sort_keys=True, separators=(",", ":"))
        with _storage_errors(), self._storage.transaction() as db:
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
            db.execute("INSERT INTO history VALUES (?,0,'acquire','not_started',NULL,NULL,NULL)", (operation_id,))
        return binding

    def diagnostics(self, operation_id: str) -> dict[str, object]:
        """Redacted original history, never a dispatch grant or recovery input."""
        with _storage_errors(), self._storage.transaction() as db:
            row = self._row(db, operation_id)
            binding = self._binding(db, operation_id)
            entry = self._entry(db, row)
            history = db.execute(
                "SELECT revision,event,transport,record_digest,observation_digest FROM history WHERE operation_id=? ORDER BY revision",
                (operation_id,),
            ).fetchall()
            return {
                "schema_version": "dpone.clickhouse.authority-diagnostics.v1",
                "binding": asdict(binding),
                "revision": row["revision"],
                "state": entry.record.state.value if entry else "registered",
                "claim_granted": entry.record.claim_granted if entry else False,
                "method": entry.record.intent.method if entry else None,
                "reason": entry.record.intent.reason if entry else None,
                "query_id": row["query_id"],
                "transport": row["transport"],
                "completion_digest": row["completion_digest"],
                "owner_retained": True,
                "history": [
                    dict(
                        zip(
                            ("revision", "event", "transport", "record_digest", "observation_digest"), item, strict=True
                        )
                    )
                    for item in history
                ],
            }

    def write_diagnostics(self, operation_id: str, destination: Path) -> None:
        """Atomically create a new report outside the private authority directory."""
        temporary = None
        try:
            if destination.parent.resolve() == self._storage.path.parent.resolve():
                raise AuthorityError("Write diagnostics outside the authority directory")
            payload = json.dumps(self.diagnostics(operation_id), sort_keys=True, indent=2, allow_nan=False)
            with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", dir=destination.parent, delete=False
            ) as stream:
                temporary = Path(stream.name)
                stream.write(payload + "\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.link(temporary, destination)
            descriptor = os.open(destination.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        except OSError as error:
            raise AuthorityError("Diagnostic output could not be created; existing files preserved") from error
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
