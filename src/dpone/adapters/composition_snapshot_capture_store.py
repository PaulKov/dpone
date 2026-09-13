"""Protected SQL capture history and root-only immutable PVC originals.

Only CLAIMED yields a fresh capture permit. Existing claims always reject; lost
ACKs require read-only reconciliation. Files commit before SQL pins their hashes.
A crash can leave an orphan/tombstone, never an unpinned CREATE authorization.
"""

from __future__ import annotations

import os
import stat
from collections.abc import Callable
from pathlib import Path
from typing import Any
from uuid import uuid4

from dpone.adapters.composition_clickhouse_gate_queries import ClickHouseGateBinding
from dpone.adapters.composition_mssql_attempts import composition_control_transaction
from dpone.adapters.composition_mssql_existing_operation import require_existing_execution_in
from dpone.adapters.composition_mssql_store_queries import CompositionMssqlLedger
from dpone.adapters.composition_snapshot_capture_schema import require_snapshot_capture_schema
from dpone.adapters.composition_supervisor_filesystem import (
    absolute_supervisor_path,
    open_protected,
    require_supervisor,
)
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.composition_persistence import (
    CompositionAttemptIdentity,
    CompositionAttemptProof,
    decode_attempt_proof,
)
from dpone.contracts.composition_snapshot_capture import (
    CAPTURE_PHASES,
    MAX_CAPTURE_METADATA_BYTES,
    SnapshotCaptureRecord,
    SnapshotCaptureSubject,
    capture_digest,
    decode_generation_seal,
)


class MssqlSnapshotCaptureStore:
    """One append-only capture per reserved attempt, verified against source closure.

    ``source_verifier`` reads immutable verified configuration only: it must not
    open a SQL transaction because callers may already hold the ledger lock.
    """

    def __init__(
        self,
        connection_factory: Any,
        *,
        expected_service_id: str,
        source_verifier: Callable[[CompositionAttemptIdentity], SnapshotCaptureSubject],
        control_schema: str = "dpone_control",
    ) -> None:
        if not callable(source_verifier):
            raise CompositionAdmissionError("snapshot_capture_source_verifier")
        self._factory, self._service, self._schema = connection_factory, expected_service_id, control_schema
        self._verify = source_verifier

    def _transaction(self):
        return composition_control_transaction(self._factory, self._schema, self._service)

    def _authorize(self, ledger: Any, subject: SnapshotCaptureSubject, *, active: bool) -> None:
        subject.__post_init__()
        if self._verify(subject.attempt) != subject:
            raise CompositionAdmissionError("snapshot_capture_source_binding")
        occurrence, receipt = require_existing_execution_in(
            ledger, subject.attempt, expected_service_id=self._service, terminal_validator=ledger.terminal_validator
        )
        if active and (occurrence.receipt.state != "ACTIVE" or receipt.state != "RUNNING"):
            raise CompositionAdmissionError("snapshot_capture_attempt")
        resource = next((r for r in occurrence.request.resources if r.guard_id == subject.target.guard_id), None)
        if resource is None or subject.target.write_subject_sha256 not in resource.write_subjects:
            raise CompositionAdmissionError("snapshot_capture_target")
        require_snapshot_capture_schema(ledger.cursor, self._schema)
        ledger.require_transaction()

    def _read(self, ledger: Any, subject: SnapshotCaptureSubject) -> dict[str, bytes]:
        ledger.cursor.execute(
            f"SELECT TOP (4) phase,subject_sha256,LOWER(CONVERT(char(36),generation_uuid)),document_sha256,document FROM {ledger.table('snapshot_captures')} WITH (HOLDLOCK) WHERE operation_key=?;",
            subject.attempt.attempt_sha256,
        )
        events: dict[str, bytes] = {}
        for row in ledger.cursor.fetchall():
            if (
                len(row) != 5
                or row[0] in events
                or row[0] not in CAPTURE_PHASES
                or row[1] != subject.subject_sha256
                or row[2] != subject.generation_uuid
                or type(row[4]) is not bytes
                or not 1 <= len(row[4]) <= MAX_CAPTURE_METADATA_BYTES
                or capture_digest(row[4]) != row[3]
            ):
                raise CompositionAdmissionError("snapshot_capture_journal")
            events[row[0]] = row[4]
        if set(events) != set(CAPTURE_PHASES[: len(events)]):
            raise CompositionAdmissionError("snapshot_capture_order")
        if events and events["CLAIMED"] != subject.to_bytes():
            raise CompositionAdmissionError("snapshot_capture_subject")
        if "CAPTURED" in events:
            captured = SnapshotCaptureRecord.from_bytes(events["CAPTURED"])
            if (
                captured.subject_sha256 != subject.subject_sha256
                or captured.rows > subject.limits.max_rows
                or captured.source_bytes > subject.limits.max_source_bytes
                or captured.wire_bytes > subject.limits.max_wire_bytes
            ):
                raise CompositionAdmissionError("snapshot_capture_budget")
        if "GENERATION_SEALED" in events:
            decode_generation_seal(
                subject, SnapshotCaptureRecord.from_bytes(events["CAPTURED"]), events["GENERATION_SEALED"]
            )
            from dpone.contracts.strict_json import strict_json_object

            sealed = strict_json_object(events["GENERATION_SEALED"])
            proofs = []
            for field, kind in (("closed_gates_sha256", "CLOSED_GATES"), ("quiescence_sha256", "QUIESCENCE")):
                ledger.cursor.execute(
                    f"SELECT TOP (2) proof_document FROM {ledger.table('proofs')} WITH (HOLDLOCK) "
                    "WHERE operation_key=? AND operation_family='execution' AND kind=? AND proof_sha256=?;",
                    subject.attempt.attempt_sha256,
                    kind,
                    sealed[field],
                )
                rows = tuple(tuple(row) for row in ledger.cursor.fetchall())
                if len(rows) != 1:
                    raise CompositionAdmissionError("snapshot_capture_closure")
                proofs.append(decode_attempt_proof(rows[0][0], sealed[field]))
            self._require_closed(ledger, subject, tuple(proofs))
        return events

    def read(self, attempt: CompositionAttemptIdentity) -> tuple[SnapshotCaptureSubject, dict[str, bytes]]:
        with self._transaction() as ledger:
            return self.read_in(ledger, attempt)

    def read_in(
        self, ledger: CompositionMssqlLedger, attempt: CompositionAttemptIdentity
    ) -> tuple[SnapshotCaptureSubject, dict[str, bytes]]:
        """Verify originals using the caller's existing protected transaction."""
        subject = self._verify(attempt)
        self._authorize(ledger, subject, active=False)
        return subject, self._read(ledger, subject)

    def claim_once(self, attempt: CompositionAttemptIdentity) -> SnapshotCaptureSubject:
        subject = self._verify(attempt)
        with self._transaction() as ledger:
            self._authorize(ledger, subject, active=True)
            if self._read(ledger, subject):
                raise CompositionAdmissionError("snapshot_capture_replay")
            self._insert(ledger, subject, "CLAIMED", subject.to_bytes())
            if self._read(ledger, subject) != {"CLAIMED": subject.to_bytes()}:
                raise CompositionAdmissionError("snapshot_capture_readback")
        if self.read(attempt) != (subject, {"CLAIMED": subject.to_bytes()}):
            raise CompositionAdmissionError("snapshot_capture_readback")
        return subject

    def record_capture(self, subject: SnapshotCaptureSubject, record: SnapshotCaptureRecord) -> None:
        self._append(subject, "CAPTURED", record.to_bytes())

    def record_generation(
        self,
        subject: SnapshotCaptureSubject,
        document: bytes,
        closed: CompositionAttemptProof,
        quiet: CompositionAttemptProof,
    ) -> None:
        self._append(subject, "GENERATION_SEALED", document, proofs=(closed, quiet))

    def _append(
        self,
        subject: SnapshotCaptureSubject,
        phase: str,
        document: bytes,
        *,
        proofs: tuple[CompositionAttemptProof, ...] = (),
    ) -> None:
        if len(document) > MAX_CAPTURE_METADATA_BYTES:
            raise CompositionAdmissionError("snapshot_capture_budget")
        with self._transaction() as ledger:
            self._authorize(ledger, subject, active=True)
            events = self._read(ledger, subject)
            if phase == "GENERATION_SEALED":
                self._require_closed(ledger, subject, proofs)
                from dpone.contracts.strict_json import strict_json_object

                body = strict_json_object(document)
                if (body.get("closed_gates_sha256"), body.get("quiescence_sha256")) != tuple(
                    p.proof_sha256 for p in proofs
                ):
                    raise CompositionAdmissionError("snapshot_capture_closure")
            if phase in events:
                if events[phase] != document:
                    raise CompositionAdmissionError("snapshot_capture_conflict")
            else:
                if CAPTURE_PHASES[len(events)] != phase:
                    raise CompositionAdmissionError("snapshot_capture_order")
                self._insert(ledger, subject, phase, document)
            expected = {**events, phase: document}
            if self._read(ledger, subject) != expected:
                raise CompositionAdmissionError("snapshot_capture_readback")
        if self.read(subject.attempt) != (subject, expected):
            raise CompositionAdmissionError("snapshot_capture_readback")

    @staticmethod
    def _insert(ledger: Any, subject: SnapshotCaptureSubject, phase: str, document: bytes) -> None:
        ledger.cursor.execute(
            f"INSERT INTO {ledger.table('snapshot_captures')} (operation_key,phase,subject_sha256,generation_uuid,document_sha256,document) VALUES (?,?,?,?,?,?);",
            subject.attempt.attempt_sha256,
            phase,
            subject.subject_sha256,
            subject.generation_uuid,
            capture_digest(document),
            document,
        )

    @staticmethod
    def _require_closed(
        ledger: Any, subject: SnapshotCaptureSubject, proofs: tuple[CompositionAttemptProof, ...]
    ) -> None:
        if tuple(p.kind for p in proofs) != ("CLOSED_GATES", "QUIESCENCE"):
            raise CompositionAdmissionError("snapshot_capture_closure")
        binding = ClickHouseGateBinding(subject.attempt, subject.target, "ingest")
        ledger.cursor.execute(
            f"SELECT TOP (2) LOWER(CONVERT(char(36),b.gate_id)) FROM {ledger.table('ch_gate_bindings')} b JOIN {ledger.table('ch_gate_events')} e ON b.gate_key=e.gate_key WHERE b.gate_key=? AND e.phase='CLOSED';",
            binding.key,
        )
        identities = tuple(tuple(row) for row in ledger.cursor.fetchall())
        if len(identities) != 1:
            raise CompositionAdmissionError("snapshot_capture_closure")
        for proof in proofs:
            proof.require_attempt(subject.attempt)
            if len(proof.authorities) != 1 or (
                proof.authorities[0].connector,
                proof.authorities[0].service_id,
                proof.authorities[0].principal_id,
            ) != ("clickhouse", subject.target.service_id, "clickhouse-user:" + identities[0][0]):
                raise CompositionAdmissionError("snapshot_capture_closure")
            ledger.cursor.execute(
                f"SELECT TOP (2) proof_document FROM {ledger.table('proofs')} WITH (HOLDLOCK) WHERE operation_key=? AND operation_family='execution' AND kind=? AND proof_sha256=?;",
                subject.attempt.attempt_sha256,
                proof.kind,
                proof.proof_sha256,
            )
            rows = tuple(tuple(row) for row in ledger.cursor.fetchall())
            if len(rows) != 1 or decode_attempt_proof(rows[0][0], proof.proof_sha256) != proof:
                raise CompositionAdmissionError("snapshot_capture_closure")


class ProtectedSnapshotFiles:
    """Exclusive root-owned source originals, with no following or replacement."""

    def __init__(self, root: Path) -> None:
        self.root = absolute_supervisor_path(root)

    def write_once(self, subject: SnapshotCaptureSubject, source: bytes, payload: bytes) -> None:
        require_supervisor()
        root_fd = open_protected(self.root, traversable=False)
        directory = subject.attempt.attempt_sha256[7:]
        try:
            os.mkdir(directory, 0o700, dir_fd=root_fd)
            os.fsync(root_fd)
            parent = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=root_fd)
            try:
                self._put(parent, "source.json", source)
                self._put(parent, "payload.native", payload)
            finally:
                os.close(parent)
        except OSError:
            raise CompositionAdmissionError("snapshot_capture_files") from None
        finally:
            os.close(root_fd)

    @staticmethod
    def _put(parent: int, name: str, document: bytes) -> None:
        temporary = ".capture-" + uuid4().hex
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o400, dir_fd=parent)
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(document)
                stream.flush()
                os.fsync(stream.fileno())
            # link is an atomic exclusive install, unlike replacing rename.
            os.link(temporary, name, src_dir_fd=parent, dst_dir_fd=parent, follow_symlinks=False)
            os.unlink(temporary, dir_fd=parent)
            os.fsync(parent)
        except BaseException:
            # Retain uncertainty/orphans; a second claimant is never permitted.
            raise

    def read(self, subject: SnapshotCaptureSubject, record: SnapshotCaptureRecord) -> tuple[bytes, bytes]:
        require_supervisor()
        source = self._read(subject, "source.json", subject.limits.max_source_bytes + subject.limits.max_rows + 65536)
        payload = self._read(subject, "payload.native", subject.limits.max_wire_bytes)
        if (
            capture_digest(source) != record.source_document_sha256
            or capture_digest(payload) != record.payload_sha256
            or len(payload) != record.wire_bytes
        ):
            raise CompositionAdmissionError("snapshot_capture_files_changed")
        return source, payload

    def _read(self, subject: SnapshotCaptureSubject, name: str, maximum: int) -> bytes:
        parent = open_protected(self.root / subject.attempt.attempt_sha256[7:], traversable=False)
        fd = None
        try:
            fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
            before = os.fstat(fd)
            if (
                not stat.S_ISREG(before.st_mode)
                or before.st_nlink != 1
                or before.st_uid != 0
                or before.st_mode & 0o222
                or not 0 <= before.st_size <= maximum
            ):
                raise CompositionAdmissionError("snapshot_capture_file_shape")
            chunks = []
            remaining = before.st_size
            while remaining:
                block = os.read(fd, min(remaining, 65536))
                if not block:
                    raise CompositionAdmissionError("snapshot_capture_file_changed")
                chunks.append(block)
                remaining -= len(block)
            after = os.fstat(fd)
            if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
                after.st_dev,
                after.st_ino,
                after.st_size,
                after.st_mtime_ns,
            ):
                raise CompositionAdmissionError("snapshot_capture_file_changed")
            return b"".join(chunks)
        except OSError:
            raise CompositionAdmissionError("snapshot_capture_files") from None
        finally:
            if fd is not None:
                os.close(fd)
            os.close(parent)
