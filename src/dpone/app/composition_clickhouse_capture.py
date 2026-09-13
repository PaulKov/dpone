"""Actual one-shot source capture and independently reconciled generation producer.

Factories provide the verified source binding, protected SQL journal, root PVC,
read-only MSSQL reader and authenticated ClickHouse catalog. This service runs
those collaborators: it never consumes a deployment sidecar or caller hash as
source truth. A failed/uncertain claim, capture or CREATE is not retried here.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from dpone.app.composition_clickhouse_source import bounded_clickhouse_rows, clickhouse_source_row_bytes
from dpone.contracts.composition_clickhouse_dispatch import ClickHouseDispatchColumn
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.composition_persistence import CompositionAttemptIdentity, CompositionAttemptProof
from dpone.contracts.composition_snapshot import SnapshotGeneration
from dpone.contracts.composition_snapshot_capture import (
    SnapshotCaptureRecord,
    SnapshotCaptureSubject,
    capture_digest,
    decode_generation_seal,
    generation_document,
    snapshot_generation_from_observation,
)
from dpone.contracts.composition_snapshot_materialization import (
    CONTENT_VERSION,
    require_snapshot_materialization_pages,
    snapshot_content_sha256,
    snapshot_scalar,
)
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.runtime.clickhouse_native import ClickHouseNativeEncoder


@dataclass(frozen=True, slots=True)
class CapturedClickHouseSnapshot:
    """Fresh supervisor-owned capture; never a replay authorization token."""

    subject: SnapshotCaptureSubject
    record: SnapshotCaptureRecord
    rows: tuple[tuple[object, ...], ...]
    payload: bytes

    @property
    def columns(self) -> tuple[ClickHouseDispatchColumn, ...]:
        return tuple(ClickHouseDispatchColumn(*c) for c in self.record.columns)


class CompositionClickHouseCapture:
    """Claim → read one bounded snapshot → fsync originals → pin SQL → observe B."""

    def __init__(self, *, store: Any, files: Any, source_reader: Any, catalog: Any) -> None:
        self._store, self._files, self._source, self._catalog = store, files, source_reader, catalog

    def capture_once(self, attempt: CompositionAttemptIdentity) -> CapturedClickHouseSnapshot:
        subject = self._store.claim_once(attempt)
        if self._source.table_identity != subject.source_table or self._source.limits != subject.limits:
            raise CompositionAdmissionError("snapshot_capture_source_binding")
        columns, source_rows = self._source.read_snapshot()
        source_identity = self._source.source_identity_original
        if type(source_identity) is not bytes or not 1 <= len(source_identity) <= 16384:
            raise CompositionAdmissionError("snapshot_capture_source_identity")
        rows = tuple(bounded_clickhouse_rows(source_rows, columns, subject.limits))
        require_snapshot_materialization_pages(columns, rows)
        schema = tuple((c.name, c.type_name) for c in columns)
        # Source reader already performs exact whole-stream Native preflight;
        # this is the one actual encoding, performed before any target mutation.
        payload = b"".join(ClickHouseNativeEncoder(schema, target_schema=schema).iter_batches(rows))
        if len(payload) > subject.limits.max_wire_bytes:
            raise CompositionAdmissionError("snapshot_capture_wire_budget")
        observed = self._catalog.observe_capture(subject, columns)
        if (
            observed.target != subject.target
            or observed.target_uuid is None
            or observed.generation_uuid is not None
            or observed.database_engine != "Atomic"
            or observed.table_engines != ("MergeTree", None)
            or observed.unsupported_features
            or observed.schema_sha256[0] is None
            or observed.physical_sha256[0] is None
            or observed.old_target_bytes is None
            or observed.retained_bytes is None
            or (observed.node_count, observed.replica_count) != (1, 1)
        ):
            raise CompositionAdmissionError("snapshot_capture_namespace")
        source_bytes = 0
        for row in rows:
            source_bytes += clickhouse_source_row_bytes(
                row, columns, remaining_bytes=subject.limits.max_source_bytes - source_bytes
            )
        source = canonical_json_bytes(
            {
                "schema": "dpone.composition-snapshot-source.v1",
                "subject_sha256": subject.subject_sha256,
                "content_version": CONTENT_VERSION,
                "columns": schema,
                "rows": [[snapshot_scalar(v, c.type_name) for v, c in zip(row, columns, strict=True)] for row in rows],
                "payload_sha256": capture_digest(payload),
                "source_identity_original": source_identity.hex(),
            }
        )
        record = SnapshotCaptureRecord(
            subject.subject_sha256,
            capture_digest(source),
            capture_digest(payload),
            snapshot_content_sha256(
                columns, rows, max_rows=subject.limits.max_rows, max_bytes=subject.limits.max_source_bytes
            ),
            observed.schema_sha256[0],
            observed.physical_sha256[0],
            observed.catalog_evidence_sha256,
            observed.target_uuid,
            schema,
            len(rows),
            source_bytes,
            len(payload),
            observed.old_target_bytes,
            observed.retained_bytes,
        )
        self._files.write_once(subject, source, payload)
        if self._files.read(subject, record) != (source, payload):
            raise CompositionAdmissionError("snapshot_capture_file_readback")
        self._store.record_capture(subject, record)
        if self._files.read(subject, record) != (source, payload):
            raise CompositionAdmissionError("snapshot_capture_file_readback")
        return CapturedClickHouseSnapshot(subject, record, rows, payload)

    def finalize(
        self, captured: CapturedClickHouseSnapshot, closed: CompositionAttemptProof, quiet: CompositionAttemptProof
    ) -> SnapshotGeneration:
        subject, record = captured.subject, captured.record
        self._files.read(subject, record)
        for proof, kind in ((closed, "CLOSED_GATES"), (quiet, "QUIESCENCE")):
            proof.require_attempt(subject.attempt)
            if proof.kind != kind:
                raise CompositionAdmissionError("snapshot_capture_closure")
        observed = self._catalog.observe_capture(subject, captured.columns, old_target_uuid=record.old_target_uuid)
        generation = snapshot_generation_from_observation(subject, record, observed)
        document = canonical_json_bytes(
            {
                "schema": "dpone.composition-snapshot-generation-seal.v1",
                "generation": strict_json_object(generation_document(generation)),
                "generation_sha256": generation.record_sha256,
                "closed_gates_sha256": closed.proof_sha256,
                "quiescence_sha256": quiet.proof_sha256,
                "observation": asdict(observed),
            }
        )
        self._store.record_generation(subject, document, closed, quiet)
        return self.load_generation(subject.attempt, generation.record_sha256)

    def load_generation(self, attempt: CompositionAttemptIdentity, generation_ref: str) -> SnapshotGeneration:
        subject, events = self._store.read(attempt)
        return self._load_originals(subject, events, generation_ref)

    def load_generation_in(
        self, ledger: Any, attempt: CompositionAttemptIdentity, generation_ref: str
    ) -> SnapshotGeneration:
        """Reopen protected originals without reacquiring the caller's SQL lock."""
        subject, events = self._store.read_in(ledger, attempt)
        return self._load_originals(subject, events, generation_ref)

    def _load_originals(
        self, subject: SnapshotCaptureSubject, events: dict[str, bytes], generation_ref: str
    ) -> SnapshotGeneration:
        try:
            record = SnapshotCaptureRecord.from_bytes(events["CAPTURED"])
            self._files.read(subject, record)
            generation = decode_generation_seal(subject, record, events["GENERATION_SEALED"])
            if generation.record_sha256 != generation_ref:
                raise ValueError
            return generation
        except (KeyError, TypeError, ValueError):
            raise CompositionAdmissionError("snapshot_capture_generation_unavailable") from None
