"""Opt-in bounded RowBinary staging with atomic local MergeTree publication.

The composition root supplies independent connectors/runners and a real backend
ALL-writer exclusion authority. Supported targets are plain MergeTree tables in
local Atomic databases, without TTL, dependencies, defaults, aliases, materialized
columns, row policies, mutations or projections. This deliberately fails closed for distributed
and replicated engines. Receipt storage must survive process restart and remain
accessible only to the authority owning this target.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Iterator, Sequence
from pathlib import Path
from time import monotonic

from dpone.contracts.bounded_window import (
    ChunkReceipt,
    PublicationStatus,
    WindowChunk,
    WindowContractError,
    WindowLease,
    WindowOutcomeUnknown,
    WindowPlan,
)
from dpone.ports.bounded_window import ExclusiveWindowWriterGuard, WindowBinaryIngest, WindowMetadataStore
from dpone.runtime.sinks.clickhouse_window_admission import validate_configuration, validate_target
from dpone.runtime.sinks.clickhouse_window_staging import WindowConnector, WindowIO, WindowStaging, identifier


def window_schema_fingerprint(schema: Sequence[tuple[str, str]]) -> str:
    """Canonical v1 hash of ordered physical ClickHouse names and type strings."""
    return hashlib.sha256(json.dumps([1, list(schema)], separators=(",", ":")).encode()).hexdigest()


class ClickHouseWindowTarget:
    """WindowTarget implementation; existing sink defaults remain unchanged.

    ``target_id`` is the composition root's sanitized endpoint/database/table
    identity. Its connector factory must construct a new connector for that exact
    endpoint per call. ``schema`` uses native ClickHouse types and is checked
    against both the frozen plan fingerprint and physical ordered columns.
    ``max_encoded_bytes`` caps individual encoded rows and emitted HTTP chunks;
    driver buffers and source objects are separate resource budgets.
    """

    def __init__(
        self,
        *,
        schema: Sequence[tuple[str, str]],
        database: str,
        table: str,
        window_column: str,
        target_id: str,
        connector_factory: Callable[[], WindowConnector],
        http_runner_factory: Callable[[], WindowBinaryIngest],
        work_dir: Path,
        metadata_store: WindowMetadataStore,
        max_encoded_bytes: int,
        writer_guard: ExclusiveWindowWriterGuard | None,
        clock: Callable[[], float] = monotonic,
    ) -> None:
        validate_configuration(
            schema,
            database=database,
            table=table,
            window_column=window_column,
            target_id=target_id,
            max_encoded_bytes=max_encoded_bytes,
        )
        self.target_id = target_id
        self.io = WindowIO(
            tuple(schema),
            database,
            table,
            window_column,
            connector_factory,
            http_runner_factory,
            Path(work_dir),
            max_encoded_bytes,
            writer_guard,
            window_schema_fingerprint(schema),
            f"{database}.{table}",
            clock,
            metadata_store,
        )
        self.staging = WindowStaging(self.io)

    def _identity(self, plan: WindowPlan, lease: WindowLease | None = None) -> None:
        if (
            plan.target_id != self.target_id
            or plan.schema_fingerprint != self.io.schema_fingerprint
            or plan.window_column != self.io.window_column
        ):
            raise WindowContractError("Plan physical target or schema identity mismatch")
        if lease is not None and lease.target_id != self.target_id:
            raise WindowContractError("Lease physical target identity mismatch")

    def validate(self, plan: WindowPlan) -> None:
        """Read-only capability preflight before any staging or source reads."""
        io = self.io
        guard = io.guard
        self._identity(plan)
        guard.validate(plan.target_id, io.physical_target)
        pending = io.metadata_store.load(io.path(io.name_for_target()))
        if pending is not None and pending.get("run_id") != plan.run_id:
            raise WindowContractError("A different run has unresolved publication; reconcile its UUIDs first")
        validate_target(io)
        io.encoder()

    def stage(
        self,
        plan: WindowPlan,
        chunk: WindowChunk,
        attempt_id: str,
        rows: Iterator[tuple[object, ...]],
        lease: WindowLease,
    ) -> ChunkReceipt:
        self._identity(plan, lease)
        try:
            return self.staging.stage(plan, chunk, attempt_id, rows, lease)
        finally:
            close = getattr(rows, "close", None)
            if close:
                close()

    def inspect_attempt(
        self, plan: WindowPlan, chunk: WindowChunk, attempt_id: str, lease: WindowLease
    ) -> ChunkReceipt | None:
        self._identity(plan, lease)
        with self.io.guard.hold(lease):
            return self.staging.inspect(plan, chunk, attempt_id, lease)

    def discard_attempt(self, plan: WindowPlan, chunk: WindowChunk, attempt_id: str, lease: WindowLease) -> None:
        self._identity(plan, lease)
        self.staging.discard(plan, chunk, attempt_id, lease)

    def generation_total(self, plan: WindowPlan, generation: str) -> int:
        """Return the durable verified generation count without post-commit SQL.

        This is verified preparation evidence, not a fresh target row count.
        """
        self._identity(plan)
        if generation != self.io.name(plan, "generation"):
            raise WindowContractError("Generation does not belong to this run")
        metadata = self.io.metadata_store.load(self.io.path(generation))
        if metadata is None or metadata.get("identity") != [
            plan.run_id,
            self.io.schema_fingerprint,
            self.io.physical_target,
        ]:
            raise WindowContractError("Verified generation metadata is unavailable")
        count = metadata.get("count")
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise WindowContractError("Verified generation count is invalid")
        return count

    def generation_evidence(self, plan: WindowPlan, generation: str) -> dict[str, object]:
        """Read durable prepublication aggregates; no postcommit source or SQL reads.

        Per-day/count/NULL/bounds metrics describe the replacement window only;
        target_total_rows includes preserved outside-window and NULL-window rows.
        Source metric equality is inferred from verified probabilistic typed parity.
        """
        self.generation_total(plan, generation)
        metadata = self.io.metadata_store.load(self.io.path(generation))
        evidence = metadata.get("evidence") if metadata else None
        if metadata is None or not isinstance(evidence, dict):
            raise WindowContractError("Verified generation aggregate evidence is unavailable")
        evidence["publish_timing"] = metadata.get(
            "publish_timing", {"status": "unavailable", "reason": "not_recorded_or_process_loss"}
        )
        return evidence

    def prepare(self, plan: WindowPlan, receipts: Sequence[ChunkReceipt], lease: WindowLease) -> str:
        self._identity(plan, lease)
        io = self.io
        started = io.clock()
        if len(receipts) != len(plan.chunks) or {r.chunk_id for r in receipts} != {c.chunk_id for c in plan.chunks}:
            raise WindowContractError("Publication requires exactly one receipt per chunk")
        receipt_identity = sorted([r.chunk_id, r.attempt_id, r.row_count, r.digest] for r in receipts)
        generation = io.name(plan, "generation")
        with io.guard.hold(lease), io.connection() as connector:
            metadata = io.metadata_store.load(io.path(generation))
            if metadata is not None:
                if metadata.get("receipts") != receipt_identity:
                    raise WindowContractError("Prepared generation receipt identity mismatch")
                status = self._inspect_publication(plan, generation)
                if status == PublicationStatus.PUBLISHED:
                    return generation
                if status != PublicationStatus.ABSENT:
                    raise WindowOutcomeUnknown("Generation identity is ambiguous")
                validate_target(io)
                evidence = io.evidence(connector, generation)
                if evidence.digest() != metadata.get("digest"):
                    raise WindowContractError("Prepared generation parity changed")
                return generation
            # No publication record means no exchange was authorized. A leftover
            # private generation from interrupted preparation can be rebuilt.
            validate_target(io)
            io.guard.fence_attempt(lease, generation)
            io.mutate(connector, f"DROP TABLE IF EXISTS {io.qualified(generation)} SYNC", lease)
            original_uuid = io.uuid(connector, io.table)
            if original_uuid is None:
                raise WindowContractError("Target disappeared")
            io.mutate(connector, f"CREATE TABLE {io.qualified(generation)} AS {io.qualified(io.table)}", lease)
            outside = f"({identifier(io.window_column)} IS NULL OR NOT {io.predicate(plan.start, plan.end)})"
            original_target = io.evidence(connector, io.table)
            original = io.evidence(connector, io.table, predicate=outside)
            io.mutate(
                connector,
                f"INSERT INTO {io.qualified(generation)} ({io.columns}) SELECT {io.columns} FROM {io.qualified(io.table)} WHERE {outside}",
                lease,
            )
            if io.evidence(connector, generation).digest() != original.digest():
                raise WindowContractError("Outside-window preservation failed")
            expected_total = original.total
            expected_count = original.count
            expected_nulls = list(original.nulls)
            chunk_metrics = []
            total_encoded_bytes = 0
            by_chunk = {receipt.chunk_id: receipt for receipt in receipts}
            for chunk in plan.chunks:
                receipt = by_chunk[chunk.chunk_id]
                if self.staging.inspect(plan, chunk, receipt.attempt_id, lease) != receipt:
                    raise WindowContractError("Unverified chunk receipt")
                name = self.staging.name(plan, chunk, receipt.attempt_id)
                evidence = io.evidence(connector, name, (chunk.start, chunk.end))
                chunk_record = io.metadata_store.load(io.path(name))
                if chunk_record is None:
                    raise WindowContractError("Chunk aggregate evidence is missing")
                chunk_metrics.append(
                    {
                        "chunk_id": chunk.chunk_id,
                        "attempt_id": receipt.attempt_id,
                        "start": chunk.start.isoformat(),
                        "end": chunk.end.isoformat(),
                        "digest": receipt.digest,
                        "source_row_count": receipt.row_count,
                        "encoded_bytes": chunk_record.get("encoded_bytes"),
                        "stage_seconds": chunk_record.get("stage_seconds"),
                        "target": chunk_record.get("metrics"),
                    }
                )
                total_encoded_bytes += evidence.encoded_bytes
                expected_total = (expected_total + evidence.total) % (1 << 256)
                expected_count += evidence.count
                expected_nulls = [a + b for a, b in zip(expected_nulls, evidence.nulls, strict=True)]
                io.mutate(
                    connector,
                    f"INSERT INTO {io.qualified(generation)} ({io.columns}) SELECT {io.columns} FROM {io.qualified(name)}",
                    lease,
                )
            observed = io.evidence(connector, generation)
            if (observed.count, observed.total, observed.nulls) != (expected_count, expected_total, expected_nulls):
                raise WindowContractError("Full generation parity failed")
            window_metrics = io.metrics(connector, generation, plan.start, plan.end, window_only=True)
            if window_metrics["row_count"] != sum(r.row_count for r in receipts):
                raise WindowContractError("Generation window count differs from staged source")
            io.metadata_store.save(
                io.path(generation),
                {
                    "version": 1,
                    "identity": [plan.run_id, io.schema_fingerprint, io.physical_target],
                    "original_uuid": original_uuid,
                    "original_digest": original_target.digest(),
                    "generation_uuid": io.uuid(connector, generation),
                    "digest": observed.digest(),
                    "count": observed.count,
                    "receipts": receipt_identity,
                    "evidence": {
                        "schema_version": "dpone.window_target_evidence.v1",
                        "run_id": plan.run_id,
                        "schema_fingerprint": io.schema_fingerprint,
                        "target_id": plan.target_id,
                        "window_column": plan.window_column,
                        "window_start": plan.start.isoformat(),
                        "window_end": plan.end.isoformat(),
                        "source_window_rows": sum(r.row_count for r in receipts),
                        "target_total_rows": observed.count,
                        "target_window": window_metrics,
                        "chunks": chunk_metrics,
                        "source_metric_parity": "inferred_from_verified_probabilistic_typed_multiset",
                        "prepare_seconds": max(0.0, io.clock() - started),
                        "encoded_bytes": total_encoded_bytes,
                        "digest_algorithm": "rowbinary-sha256-sum-v1",
                        "exact_outside_window_check": "required_before_exchange",
                    },
                },
            )
        return generation

    def inspect_publication(self, plan: WindowPlan, generation: str, lease: WindowLease) -> PublicationStatus:
        """Reconcile and settle markers under current target writer authority."""
        self._identity(plan, lease)
        with self.io.guard.hold(lease):
            return self._inspect_publication(plan, generation)

    def _inspect_publication(self, plan: WindowPlan, generation: str) -> PublicationStatus:
        self._identity(plan)
        io = self.io
        if generation != io.name(plan, "generation"):
            raise WindowContractError("Generation does not belong to this run")
        metadata = io.metadata_store.load(io.path(generation))
        if metadata is None:
            return PublicationStatus.UNKNOWN
        if metadata.get("identity") != [plan.run_id, io.schema_fingerprint, io.physical_target]:
            raise WindowContractError("Generation metadata identity mismatch")
        with io.connection() as connector:
            target_uuid, generation_uuid = io.uuid(connector, io.table), io.uuid(connector, generation)
        original, prepared = metadata.get("original_uuid"), metadata.get("generation_uuid")
        if not original or not prepared or original == prepared:
            return PublicationStatus.UNKNOWN
        if (target_uuid, generation_uuid) == (prepared, original):
            pending = io.path(io.name_for_target())
            marker = io.metadata_store.load(pending)
            if marker is not None and marker.get("run_id") == plan.run_id:
                io.metadata_store.remove(pending)
            return PublicationStatus.PUBLISHED
        if (target_uuid, generation_uuid) == (original, prepared):
            return PublicationStatus.ABSENT
        return PublicationStatus.UNKNOWN

    def publish(self, plan: WindowPlan, generation: str, lease: WindowLease) -> None:
        self._identity(plan, lease)
        io = self.io
        with io.guard.hold(lease), io.connection() as connector:
            status = self._inspect_publication(plan, generation)
            if status == PublicationStatus.PUBLISHED:
                return
            if status != PublicationStatus.ABSENT:
                raise WindowOutcomeUnknown("Exchange cannot be retried without observed UUID identity")
            validate_target(io)
            metadata = io.metadata_store.load(io.path(generation))
            if metadata is None or io.evidence(connector, generation).digest() != metadata.get("digest"):
                raise WindowContractError("Generation changed before publication")
            if io.evidence(connector, io.table).digest() != metadata.get("original_digest"):
                raise WindowContractError("Target changed after generation preparation")
            outside = f"({identifier(io.window_column)} IS NULL OR NOT {io.predicate(plan.start, plan.end)})"
            delta = "__dpone_balance"
            while delta in {name for name, _ in io.schema}:
                delta += "_"
            delta_sql = identifier(delta)
            # Exact server-side row multiplicity comparison protects preserved
            # data, independently of the probabilistic typed hash evidence.
            mismatch = connector.get_records(
                f"SELECT count() FROM (SELECT {io.columns} FROM ("
                f"SELECT {io.columns}, toInt64(1) AS {delta_sql} FROM {io.qualified(io.table)} WHERE {outside} "
                f"UNION ALL SELECT {io.columns}, toInt64(-1) AS {delta_sql} FROM {io.qualified(generation)} WHERE {outside}"
                f") GROUP BY {io.columns} HAVING sum({delta_sql}) != 0)"
            )
            if mismatch != [(0,)]:
                raise WindowContractError("Outside-window target rows changed after preparation")
            metadata["evidence"]["exact_outside_window_check"] = "passed"
            io.metadata_store.save(
                io.path(io.name_for_target()), {"version": 1, "run_id": plan.run_id, "generation": generation}
            )
            exchange_started = io.clock()
            try:
                io.mutate(connector, f"EXCHANGE TABLES {io.qualified(io.table)} AND {io.qualified(generation)}", lease)
            except Exception as error:
                metadata["publish_timing"] = {
                    "status": "measured",
                    "outcome": "unknown",
                    "publish_attempt_seconds": max(0.0, io.clock() - exchange_started),
                }
                io.metadata_store.save(io.path(generation), metadata)
                # Keep both generations and metadata. Recovery must inspect UUIDs;
                # never infer that a transport error means the exchange failed.
                raise WindowOutcomeUnknown("Exchange outcome requires UUID inspection") from error
            metadata["publish_timing"] = {
                "status": "measured",
                "outcome": "acknowledged",
                "publish_attempt_seconds": max(0.0, io.clock() - exchange_started),
            }
            io.metadata_store.save(io.path(generation), metadata)
            if self._inspect_publication(plan, generation) != PublicationStatus.PUBLISHED:
                raise WindowOutcomeUnknown("Exchange completion could not be verified")
