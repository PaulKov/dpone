"""Build a full generation and exchange one UUID-identified table pair once."""

from __future__ import annotations

from collections.abc import Sequence

from dpone.adapters.window_metadata_files import load_metadata, save_metadata
from dpone.contracts.bounded_window import ChunkReceipt, PublicationStatus, WindowLease, WindowPlan
from dpone.contracts.process_errors import WindowContractError, WindowOutcomeUnknown
from dpone.runtime.sinks.clickhouse_window_staging import WindowIO, WindowStaging, identifier


class WindowPublication:
    """Durable UUID pair prevents an exchange retry from undoing publication."""

    def __init__(self, io: WindowIO, staging: WindowStaging) -> None:
        self.io, self.staging = io, staging

    def prepare(self, plan: WindowPlan, receipts: Sequence[ChunkReceipt], lease: WindowLease) -> str:
        io = self.io
        started = io.clock()
        if len(receipts) != len(plan.chunks) or {r.chunk_id for r in receipts} != {c.chunk_id for c in plan.chunks}:
            raise WindowContractError("Publication requires exactly one receipt per chunk")
        receipt_identity = sorted([r.chunk_id, r.attempt_id, r.row_count, r.digest] for r in receipts)
        generation = io.name(plan, "generation")
        with io.guard.hold(lease), io.connection() as connector:
            metadata = load_metadata(io.path(generation))
            if metadata is not None:
                if metadata.get("receipts") != receipt_identity:
                    raise WindowContractError("Prepared generation receipt identity mismatch")
                status = self.inspect(plan, generation)
                if status == PublicationStatus.PUBLISHED:
                    return generation
                if status != PublicationStatus.ABSENT:
                    raise WindowOutcomeUnknown("Generation identity is ambiguous")
                evidence = io.evidence(connector, generation)
                if evidence.digest() != metadata.get("digest"):
                    raise WindowContractError("Prepared generation parity changed")
                return generation
            # No publication record means no exchange was authorized. A leftover
            # private generation from interrupted preparation can be rebuilt.
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
                chunk_record = load_metadata(io.path(name))
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
            save_metadata(
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

    def inspect(self, plan: WindowPlan, generation: str) -> PublicationStatus:
        io = self.io
        if generation != io.name(plan, "generation"):
            raise WindowContractError("Generation does not belong to this run")
        metadata = load_metadata(io.path(generation))
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
            marker = load_metadata(pending)
            if marker is not None and marker.get("run_id") == plan.run_id:
                pending.unlink(missing_ok=True)
            return PublicationStatus.PUBLISHED
        if (target_uuid, generation_uuid) == (original, prepared):
            return PublicationStatus.ABSENT
        return PublicationStatus.UNKNOWN

    def publish(self, plan: WindowPlan, generation: str, lease: WindowLease) -> None:
        io = self.io
        with io.guard.hold(lease), io.connection() as connector:
            status = self.inspect(plan, generation)
            if status == PublicationStatus.PUBLISHED:
                return
            if status != PublicationStatus.ABSENT:
                raise WindowOutcomeUnknown("Exchange cannot be retried without observed UUID identity")
            metadata = load_metadata(io.path(generation))
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
            save_metadata(
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
                save_metadata(io.path(generation), metadata)
                # Keep both generations and metadata. Recovery must inspect UUIDs;
                # never infer that a transport error means the exchange failed.
                raise WindowOutcomeUnknown("Exchange outcome requires UUID inspection") from error
            metadata["publish_timing"] = {
                "status": "measured",
                "outcome": "acknowledged",
                "publish_attempt_seconds": max(0.0, io.clock() - exchange_started),
            }
            save_metadata(io.path(generation), metadata)
            if self.inspect(plan, generation) != PublicationStatus.PUBLISHED:
                raise WindowOutcomeUnknown("Exchange completion could not be verified")
