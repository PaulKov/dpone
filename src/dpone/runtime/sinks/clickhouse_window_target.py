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
import re
from collections.abc import Callable, Iterator, Sequence
from pathlib import Path
from time import monotonic

from dpone.adapters.window_metadata_files import load_metadata
from dpone.contracts.bounded_window import ChunkReceipt, PublicationStatus, WindowChunk, WindowLease, WindowPlan
from dpone.contracts.process_errors import WindowContractError
from dpone.ports.clickhouse_window_ingest import WindowBinaryIngest
from dpone.ports.window_exclusion import ExclusiveWindowWriterGuard
from dpone.runtime.sinks.clickhouse_window_publication import WindowPublication
from dpone.runtime.sinks.clickhouse_window_staging import WindowConnector, WindowIO, WindowStaging, identifier, literal


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
        max_encoded_bytes: int,
        writer_guard: ExclusiveWindowWriterGuard | None,
        clock: Callable[[], float] = monotonic,
    ) -> None:
        for name in (database, table, window_column, *(name for name, _ in schema)):
            identifier(name)
        if not schema or len({name for name, _ in schema}) != len(schema):
            raise WindowContractError("Schema must contain unique columns")
        window_type = dict(schema).get(window_column, "")
        if not re.fullmatch(r"(?:Nullable\()?DateTime64\([0-6],\s*'UTC'\)\)?", window_type):
            raise WindowContractError("Window column must be UTC DateTime64 with precision at most six")
        if isinstance(max_encoded_bytes, bool) or not isinstance(max_encoded_bytes, int) or max_encoded_bytes <= 0:
            raise WindowContractError("max_encoded_bytes must be a positive integer")
        if not target_id:
            raise WindowContractError("Physical target identity is required")
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
        )
        self.staging = WindowStaging(self.io)
        self.publication = WindowPublication(self.io, self.staging)

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
        pending = load_metadata(io.path(io.name_for_target()))
        if pending is not None and pending.get("run_id") != plan.run_id:
            raise WindowContractError("A different run has unresolved publication; reconcile its UUIDs first")
        with io.connection() as connector:
            database = connector.get_records(f"SELECT engine FROM system.databases WHERE name = {literal(io.database)}")
            if database != [("Atomic",)]:
                raise WindowContractError("Window publication requires a local Atomic database")
            rows = connector.get_records(
                f"SELECT engine, create_table_query, dependencies_database, dependencies_table FROM system.tables WHERE database = {literal(io.database)} AND name = {literal(io.table)}"
            )
            if len(rows) != 1 or rows[0][0] != "MergeTree" or rows[0][2] or rows[0][3]:
                raise WindowContractError("Window target requires plain MergeTree without dependencies")
            if re.search(r"\b(TTL|PROJECTION)\b", str(rows[0][1]), re.IGNORECASE):
                raise WindowContractError("TTL and projections are unsupported for window publication")
            columns = connector.get_records(
                f"SELECT name, type, default_kind FROM system.columns WHERE database = {literal(io.database)} AND table = {literal(io.table)} ORDER BY position"
            )
            if tuple((str(row[0]), str(row[1])) for row in columns) != io.schema or any(row[2] for row in columns):
                raise WindowContractError("Physical schema differs or contains computed columns")
            mutations = connector.get_records(
                f"SELECT count() FROM system.mutations WHERE database = {literal(io.database)} AND table = {literal(io.table)} AND NOT is_done"
            )
            if mutations != [(0,)]:
                raise WindowContractError("Target has active mutations")
            policies = connector.get_records(
                f"SELECT count() FROM system.row_policies WHERE database IN ({literal(io.database)}, '*') "
                f"AND table IN ({literal(io.table)}, '*')"
            )
            if policies != [(0,)]:
                raise WindowContractError("Row policies may hide target data; window publication is unsupported")
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

    def prepare(self, plan: WindowPlan, receipts: Sequence[ChunkReceipt], lease: WindowLease) -> str:
        self._identity(plan, lease)
        return self.publication.prepare(plan, receipts, lease)

    def publish(self, plan: WindowPlan, generation: str, lease: WindowLease) -> None:
        self._identity(plan, lease)
        self.publication.publish(plan, generation, lease)

    def inspect_publication(self, plan: WindowPlan, generation: str) -> PublicationStatus:
        self._identity(plan)
        return self.publication.inspect(plan, generation)

    def generation_total(self, plan: WindowPlan, generation: str) -> int:
        """Return the durable verified generation count without post-commit SQL.

        This is verified preparation evidence, not a fresh target row count.
        """
        self._identity(plan)
        if generation != self.io.name(plan, "generation"):
            raise WindowContractError("Generation does not belong to this run")
        metadata = load_metadata(self.io.path(generation))
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
        metadata = load_metadata(self.io.path(generation))
        evidence = metadata.get("evidence") if metadata else None
        if metadata is None or not isinstance(evidence, dict):
            raise WindowContractError("Verified generation aggregate evidence is unavailable")
        evidence["publish_timing"] = metadata.get(
            "publish_timing", {"status": "unavailable", "reason": "not_recorded_or_process_loss"}
        )
        return evidence
