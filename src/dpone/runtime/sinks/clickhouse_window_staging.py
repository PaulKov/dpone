"""Isolated ClickHouse staging and independently read-back verification."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Generator, Iterable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

from dpone.contracts.bounded_window import (
    ChunkReceipt,
    WindowChunk,
    WindowContractError,
    WindowLease,
    WindowPlan,
    WindowTransientError,
)
from dpone.ports.bounded_window import ExclusiveWindowWriterGuard, WindowBinaryIngest, WindowMetadataStore
from dpone.runtime.clickhouse_rowbinary import ClickHouseRowBinaryEncoder
from dpone.runtime.sinks.clickhouse_window_evidence import TypedMultiset
from dpone.runtime.sinks.clickhouse_window_queries import WindowQueryReader, read_window_metrics


class WindowConnector(WindowQueryReader, Protocol):
    """Fresh per-operation ClickHouse connection with streaming readback."""

    def execute_query(self, query: str) -> Any: ...
    def close(self) -> None: ...


def identifier(value: str) -> str:
    """Restrict configured names, also protecting existing HTTP identifier quoting."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise WindowContractError("Window target requires simple SQL identifiers")
    return f"`{value}`"


def literal(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def window_schema_fingerprint(schema: Sequence[tuple[str, str]]) -> str:
    """Canonical v1 hash of ordered physical ClickHouse names and type strings."""
    return hashlib.sha256(json.dumps([1, list(schema)], separators=(",", ":")).encode()).hexdigest()


def validate_target(io: WindowIO) -> None:
    """Reject topology or schema capabilities that cannot preserve visible rows.

    The owning target checks plan identity and writer authority first. This
    inspection performs no mutations.
    """
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


@dataclass(frozen=True)
class WindowIO:
    """Injected resources and immutable identity shared by staging/publication."""

    schema: tuple[tuple[str, str], ...]
    database: str
    table: str
    window_column: str
    connector_factory: Callable[[], WindowConnector]
    http_runner_factory: Callable[[], WindowBinaryIngest]
    work_dir: Path
    max_encoded_bytes: int
    writer_guard: ExclusiveWindowWriterGuard | None
    schema_fingerprint: str
    physical_target: str
    clock: Callable[[], float]
    metadata_store: WindowMetadataStore

    @property
    def guard(self) -> ExclusiveWindowWriterGuard:
        if self.writer_guard is None:
            raise WindowContractError("An exclusive ALL-writer authority is required")
        return self.writer_guard

    @contextmanager
    def connection(self) -> Iterator[WindowConnector]:
        connector = self.connector_factory()
        try:
            yield connector
        finally:
            connector.close()

    def qualified(self, table: str) -> str:
        return f"{identifier(self.database)}.{identifier(table)}"

    @property
    def columns(self) -> str:
        return ", ".join(identifier(name) for name, _ in self.schema)

    def name(self, plan: WindowPlan, suffix: str) -> str:
        return "__dpone_window_" + hashlib.sha256((plan.run_id + suffix).encode()).hexdigest()

    def name_for_target(self) -> str:
        return "pending_" + hashlib.sha256(self.physical_target.encode()).hexdigest()

    def path(self, name: str) -> Path:
        return self.work_dir / f"{name}.json"

    def generation_total(self, plan: WindowPlan, generation: str) -> int:
        """Read verified preparation count after the target validates plan identity."""
        if generation != self.name(plan, "generation"):
            raise WindowContractError("Generation does not belong to this run")
        metadata = self.metadata_store.load(self.path(generation))
        if metadata is None or metadata.get("identity") != [
            plan.run_id,
            self.schema_fingerprint,
            self.physical_target,
        ]:
            raise WindowContractError("Verified generation metadata is unavailable")
        count = metadata.get("count")
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise WindowContractError("Verified generation count is invalid")
        return count

    def generation_evidence(self, plan: WindowPlan, generation: str) -> dict[str, object]:
        """Read stored aggregates after the target's generation_total validation.

        This second metadata read preserves existing recovery behavior; neither
        method inspects current target rows or determines publication status.
        """
        metadata = self.metadata_store.load(self.path(generation))
        evidence = metadata.get("evidence") if metadata else None
        if metadata is None or not isinstance(evidence, dict):
            raise WindowContractError("Verified generation aggregate evidence is unavailable")
        evidence["publish_timing"] = metadata.get(
            "publish_timing", {"status": "unavailable", "reason": "not_recorded_or_process_loss"}
        )
        return evidence

    def encoder(self) -> ClickHouseRowBinaryEncoder:
        return ClickHouseRowBinaryEncoder(
            self.schema,
            schema_kind="clickhouse",
            chunk_rows=1,
            max_batch_bytes=self.max_encoded_bytes,
            max_row_bytes=self.max_encoded_bytes,
        )

    def uuid(self, connector: WindowConnector, name: str) -> str | None:
        records = connector.get_records(
            f"SELECT toString(uuid) FROM system.tables WHERE database = {literal(self.database)} AND name = {literal(name)}"
        )
        return str(records[0][0]) if records else None

    def mutate(self, connector: WindowConnector, query: str, lease: WindowLease) -> None:
        self.guard.assert_lease(lease)
        connector.execute_query(query)
        self.guard.assert_lease(lease)

    def predicate(self, start: datetime, end: datetime) -> str:
        def timestamp(value: datetime) -> str:
            normalized = value.astimezone(timezone.utc)  # noqa: UP017
            return "toDateTime64(" + literal(normalized.strftime("%Y-%m-%d %H:%M:%S.%f")) + ", 6, 'UTC')"

        column = identifier(self.window_column)
        return f"({column} >= {timestamp(start)} AND {column} < {timestamp(end)})"

    def evidence(
        self,
        connector: WindowConnector,
        table: str,
        bounds: tuple[datetime, datetime] | None = None,
        predicate: str | None = None,
    ) -> TypedMultiset:
        evidence = TypedMultiset(len(self.schema))
        query = f"SELECT {self.columns} FROM {self.qualified(table)}"
        if predicate:
            query += " WHERE " + predicate
        rows = connector.get_records_iterator(query)
        try:
            for _ in encoded_rows(
                rows,
                self.encoder().iter_batches,
                evidence,
                [name for name, _ in self.schema].index(self.window_column),
                bounds,
            ):
                pass
        finally:
            close = getattr(rows, "close", None)
            if close:
                close()
        return evidence

    def metrics(
        self, connector: WindowConnector, table: str, start: datetime, end: datetime, *, window_only: bool = False
    ) -> dict[str, Any]:
        """Read server aggregates through the read-only query capability."""
        column = identifier(self.window_column)
        predicate = self.predicate(start, end)
        columns = tuple((name, identifier(name)) for name, _ in self.schema)
        return read_window_metrics(
            connector,
            table_sql=self.qualified(table),
            column_sql=column,
            predicate_sql=predicate,
            columns=columns,
            window_only=window_only,
        )


class WindowStaging:
    """Own attempt tables; receipts become durable only after server readback."""

    def __init__(self, io: WindowIO) -> None:
        self.io = io

    def name(self, plan: WindowPlan, chunk: WindowChunk, attempt: str) -> str:
        if chunk not in plan.chunks or not attempt:
            raise WindowContractError("Invalid chunk attempt identity")
        return self.io.name(plan, chunk.chunk_id + attempt)

    def inspect(self, plan: WindowPlan, chunk: WindowChunk, attempt: str, lease: WindowLease) -> ChunkReceipt | None:
        io = self.io
        io.guard.assert_lease(lease)
        name = self.name(plan, chunk, attempt)
        metadata = io.metadata_store.load(io.path(name))
        if metadata is None:
            return None
        if metadata.get("identity") != [
            plan.run_id,
            chunk.chunk_id,
            attempt,
            io.schema_fingerprint,
            io.physical_target,
        ]:
            raise WindowContractError("Attempt receipt identity mismatch")
        with io.connection() as connector:
            if io.uuid(connector, name) != metadata.get("uuid"):
                raise WindowContractError("Attempt staging UUID changed")
            evidence = io.evidence(connector, name, (chunk.start, chunk.end))
        if evidence.count != metadata.get("count") or evidence.digest() != metadata.get("digest"):
            raise WindowContractError("Attempt staging parity changed")
        return ChunkReceipt(chunk.chunk_id, attempt, evidence.count, evidence.digest())

    def stage(
        self, plan: WindowPlan, chunk: WindowChunk, attempt: str, rows: Iterator[tuple[object, ...]], lease: WindowLease
    ) -> ChunkReceipt:
        io = self.io
        name = self.name(plan, chunk, attempt)
        started = io.clock()
        with io.guard.hold(lease), io.connection() as connector:
            existing = self.inspect(plan, chunk, attempt, lease)
            if existing:
                return existing
            if io.uuid(connector, name) is not None:
                raise WindowContractError("Unverified attempt exists; fence and discard before retry")
            io.mutate(connector, f"CREATE TABLE {io.qualified(name)} AS {io.qualified(io.table)}", lease)
            evidence = TypedMultiset(len(io.schema))
            stream = encoded_rows(
                rows,
                io.encoder().iter_batches,
                evidence,
                [name for name, _ in io.schema].index(io.window_column),
                (chunk.start, chunk.end),
            )
            runner = io.http_runner_factory()
            io.guard.assert_lease(lease)
            batches = coalesce_encoded_rows(stream, io.max_encoded_bytes)
            try:
                runner.insert_window_stream(
                    io.database, io.qualified(name), [column for column, _ in io.schema], batches, name
                )
            except (ConnectionError, TimeoutError) as error:
                raise WindowTransientError("HTTP stage connection failed; inspect and fence before retry") from error
            finally:
                batches.close()
            io.guard.assert_lease(lease)
            observed = io.evidence(connector, name, (chunk.start, chunk.end))
            if observed.count != evidence.count or observed.digest() != evidence.digest():
                raise WindowContractError("Typed staging multiset parity failed")
            metrics = io.metrics(connector, name, chunk.start, chunk.end)
            if metrics["row_count"] != evidence.count or metrics["outside_window"] != 0:
                raise WindowContractError("Staging aggregate metrics differ from verified stream")
            metadata = {
                "version": 1,
                "identity": [plan.run_id, chunk.chunk_id, attempt, io.schema_fingerprint, io.physical_target],
                "uuid": io.uuid(connector, name),
                "count": evidence.count,
                "digest": evidence.digest(),
                "metrics": metrics,
                "source_row_count": evidence.count,
                "encoded_bytes": evidence.encoded_bytes,
                "stage_seconds": max(0.0, io.clock() - started),
                "chunk_start": chunk.start.isoformat(),
                "chunk_end": chunk.end.isoformat(),
            }
            io.metadata_store.save(io.path(name), metadata)
            return ChunkReceipt(chunk.chunk_id, attempt, evidence.count, evidence.digest())

    def discard(self, plan: WindowPlan, chunk: WindowChunk, attempt: str, lease: WindowLease) -> None:
        io = self.io
        name = self.name(plan, chunk, attempt)
        with io.guard.hold(lease), io.connection() as connector:
            io.guard.fence_attempt(lease, name)
            io.mutate(connector, f"DROP TABLE IF EXISTS {io.qualified(name)} SYNC", lease)
            if io.uuid(connector, name) is not None:
                raise WindowContractError("Attempt discard not confirmed")
            io.metadata_store.remove(io.path(name))


def encoded_rows(
    rows: Iterable[tuple[object, ...]],
    encode: Callable[[Iterable[Sequence[object]]], Iterable[bytes]],
    evidence: TypedMultiset,
    window_index: int,
    bounds: tuple[datetime, datetime] | None,
) -> Iterable[bytes]:
    """Encode one bounded row at a time and validate the half-open interval."""
    for row in rows:
        if len(row) != evidence.columns:
            raise WindowContractError("Typed row shape differs from schema")
        if bounds is not None:
            value = row[window_index]
            if not isinstance(value, datetime):
                raise WindowContractError("Window value must be a non-null timestamp")
            if value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)  # noqa: UP017
            if not bounds[0] <= value < bounds[1]:
                raise WindowContractError("Row falls outside its chunk bounds")
        try:
            encoded = b"".join(encode([row]))
        except ValueError as error:
            raise WindowContractError(
                "Typed row cannot be encoded within the declared schema and byte bounds"
            ) from error
        evidence.add(row, encoded)
        yield encoded


def coalesce_encoded_rows(rows: Iterable[bytes], max_bytes: int) -> Generator[bytes, None, None]:
    """Lazily combine rows into byte-capped HTTP chunks with one-row lookahead.

    The producer may retain one complete next row while yielding a full batch.
    Each input row and emitted batch must independently fit max_bytes; no dataset
    materialization or asynchronous prefetch occurs. Closing the consumer closes
    the upstream generator, preserving cancellation and error propagation.
    """
    if isinstance(max_bytes, bool) or not isinstance(max_bytes, int) or max_bytes <= 0:
        raise WindowContractError("Encoded batch byte bound must be a positive integer")
    iterator = iter(rows)
    buffer = bytearray()
    try:
        for row in iterator:
            if len(row) > max_bytes:
                raise WindowContractError("Encoded row exceeds HTTP batch byte bound")
            if buffer and len(buffer) + len(row) > max_bytes:
                yield bytes(buffer)
                buffer.clear()
            buffer.extend(row)
            if len(buffer) == max_bytes:
                yield bytes(buffer)
                buffer.clear()
        if buffer:
            yield bytes(buffer)
    finally:
        close = getattr(iterator, "close", None)
        if close:
            close()
