"""Isolated ClickHouse staging and independently read-back verification."""

from __future__ import annotations

import hashlib
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


class WindowConnector(Protocol):
    """Fresh per-operation ClickHouse connection with streaming readback."""

    def get_records(self, query: str, *, as_dict: bool = False) -> list[Any]: ...
    def get_records_iterator(self, query: str) -> Iterator[Any]: ...
    def execute_query(self, query: str) -> Any: ...
    def close(self) -> None: ...


def identifier(value: str) -> str:
    """Restrict configured names, also protecting existing HTTP identifier quoting."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise WindowContractError("Window target requires simple SQL identifiers")
    return f"`{value}`"


def literal(value: str) -> str:
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


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
        """Read server aggregates; histogram size scales with window calendar days."""
        column = identifier(self.window_column)
        predicate = self.predicate(start, end)
        where = f" WHERE {predicate}" if window_only else ""
        null_sql = ", ".join(f"countIf(isNull({identifier(name)}))" for name, _ in self.schema)
        aggregate = connector.get_records(
            f"SELECT count(), minOrNull({column}), maxOrNull({column}), "
            f"countIf(isNull({column}) OR NOT {predicate}), {null_sql} "
            f"FROM {self.qualified(table)}{where}"
        )[0]

        def timestamp(value: Any) -> str | None:
            if value is None:
                return None
            if value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)  # noqa: UP017
            return value.isoformat()

        day_where = where + (" AND " if where else " WHERE ") + f"isNotNull({column})"
        day_rows = connector.get_records_iterator(
            f"SELECT formatDateTime({column}, '%Y-%m-%d', 'UTC'), count() FROM {self.qualified(table)}"
            f"{day_where} GROUP BY formatDateTime({column}, '%Y-%m-%d', 'UTC') "
            f"ORDER BY formatDateTime({column}, '%Y-%m-%d', 'UTC')"
        )
        try:
            days = {str(day): int(count) for day, count in day_rows}
        finally:
            close = getattr(day_rows, "close", None)
            if close:
                close()
        return {
            "row_count": int(aggregate[0]),
            "min_window_utc": timestamp(aggregate[1]),
            "max_window_utc": timestamp(aggregate[2]),
            "outside_window": int(aggregate[3]),
            "null_counts": {name: int(count) for (name, _), count in zip(self.schema, aggregate[4:], strict=True)},
            "utc_day_counts": days,
        }


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
