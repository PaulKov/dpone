"""PostgreSQL window reads sharing one live, exported read-only snapshot.

The keeper owns the snapshot and an ACCESS SHARE relation lock for the complete
context lifetime. Each worker imports that snapshot before its first query and
uses a server cursor. Restarting the keeper creates a different source version;
completed chunks from a previous source version must never be reused.
"""

from __future__ import annotations

import hashlib
import json
import threading
import uuid
from collections.abc import Callable, Iterator, Sequence
from typing import Any

from dpone.contracts.bounded_window import WindowChunk, WindowPlan
from dpone.contracts.process_errors import WindowContractError


def _fingerprint(value: object) -> str:
    return hashlib.sha256(json.dumps(value, separators=(",", ":"), sort_keys=True).encode()).hexdigest()


def _identifier(value: str) -> str:
    if not isinstance(value, str) or not value or "\x00" in value:
        raise WindowContractError("PostgreSQL identifiers must be nonempty text without NUL")
    return '"' + value.replace('"', '""') + '"'


class PostgresWindowSource:
    """Context-managed live source version with independent worker connections.

    ``connection_factory`` must return a fresh PostgreSQL DBAPI connection (the
    psycopg named-cursor API), never a shared connection. Credentials remain in
    the injected factory. Only ordinary tables and timestamptz window columns
    are initially supported. Column order is explicit and duplicate-free.

    ``schema_fingerprint`` is the caller's target typed-schema fingerprint.
    Source catalog metadata is independently bound into parameters/source
    identities, so matching a target hash cannot hide source schema drift.
    Source Python values and driver buffers are not a process-memory guarantee;
    ``batch_rows`` bounds fetch cardinality, not row size.
    """

    def __init__(
        self,
        *,
        connection_factory: Callable[[], Any],
        schema_name: str,
        table_name: str,
        columns: Sequence[str],
        window_column: str,
        schema_fingerprint: str,
        batch_rows: int = 8192,
    ) -> None:
        self._relation = f"{_identifier(schema_name)}.{_identifier(table_name)}"
        self._projection = ", ".join(_identifier(column) for column in columns)
        self._window = _identifier(window_column)
        if not columns or len(set(columns)) != len(columns) or not schema_fingerprint:
            raise WindowContractError("Selected columns and target schema fingerprint must be unambiguous")
        if isinstance(batch_rows, bool) or not isinstance(batch_rows, int) or batch_rows <= 0:
            raise WindowContractError("batch_rows must be a positive integer")
        self._factory = connection_factory
        self._schema_name = schema_name
        self._table_name = table_name
        self._columns = tuple(columns)
        self._window_column = window_column
        self.schema_fingerprint = schema_fingerprint
        self._batch_rows = batch_rows
        self._lock = threading.RLock()
        self._keeper: Any = None
        self._workers: dict[int, Any] = {}
        self._snapshot = ""
        self._transaction = None
        self._metadata: tuple[tuple[object, ...], ...] = ()
        self.source_schema_fingerprint = ""
        self.source_version = ""
        self.parameters_fingerprint = ""

    def __enter__(self) -> PostgresWindowSource:
        with self._lock:
            if self._keeper is not None or self._workers:
                raise WindowContractError("Snapshot context is already active")
            connection = self._factory()
            self._keeper = connection
            try:
                connection.autocommit = True
                with connection.cursor() as cursor:
                    cursor.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
                    cursor.execute(f"LOCK TABLE {self._relation} IN ACCESS SHARE MODE")
                    self._metadata = self._catalog(cursor)
                    cursor.execute("SELECT txid_current(), pg_export_snapshot()")
                    self._transaction, self._snapshot = cursor.fetchone()
                self.source_schema_fingerprint = _fingerprint(self._metadata)
                self.parameters_fingerprint = _fingerprint(
                    [
                        1,
                        self._schema_name,
                        self._table_name,
                        self._columns,
                        self._window_column,
                        self.source_schema_fingerprint,
                    ]
                )
                self.source_version = _fingerprint(
                    [1, self._snapshot, uuid.uuid4().hex, self.source_schema_fingerprint]
                )
                return self
            except BaseException:
                self._keeper = None
                connection.close()
                raise

    def __exit__(self, *_: object) -> None:
        with self._lock:
            keeper, self._keeper = self._keeper, None
            for worker in self._workers.values():
                worker.close()
            if keeper is not None:
                keeper.close()  # Closing rolls back, releasing locks and snapshot.

    def _catalog(self, cursor: Any) -> tuple[tuple[object, ...], ...]:
        cursor.execute(
            "SELECT a.attname, a.atttypid, a.atttypmod, a.attnotnull, a.attcollation, c.relkind "
            "FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c ON c.oid=a.attrelid "
            "JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname=%s AND c.relname=%s AND a.attnum>0 AND NOT a.attisdropped",
            (self._schema_name, self._table_name),
        )
        available = {row[0]: tuple(row) for row in cursor.fetchall()}
        required = tuple(dict.fromkeys((*self._columns, self._window_column)))
        if any(name not in available for name in required):
            raise WindowContractError("PostgreSQL selected column is unavailable")
        metadata = tuple(available[name] for name in required)
        if any(row[-1] != "r" for row in metadata):
            raise WindowContractError("PostgreSQL window source requires an ordinary table")
        if available[self._window_column][1] != 1184:
            raise WindowContractError("PostgreSQL window column requires timestamp with time zone")
        return metadata

    def validate(self, plan: WindowPlan) -> None:
        """Check frozen identities and a still-live keeper transaction."""
        with self._lock:
            if self._keeper is None or not self.source_version:
                raise WindowContractError("PostgreSQL source snapshot is expired")
            if (
                plan.source_version != self.source_version
                or plan.window_column != self._window_column
                or plan.schema_fingerprint != self.schema_fingerprint
                or plan.parameters_fingerprint != self.parameters_fingerprint
            ):
                raise WindowContractError("PostgreSQL source plan identity mismatch")
            try:
                with self._keeper.cursor() as cursor:
                    cursor.execute("SELECT txid_current()")
                    if cursor.fetchone()[0] != self._transaction:
                        raise WindowContractError("PostgreSQL source snapshot transaction changed")
            except Exception as error:
                raise WindowContractError("PostgreSQL source snapshot is unavailable") from error

    def read(self, plan: WindowPlan, chunk: WindowChunk) -> Iterator[tuple[object, ...]]:
        """Read exactly the plan's chunk; cancellation closes all worker resources."""
        self.validate(plan)
        if chunk not in plan.chunks:
            raise WindowContractError("PostgreSQL source chunk is not part of the frozen plan")
        connection = self._factory()
        with self._lock:
            if connection is self._keeper or id(connection) in self._workers:
                raise WindowContractError("PostgreSQL workers require independent connections")
            self._workers[id(connection)] = connection
            snapshot = self._snapshot
        try:
            self.validate(plan)
            connection.autocommit = True
            with connection.cursor() as cursor:
                cursor.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
                # SET SNAPSHOT cannot use server-side bind parameters. Only the
                # strict hexadecimal exported-token grammar becomes a literal.
                if not snapshot or any(char not in "0123456789ABCDEFabcdef-" for char in snapshot):
                    raise WindowContractError("PostgreSQL exported snapshot token is invalid")
                cursor.execute("SET TRANSACTION SNAPSHOT '" + snapshot + "'")
                if self._catalog(cursor) != self._metadata:
                    raise WindowContractError("PostgreSQL worker source schema drift")
            with connection.cursor(name="dpone_window_" + uuid.uuid4().hex) as cursor:
                cursor.execute(
                    f"SELECT {self._projection} FROM {self._relation} "
                    f"WHERE {self._window} >= %s AND {self._window} < %s",
                    (chunk.start, chunk.end),
                )
                while True:
                    self.validate(plan)
                    rows = cursor.fetchmany(self._batch_rows)
                    if not rows:
                        return
                    for row in rows:
                        if self._keeper is None or plan.source_version != self.source_version:
                            raise WindowContractError("PostgreSQL source snapshot is expired")
                        if len(row) != len(self._columns):
                            raise WindowContractError("PostgreSQL source row shape mismatch")
                        yield tuple(row)
        finally:
            try:
                connection.close()
            finally:
                with self._lock:
                    self._workers.pop(id(connection), None)
