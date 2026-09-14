"""Identified, bounded query port for one explicit ClickHouse file stage.

Adapters depend on these runtime values, never on the sink/service packages.
Result bytes are transport observations; only the service grants stage authority.
"""

from __future__ import annotations

import json
import re
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Literal, Protocol
from uuid import UUID, uuid4

CHUNK_BYTES = 1_048_576
MAX_RESPONSE_BYTES = 65_536
MAX_EVENT_BYTES = 262_144
ABORT_PHASE_SECONDS = 5
REMOTE_CONFIRMATION_SECONDS = 30
SYNC_SETTINGS = {"async_insert": 0, "input_format_allow_errors_num": 0, "input_format_allow_errors_ratio": 0}

# Query metadata and COUNT are parsed as canonical JSON integers.
QUERY_SETTINGS = {**SYNC_SETTINGS, "output_format_json_quote_64bit_integers": 0}

QueryKind = Literal["probe", "create", "describe", "insert", "count", "drop"]


@dataclass(frozen=True, slots=True)
class ClickHouseFilePlan:
    """Frozen interpretation of the source and derived synchronous wire."""

    source_schema: tuple[tuple[str, str], ...]
    target_schema: tuple[tuple[str, str], ...]
    mode: str
    binary_encoding: str
    transport_timeout_seconds: float
    config_sha256: str
    target_database: str
    target_table: str


@dataclass(frozen=True, slots=True)
class EndpointBinding:
    """Observed server identity and database, shared by stage and its finalizer."""

    server_uuid: str
    database: str
    host: str
    port: int
    secure: bool
    database_uuid: str


@dataclass(frozen=True, slots=True)
class QueryIdentity:
    query_id: str
    kind: QueryKind
    endpoint: EndpointBinding


@dataclass(frozen=True, slots=True)
class IdentifiedStageQuery:
    identity: QueryIdentity
    sql: str


@dataclass(frozen=True, slots=True)
class StageQueryResult:
    """Completed transport result; no row estimate or commit capability."""

    query_id: str
    emitted_bytes: int
    emitted_sha256: str
    response: bytes
    remote_state: Literal["completed"] = "completed"


@dataclass(frozen=True, slots=True)
class QueryObservation:
    local_state: Literal["stopped", "unknown"]
    remote_state: Literal["completed", "cancelled", "unknown"]


class ClickHouseFileStageRunner(Protocol):
    """One admitted transport; every mutation is identified and submitted once."""

    def preflight(self, plan: ClickHouseFilePlan) -> EndpointBinding:
        """Read and bind the exact selected node before mutation."""
        ...

    def execute(
        self,
        request: IdentifiedStageQuery,
        *,
        chunks: Iterable[bytes] | None,
        deadline_monotonic: float,
    ) -> StageQueryResult:
        """Finish bounded I/O or raise, retaining any unjoined local resource."""
        ...

    def cancel_and_observe(self, query: QueryIdentity, *, deadline_monotonic: float) -> QueryObservation:
        """Settle the sender and separately establish exact-query termination."""
        ...


def sql_literal(value: str) -> str:
    """Escape ClickHouse string literals, including its backslash grammar."""
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def sql_identifier(value: str) -> str:
    return "`" + value.replace("\\", "\\\\").replace("`", "\\`") + "`"


def identity_sql(database: str, *, formatted: bool = True) -> str:
    sql = f"SELECT toString(serverUUID()), name, toString(uuid), engine FROM system.databases WHERE name = {sql_literal(database)}"
    return sql + (" FORMAT JSONCompactEachRow" if formatted else "")


def response_rows(body: bytes) -> list[list[object]]:
    """Reject malformed, over-limit or non-row JSON responses without coercion."""
    if len(body) > MAX_RESPONSE_BYTES:
        raise RuntimeError("clickhouse_file_response_limit")
    rows = [json.loads(line) for line in body.splitlines() if line.strip()]
    if not all(isinstance(row, list) for row in rows):
        raise RuntimeError("clickhouse_file_response_shape")
    return rows


def require_endpoint_row(rows: object, database: str) -> tuple[str, str]:
    """Bind one Atomic database and persisted server UUID; never accept zero IDs."""
    if not isinstance(rows, (list, tuple)) or len(rows) != 1:
        raise RuntimeError("clickhouse_file_endpoint_identity")
    row = rows[0]
    if not isinstance(row, (list, tuple)) or len(row) != 4 or row[1] != database or row[3] != "Atomic":
        raise RuntimeError("clickhouse_file_endpoint_identity")
    server, namespace = str(row[0]), str(row[2])
    if UUID(server).int == 0 or UUID(namespace).int == 0:
        raise RuntimeError("clickhouse_file_endpoint_identity")
    return server, namespace


class IdentifiedFileRunner(ABC):
    """Shared same-node query identity and cancellation policy for two adapters."""

    def __init__(self, *, host: str, port: int, secure: bool, clock: Callable[[], float]) -> None:
        self.host, self.port, self.secure, self.clock = host, port, secure, clock
        self.endpoint: EndpointBinding | None = None
        self.local_stopped = True
        self._completed: set[str] = set()

    def preflight(self, plan: ClickHouseFilePlan) -> EndpointBinding:
        placeholder = self.endpoint or EndpointBinding(
            str(UUID(int=0)), plan.target_database, self.host, self.port, self.secure, str(UUID(int=0))
        )
        request = IdentifiedStageQuery(
            QueryIdentity(f"dpone-b02-{uuid4().hex}-probe", "probe", placeholder), identity_sql(plan.target_database)
        )
        result = self.execute(request, chunks=None, deadline_monotonic=self.clock() + plan.transport_timeout_seconds)
        server, namespace = require_endpoint_row(response_rows(result.response), plan.target_database)
        endpoint = EndpointBinding(server, plan.target_database, self.host, self.port, self.secure, namespace)
        if self.endpoint is not None and self.endpoint != endpoint:
            raise RuntimeError("clickhouse_file_endpoint_changed")
        self.endpoint = endpoint
        return endpoint

    def require_request(self, request: IdentifiedStageQuery) -> None:
        identity = request.identity
        if not re.fullmatch(
            r"dpone-b02-[0-9a-f]{32}-(?:probe|create|describe|insert|count|drop)(?:-[a-z]+)?", identity.query_id
        ):
            raise ValueError("invalid file-stage query identity")
        if self.endpoint is None and identity.kind != "probe":
            raise RuntimeError("clickhouse_file_preflight_required")
        if self.endpoint is not None and identity.endpoint != self.endpoint:
            raise RuntimeError("clickhouse_file_endpoint_changed")

    @abstractmethod
    def execute(
        self, request: IdentifiedStageQuery, *, chunks: Iterable[bytes] | None, deadline_monotonic: float
    ) -> StageQueryResult:
        """Implement one bounded attempt, recording only complete acknowledgments."""

    def cancel_and_observe(self, query: QueryIdentity, *, deadline_monotonic: float) -> QueryObservation:
        if not self.local_stopped:
            return QueryObservation("unknown", "unknown")
        if query.query_id in self._completed:
            return QueryObservation("stopped", "completed")
        kill = IdentifiedStageQuery(
            QueryIdentity(f"dpone-b02-{uuid4().hex}-probe", "probe", query.endpoint),
            f"KILL QUERY WHERE query_id = {sql_literal(query.query_id)} SYNC FORMAT JSONCompactEachRow",
        )
        try:
            result = self.execute(kill, chunks=None, deadline_monotonic=deadline_monotonic)
            rows = response_rows(result.response)
            if len(rows) == 1 and len(rows[0]) >= 2 and rows[0][0] == "finished" and rows[0][1] == query.query_id:
                return QueryObservation("stopped", "cancelled")
        except (Exception, KeyboardInterrupt):
            pass
        return QueryObservation("stopped" if self.local_stopped else "unknown", "unknown")

    def completed(self, request: IdentifiedStageQuery, body: bytes, size: int, digest: str) -> StageQueryResult:
        if request.identity.kind in {"create", "insert", "drop"} and body.strip():
            raise RuntimeError("clickhouse_file_unexpected_mutation_response")
        self._completed.add(request.identity.query_id)
        return StageQueryResult(request.identity.query_id, size, digest, body)
