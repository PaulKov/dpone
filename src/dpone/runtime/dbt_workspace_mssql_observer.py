"""Read actual SQL Server workspace targets through a dedicated bounded session.

This observer never executes model SQL, creates reservations, or grants
activation. Its caller must supply source-verified rows and authenticated
binding context; a successful catalog read is not a protected lifetime lease.
"""

from __future__ import annotations

import json
import math
import time
from collections import deque
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, TypeVar

from dpone.runtime.dbt_workspace_mssql_queries import DEPENDENCIES_SQL, HEADER_SQL, SLOTS_SQL
from dpone.runtime.dbt_workspace_mssql_rows import (
    MssqlWorkspaceDependency,
    MssqlWorkspaceObservation,
    MssqlWorkspaceObservationRequest,
    WorkspaceObservationError,
    dependency_row,
    header_row,
    integer,
    require_observable_database,
    require_observable_identifier,
    slot_rows,
)
from dpone.runtime.state.mssql_database_authority_support import (
    bounded_authority_connection,
    database_connector,
    verify_pin,
)

_T = TypeVar("_T")
_Key = tuple[str, str, str]

if TYPE_CHECKING:
    from dpone.contracts.runtime_connection import ResolvedBindingConnection


class MssqlWorkspaceCatalogObserver:
    """One observation, one owned connection, strict rollback and close.

    Connector construction and monotonic time are injected. ODBC login and
    statement timeouts are capped; elapsed-time checks reject a late result.
    Python cannot forcibly interrupt a stuck native driver cleanup operation.
    """

    def __init__(
        self,
        *,
        connector_factory: Callable[[ResolvedBindingConnection, str], Any] = database_connector,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._connector_factory = connector_factory
        self._clock = clock

    def observe(
        self, request: MssqlWorkspaceObservationRequest, connection: ResolvedBindingConnection
    ) -> MssqlWorkspaceObservation:
        """Return complete metadata only after successful resource cleanup."""

        if not isinstance(request, MssqlWorkspaceObservationRequest):
            raise WorkspaceObservationError("request")
        request.__post_init__()
        if connection.descriptor is None or connection.descriptor.connection_type != "mssql":
            raise WorkspaceObservationError("connector")
        default_database = require_observable_identifier(connection.credentials.database)
        if default_database != request.default_database:
            raise WorkspaceObservationError("database_context")
        for write in request.writes:
            if write.database is None and write.kind != "transfer":
                require_observable_database(default_database)
        coordinates = tuple(
            (write.database or default_database, write.schema, write.relation) for write in request.writes
        )
        payload = _json_payload(
            [
                {"slot_id": index, "database": key[0], "schema": key[1], "relation": key[2]}
                for index, key in enumerate(coordinates)
            ],
            request.limits.max_json_bytes,
        )
        deadline = self._clock() + request.limits.timeout_seconds
        connector = None
        result = None
        failure = None
        try:
            bounded = bounded_authority_connection(
                connection, connect_timeout_seconds=min(10, request.limits.timeout_seconds)
            )
            connector = self._connector_factory(bounded, request.pin.database_name)
            session = _CatalogSession(connector, deadline=deadline, clock=self._clock)
            session.run(connector.begin)
            result = self._read(session, request, coordinates, payload)
        except WorkspaceObservationError as error:
            failure = WorkspaceObservationError(error.reason)
        except Exception:
            failure = WorkspaceObservationError("catalog_unavailable")
        finally:
            if connector is not None:
                # Cleanup is attempted even after deadline, begin, or rollback failure.
                for operation in (connector.rollback, connector.close):
                    try:
                        operation()
                    except Exception:
                        failure = WorkspaceObservationError("cleanup")
        if self._clock() >= deadline:
            failure = WorkspaceObservationError("deadline")
        if failure is not None:
            raise failure from None  # Outside handlers: no secret-bearing exception context.
        if result is None:
            raise WorkspaceObservationError("catalog_unavailable")
        return result

    @staticmethod
    def _read(
        session: _CatalogSession,
        request: MssqlWorkspaceObservationRequest,
        coordinates: tuple[_Key, ...],
        payload: str,
    ) -> MssqlWorkspaceObservation:
        verify_pin(session.connector, request.pin, role="target", query_runner=session.run)
        database_names = tuple(dict.fromkeys((request.default_database, *request.invocation_databases)))
        databases = _json_payload(
            [{"database": name} for name in database_names],
            request.limits.max_json_bytes,
        )
        header = header_row(session.one(HEADER_SQL, (databases,)), request)
        slots = slot_rows(session.rows(SLOTS_SQL, (len(request.writes) + 1, payload)), request)
        roots = tuple(dict.fromkeys(key for key, write in zip(coordinates, request.writes) if write.kind != "transfer"))
        view_ids = {slot.object_id for slot in slots if slot.object_type == "V" and slot.object_id is not None}
        dependencies = _incoming_closure(session, request, roots, view_ids)
        # Continuity checks are not protection against concurrent DDL or later writes.
        if header_row(session.one(HEADER_SQL, (databases,)), request) != header:
            raise WorkspaceObservationError("session_changed")
        verify_pin(session.connector, request.pin, role="target", query_runner=session.run)
        return MssqlWorkspaceObservation(request, header, slots, dependencies)


class _CatalogSession:
    """Enforce per-query timeout and total elapsed budget on one connector."""

    def __init__(self, connector: Any, *, deadline: float, clock: Callable[[], float]) -> None:
        self.connector, self.deadline, self.clock = connector, deadline, clock

    def run(self, operation: Callable[[], _T]) -> _T:
        remaining = self.deadline - self.clock()
        if remaining <= 0:
            raise WorkspaceObservationError("deadline")
        with self.connector.bounded_query_timeout(max(1, min(10, math.ceil(remaining)))):
            result = operation()
        if self.clock() >= self.deadline:
            raise WorkspaceObservationError("deadline")
        return result

    def rows(self, sql: str, params: tuple[Any, ...]) -> list[dict[str, Any]]:
        rows = self.run(lambda: self.connector.get_records(sql, params, as_dict=True))
        if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
            raise WorkspaceObservationError("catalog_row")
        return rows

    def one(self, sql: str, params: tuple[Any, ...]) -> dict[str, Any]:
        rows = self.rows(sql, params)
        if len(rows) != 1:
            raise WorkspaceObservationError("catalog_row")
        return rows[0]


def _json_payload(rows: list[dict[str, object]], maximum: int) -> str:
    payload = json.dumps(rows, ensure_ascii=True, separators=(",", ":"), allow_nan=False)
    if len(payload.encode("utf-8")) > maximum:
        raise WorkspaceObservationError("json_budget")
    return payload


def _incoming_closure(
    session: _CatalogSession,
    request: MssqlWorkspaceObservationRequest,
    roots: tuple[_Key, ...],
    view_ids: set[int],
) -> tuple[MssqlWorkspaceDependency, ...]:
    limits = request.limits
    if len(view_ids) > limits.max_nodes:
        raise WorkspaceObservationError("node_budget")
    frontier, seen = roots, set(roots)
    graph: dict[_Key, set[_Key]] = {root: set() for root in roots}
    observations: list[MssqlWorkspaceDependency] = []
    while frontier:
        payload = _json_payload(
            [
                {"frontier_id": index, "database": key[0], "schema": key[1], "relation": key[2]}
                for index, key in enumerate(frontier)
            ],
            limits.max_json_bytes,
        )
        remaining = limits.max_edges - len(observations)
        rows = session.rows(DEPENDENCIES_SQL, (remaining + 1, payload))
        if len(rows) > remaining:
            raise WorkspaceObservationError("edge_budget")
        following = []
        for row in rows:
            parent_id = integer(row.get("frontier_id"), minimum=0)
            if parent_id >= len(frontier):
                raise WorkspaceObservationError("catalog_row")
            parent = frontier[parent_id]
            edge = dependency_row(row, parent)
            observations.append(edge)
            view_ids.add(edge.object_id)
            if len(view_ids) > limits.max_nodes:
                raise WorkspaceObservationError("node_budget")
            child = edge.database_arg, edge.schema_name, edge.object_name
            graph[parent].add(child)
            graph.setdefault(child, set())
            if child not in seen:
                seen.add(child)
                following.append(child)
        _validate_depth(graph, limits.max_depth)
        frontier = tuple(following)
    return tuple(observations)


def _validate_depth(graph: dict[_Key, set[_Key]], maximum: int) -> None:
    """Topological longest path, not shortest BFS depth; diamonds are not cycles."""

    indegrees = dict.fromkeys(graph, 0)
    depths = dict.fromkeys(graph, 0)
    for children in graph.values():
        for child in children:
            indegrees[child] += 1
    ready = deque(key for key, count in indegrees.items() if count == 0)
    completed = 0
    while ready:
        key = ready.popleft()
        completed += 1
        for child in graph[key]:
            depths[child] = max(depths[child], depths[key] + 1)
            if depths[child] > maximum:
                raise WorkspaceObservationError("depth_budget")
            indegrees[child] -= 1
            if indegrees[child] == 0:
                ready.append(child)
    if completed != len(graph):
        raise WorkspaceObservationError("dependency_cycle")
