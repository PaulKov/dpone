"""Invocation-scoped direct-member connection lifecycle."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any


class ExternalReplicaConnectionProvider:
    """Resolve, probe, cache, and deterministically close member clients."""

    def __init__(
        self,
        connector: Any,
        *,
        topology: Any,
        connect: Callable[..., Any],
        unavailable: Callable[[], Exception],
    ) -> None:
        self._connector = connector
        self._topology = topology
        self._connect = connect
        self._unavailable = unavailable
        self._connections: dict[str, Any] = {}
        self._member_ids_by_connection: dict[int, str] = {}

    def connection_for(self, member_id: str) -> Any:
        cached = self._connections.get(member_id)
        if cached is not None:
            return cached
        host, address, port = self._topology.member_endpoint(member_id)
        try:
            connection = self._connect(self._connector, host, address, port)
            self._connections[member_id] = connection
            self._member_ids_by_connection[id(connection)] = member_id
            return connection
        except Exception:
            raise self._unavailable() from None

    def require_connections(self) -> None:
        try:
            for member_id in self._topology.member_ids():
                if self.connection_for(member_id).get_records("SELECT 1") != [(1,)]:
                    raise RuntimeError("direct member probe returned an unexpected result")
        except Exception:
            raise self._unavailable() from None
        finally:
            self.close()

    def member_identity(self, connection: Any) -> str:
        return self._member_ids_by_connection.get(id(connection), "")

    def close(self) -> None:
        for direct in self._connections.values():
            if callable(close := getattr(direct, "close", None)):
                close()
        self._connections.clear()
        self._member_ids_by_connection.clear()


__all__ = ["ExternalReplicaConnectionProvider"]
