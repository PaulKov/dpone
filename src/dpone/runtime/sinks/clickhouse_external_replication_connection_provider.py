"""Invocation-scoped direct-member connection lifecycle."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
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
        connections = tuple(self._connections.values())
        self._connections.clear()
        self._member_ids_by_connection.clear()
        failures: list[Exception] = []
        for direct in connections:
            if callable(close := getattr(direct, "close", None)):
                try:
                    close()
                except Exception as exc:
                    failures.append(exc)
        if failures:
            error = RuntimeError("direct member connection cleanup failed")
            error.add_note(f"failed_connections={len(failures)}")
            raise error from failures[0]


@contextmanager
def managed_member_connection(
    provider: Any,
    member_id: str,
    *,
    unavailable: Callable[[], Exception],
) -> Iterator[Any]:
    """Close an invocation provider without masking a primary driver failure."""

    connection = provider(member_id) if callable(provider) else provider.connection_for(member_id)
    if connection is None:
        raise unavailable()
    primary_error: BaseException | None = None
    try:
        yield connection
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        if callable(close := getattr(provider, "close", None)):
            try:
                close()
            except Exception as close_error:
                if primary_error is None:
                    raise
                primary_error.add_note(
                    "A direct member connection cleanup failure was suppressed to preserve this primary error."
                )
                primary_error.add_note(f"cleanup_error_type={type(close_error).__name__}")


__all__ = ["ExternalReplicaConnectionProvider", "managed_member_connection"]
