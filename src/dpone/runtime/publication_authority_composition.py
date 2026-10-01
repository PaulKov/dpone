"""Compose SQL authority against deployment-owned, non-secret endpoint pins.

Connection descriptors come from the verified workload registry. Endpoint pins
must be admitted during reviewed catalog setup, never populated opportunistically
by observing whichever server a runtime alias happens to resolve to today.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping, Sequence
from contextlib import suppress
from threading import RLock
from typing import TYPE_CHECKING, Any, Generic, TypeVar
from uuid import UUID

from dpone.ports.clickhouse_cluster_publication import contracts as c
from dpone.runtime.credentials.resolved_connector_factory import ResolvedConnectorFactory
from dpone.runtime.state.mssql_publication_authority import MssqlPublicationAuthority
from dpone.runtime.state.mssql_publication_schema import MssqlPublicationSchema

if TYPE_CHECKING:
    from dpone.contracts.runtime_connection import ResolvedBindingConnection
    from dpone.ports.clickhouse_cluster_publication import ClusterPublicationAuthorityPort
    from dpone.ports.mssql_publication import (
        PublicationAuthorityBinding,
        PublicationSqlSession,
    )

_IDENTITY_SQL = (
    "SELECT CONVERT(nvarchar(128),SERVERPROPERTY('ServerName')),DB_NAME(),"
    "CONVERT(varchar(36),database_guid) FROM sys.database_recovery_status WHERE database_id=DB_ID()"
)
AuthorityT = TypeVar("AuthorityT", bound="ClusterPublicationAuthorityPort")


class BoundPublicationAuthorityProvider(Generic[AuthorityT]):
    """One binding across sink databases; readiness re-admits external storage.

    The factory owns structural admission and closes its observer. Services get
    only the authority port; no service guesses an adapter-specific ensure API.
    Failed re-admission invalidates the previous handle, never falls back.
    """

    def __init__(self, factory: Callable[[], AuthorityT]) -> None:
        self._factory = factory
        self._authority: AuthorityT | None = None
        self._lock = RLock()

    def ensure(self, cluster: str, database: str, hosts: Sequence[str]) -> None:
        del cluster, database, hosts  # target identity belongs to the slot payload
        with self._lock:
            self._authority = None
            self._authority = self._factory()

    def for_database(self, database: str) -> AuthorityT:
        del database
        with self._lock:
            if self._authority is None:
                self._authority = self._factory()
            return self._authority


def build_publication_authority(
    *,
    connection: ResolvedBindingConnection,
    binding: PublicationAuthorityBinding,
    environment: str,
    connector_factory: Callable[[ResolvedBindingConnection], Any] | None = None,
) -> MssqlPublicationAuthority:
    """Admit once structurally; verify endpoint identity for *every* new session.

    The selected registry connection must carry ``publication_authority`` with
    service_id, environment and endpoint_identity_sha256. This is deployment
    metadata, not a credential or an author-supplied runtime success flag.
    The catalog observer and every mutation/read session resolve from the same
    immutable workload connection snapshot. No xmin-state connector is reused.
    """
    sessions = _PublicationConnections(connection, binding, environment, connector_factory)
    catalog = sessions.connector()
    try:
        return MssqlPublicationAuthority(
            catalog_connector=catalog, session_factory=sessions.session, binding=binding, endpoint_identity=sessions.pin
        )
    finally:
        try:
            catalog.close()
        except Exception:
            raise ValueError("publication_authority: catalog session close failed") from None


def build_publication_schema(
    *,
    connection: ResolvedBindingConnection,
    binding: PublicationAuthorityBinding,
    environment: str,
    connector_factory: Callable[[ResolvedBindingConnection], Any] | None = None,
) -> MssqlPublicationSchema:
    """Share endpoint admission, without requiring a catalog before explicit setup.

    Plan construction is I/O-free; inspect/apply validate the pinned SQL server,
    database and database GUID on every dedicated session. The caller still
    needs exact plan confirmation to provision an absent catalog.
    """
    sessions = _PublicationConnections(connection, binding, environment, connector_factory)
    return MssqlPublicationSchema(session_factory=sessions.session, binding=binding, endpoint_identity=sessions.pin)


class _PublicationConnections:
    """One admitted session source shared by catalog setup and runtime access."""

    def __init__(
        self,
        connection: ResolvedBindingConnection,
        binding: PublicationAuthorityBinding,
        environment: str,
        factory: Callable[[ResolvedBindingConnection], Any] | None,
    ) -> None:
        self.pin = require_publication_endpoint_pin(connection, binding, environment)
        self._connection = connection
        self._factory = factory if factory is not None else ResolvedConnectorFactory.create

    def connector(self) -> Any:
        connector = None
        try:
            connector = self._factory(self._connection)
            if _endpoint_digest(connector.get_records(_IDENTITY_SQL)) != self.pin:
                raise ValueError("endpoint identity differs")
            return connector
        except Exception:
            if connector is not None:
                with suppress(Exception):
                    connector.close()
            raise ValueError("publication_authority: SQL endpoint admission failed") from None

    def session(self) -> PublicationSqlSession:
        connector = self.connector()
        try:
            return connector.connection
        except Exception:
            with suppress(Exception):
                connector.close()
            raise ValueError("publication_authority: dedicated SQL session unavailable") from None


def build_runtime_publication_provider(
    *, connection: ResolvedBindingConnection | None, binding: PublicationAuthorityBinding | None
) -> BoundPublicationAuthorityProvider[MssqlPublicationAuthority] | None:
    """Preflight the independent store before any source/sink is constructed."""
    if binding is None:
        if connection is not None:
            raise ValueError("publication_authority: binding required")
        return None
    if connection is None:
        raise ValueError("publication_authority: independent resolved connection required")
    provider = BoundPublicationAuthorityProvider(
        lambda: build_publication_authority(connection=connection, binding=binding, environment=binding.environment)
    )
    provider.ensure("", "", ())
    return provider


def require_publication_endpoint_pin(
    connection: ResolvedBindingConnection, binding: PublicationAuthorityBinding, environment: str
) -> str:
    """Validate registry-owned SQL scope without opening a connector."""
    binding.require_descriptor(connection.descriptor)
    if environment != binding.environment or connection.credentials.database != binding.database:
        raise ValueError("publication_authority: runtime environment/storage differs")
    assert connection.descriptor is not None
    policy = connection.descriptor.properties.get("publication_authority")
    if not isinstance(policy, Mapping) or set(policy) != {"service_id", "environment", "endpoint_identity_sha256"}:
        raise ValueError("publication_authority: deployment-owned endpoint pin required")
    pin = policy["endpoint_identity_sha256"]
    if (
        policy["service_id"] != binding.service_id
        or policy["environment"] != environment
        or not isinstance(pin, str)
        or re.fullmatch(r"[0-9a-f]{64}", pin) is None
    ):
        raise ValueError("publication_authority: deployment policy differs")
    return pin


def _endpoint_digest(rows: Any) -> str:
    if len(rows) != 1 or len(rows[0]) != 3:
        raise ValueError("SQL endpoint identity unavailable")
    server, database, guid = rows[0]
    if any(not isinstance(value, str) or not value or value != value.strip() for value in (server, database, guid)):
        raise ValueError("SQL endpoint identity malformed")
    return c.digest_payload(
        {
            "contract": "dpone.mssql-publication-endpoint.v1",
            "server": server,
            "database": database,
            "database_guid": str(UUID(guid)),
        }
    )
