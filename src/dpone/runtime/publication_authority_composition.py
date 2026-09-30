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
from typing import TYPE_CHECKING, Any
from uuid import UUID

from dpone.ports.clickhouse_cluster_publication import contracts as c
from dpone.runtime.credentials.resolved_connector_factory import ResolvedConnectorFactory
from dpone.runtime.state.mssql_publication_authority import MssqlPublicationAuthority

if TYPE_CHECKING:
    from dpone.ports.clickhouse_cluster_publication import ClusterPublicationAuthorityPort
    from dpone.ports.mssql_publication import (
        PublicationAuthorityBinding,
        PublicationSqlSession,
        ResolvedBindingConnection,
    )

_IDENTITY_SQL = (
    "SELECT CONVERT(nvarchar(128),SERVERPROPERTY('ServerName')),DB_NAME(),"
    "CONVERT(varchar(36),database_guid) FROM sys.database_recovery_status WHERE database_id=DB_ID()"
)


class BoundPublicationAuthorityProvider:
    """One binding across sink databases; readiness re-admits external storage.

    The factory owns structural admission and closes its observer. Services get
    only the authority port; no service guesses an adapter-specific ensure API.
    Failed re-admission invalidates the previous handle, never falls back.
    """

    def __init__(self, factory: Callable[[], ClusterPublicationAuthorityPort]) -> None:
        self._factory = factory
        self._authority: ClusterPublicationAuthorityPort | None = None
        self._lock = RLock()

    def ensure(self, cluster: str, database: str, hosts: Sequence[str]) -> None:
        del cluster, database, hosts  # target identity belongs to the slot payload
        with self._lock:
            self._authority = None
            self._authority = self._factory()

    def for_database(self, database: str) -> ClusterPublicationAuthorityPort:
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
    pin = _require_deployment_pin(connection, binding, environment)
    factory = connector_factory if connector_factory is not None else ResolvedConnectorFactory.create

    def admitted_connector() -> Any:
        connector = None
        try:
            connector = factory(connection)
            if _endpoint_digest(connector.get_records(_IDENTITY_SQL)) != pin:
                raise ValueError("endpoint identity differs")
            return connector
        except Exception:
            if connector is not None:
                with suppress(Exception):
                    connector.close()
            raise ValueError("publication_authority: SQL endpoint admission failed") from None

    def session_factory() -> PublicationSqlSession:
        connector = admitted_connector()
        try:
            return connector.connection
        except Exception:
            with suppress(Exception):
                connector.close()
            raise ValueError("publication_authority: dedicated SQL session unavailable") from None

    catalog = admitted_connector()
    try:
        return MssqlPublicationAuthority(
            catalog_connector=catalog, session_factory=session_factory, binding=binding, endpoint_identity=pin
        )
    finally:
        try:
            catalog.close()
        except Exception:
            raise ValueError("publication_authority: catalog session close failed") from None


def build_runtime_publication_provider(
    *, connection: ResolvedBindingConnection | None, binding: PublicationAuthorityBinding | None
) -> BoundPublicationAuthorityProvider | None:
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


def _require_deployment_pin(
    connection: ResolvedBindingConnection, binding: PublicationAuthorityBinding, environment: str
) -> str:
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
