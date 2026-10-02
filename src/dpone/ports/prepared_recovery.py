"""Explicit native-history and deployment-held safety capabilities for recovery."""

from __future__ import annotations

from contextlib import AbstractContextManager
from typing import TYPE_CHECKING, Protocol

from dpone.ports.clickhouse_cluster_publication import (
    ClusterPublicationAuthorityPort,
    ClusterPublicationBootstrapPort,
)

if TYPE_CHECKING:
    from dpone.contracts.prepared_recovery import PreparedRecoverySafetyObservation
    from dpone.contracts.publication_preparation import NativePublicationPreparation


class NativePublicationAuthorityPort(ClusterPublicationAuthorityPort, Protocol):
    def read_native_preparation(self, target_key: str, operation_id: str) -> NativePublicationPreparation: ...


class NativePublicationAuthorityProvider(ClusterPublicationBootstrapPort, Protocol):
    """One admitted selected provider, not independently supplied read/write stores."""

    def for_database(self, database: str) -> NativePublicationAuthorityPort: ...


class HeldPreparedRecoverySafety(Protocol):
    def require_held(self) -> None:
        """Fail if admitted exclusion/drain no longer protects the recovery."""
        ...

    def observe_unpublished(self, preparation: NativePublicationPreparation) -> PreparedRecoverySafetyObservation:
        """Authenticate full per-replica negative history and writer exclusion."""
        ...


class PreparedRecoverySafetyObserver(Protocol):
    def hold(
        self, *, cluster: str, preparation: NativePublicationPreparation
    ) -> AbstractContextManager[HeldPreparedRecoverySafety]:
        """Hold deployment-admitted safety; exiting does not unfreeze deployment.

        The recovery is the only admitted executor. The observer must exclude old,
        restarted and manual writers and retain the freeze through the cutover.
        There is intentionally no permissive default or operator-JSON adapter.
        """
        ...
