"""Keep existing publication lifecycle effects inside one admitted recovery hold."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.ports.clickhouse_cluster_publication import ClusterPublicationDdlPort
    from dpone.ports.clickhouse_cluster_publication import contracts as c
    from dpone.ports.prepared_recovery import HeldPreparedRecoverySafety, NativePublicationAuthorityPort


class HeldRecoveryAuthority:
    """Bind ordinary cleanup to operation-aware native reads and the same handle."""

    def __init__(
        self,
        authority: NativePublicationAuthorityPort,
        held: HeldPreparedRecoverySafety,
        read_current: Callable[[], c.VersionedAuthorityRecord],
    ) -> None:
        self._authority, self._held, self._read_current = authority, held, read_current

    def read_versioned(self, target_key: str) -> c.VersionedAuthorityRecord:
        self._held.require_held()
        current = self._read_current()
        if current.record.target_key != target_key:
            raise ValueError("recovery target changed")
        return current

    def read_for_operation(self, target_key: str, operation_id: str) -> c.VersionedAuthorityRecord:
        current = self.read_versioned(target_key)
        if current.record.operation_id != operation_id:
            raise ValueError("recovery operation changed")
        return current

    def create_if_absent(self, record: c.AuthorityRecord) -> c.AuthorityMutationResult:
        raise ValueError("recovery cannot create authority")

    def compare_and_swap(
        self,
        current: c.VersionedAuthorityRecord,
        desired: c.AuthorityRecord,
    ) -> c.AuthorityMutationResult:
        observed = self.read_versioned(current.record.target_key)
        if observed != current:
            raise ValueError("recovery current authority changed")
        self._held.require_held()
        return self._authority.compare_and_swap(current, desired)


class HeldRecoveryDdl:
    """Reuse actual DDL/permit implementation, checking the hold before effects."""

    def __init__(self, ddl: ClusterPublicationDdlPort, held: HeldPreparedRecoverySafety) -> None:
        self._ddl, self._held = ddl, held

    def publication_query_digest(self, record: c.AuthorityRecord, *, cluster: str) -> str:
        return self._ddl.publication_query_digest(record, cluster=cluster)

    def cleanup_query_digest(self, record: c.AuthorityRecord, *, cluster: str) -> str:
        return self._ddl.cleanup_query_digest(record, cluster=cluster)

    def dispatch_publication(self, record: c.AuthorityRecord, permit: c.DispatchPermit, *, cluster: str) -> None:
        self._held.require_held()
        self._ddl.dispatch_publication(record, permit, cluster=cluster)

    def drop_predecessor(self, record: c.AuthorityRecord, permit: c.DispatchPermit, *, cluster: str) -> None:
        self._held.require_held()
        self._ddl.drop_predecessor(record, permit, cluster=cluster)

    def find_entries(self, cluster: str, correlation_token: str) -> tuple[c.QueueEntry, ...]:
        self._held.require_held()
        return self._ddl.find_entries(cluster, correlation_token)

    def read_entry(self, cluster: str, entry: str) -> c.QueueEntry | None:
        self._held.require_held()
        return self._ddl.read_entry(cluster, entry)
