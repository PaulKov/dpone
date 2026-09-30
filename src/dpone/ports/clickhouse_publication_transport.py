"""Narrow injected capabilities for one-shot publication; no SDK types."""

from contextlib import AbstractContextManager
from typing import Protocol

from dpone.contracts.clickhouse_authority import (
    AuthorityStorageIdentity,
    DispatchGrant,
    OperationBinding,
    TransportState,
)
from dpone.contracts.clickhouse_publication import JournalEntry


class PublicationAuthority(Protocol):
    """Protected originals and irreversible transitions, never a grant cache."""

    def execution_identity(self) -> AuthorityStorageIdentity: ...
    def binding(self, operation_id: str) -> OperationBinding: ...
    def read(self, operation_id: str) -> JournalEntry | None: ...
    def transport_state(self, operation_id: str) -> TransportState: ...
    def begin_send(self, grant: DispatchGrant) -> None: ...
    def close_without_send(self, operation_id: str) -> None: ...
    def record_terminal(self, grant: DispatchGrant, completion_digest: str) -> None: ...


class ExecutionSession(Protocol):
    """Invocation-local exclusion; invalid after exit or across PID/thread."""

    def assert_current(self) -> None: ...


class PublicationExclusion(Protocol):
    """Hold across dispatch/closure, not merely across a SQLite transaction."""

    def hold(self, operation_id: str) -> AbstractContextManager[ExecutionSession]: ...
