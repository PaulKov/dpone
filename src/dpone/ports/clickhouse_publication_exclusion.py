"""Local execution exclusion needs original identity, never dispatch rights."""

from contextlib import AbstractContextManager
from typing import Protocol


class ExecutionSession(Protocol):
    """Invocation-local exclusion; invalid after exit or across PID/thread."""

    def assert_current(self) -> None: ...


class PublicationExclusion(Protocol):
    """Hold across dispatch/closure, not merely across a SQLite transaction."""

    def hold(self, operation_id: str) -> AbstractContextManager[ExecutionSession]: ...
