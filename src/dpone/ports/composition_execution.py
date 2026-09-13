"""Structural execution lifecycle boundaries supplied by the application.

The listener never chooses deadline or cleanup policy. The application pairs
one stop signal with its budget factory; implementations own synchronization,
first-stop timestamps and the nonrenewable cleanup bound.
"""

from __future__ import annotations

from typing import Protocol


class AdmissionStopSignal(Protocol):
    """Signal-safe admission notification; draining belongs to normal coordination."""

    def notify(self) -> None:
        """Publish the first stop without acquiring lifecycle locks."""
        ...

    def is_set(self) -> bool:
        """Whether admission has closed."""
        ...


class ExecutionBudget(Protocol):
    """One execution lifetime with explicit, bounded cleanup and effect guards."""

    @property
    def execution_deadline(self) -> float:
        """The immutable absolute execution deadline."""
        ...

    @property
    def execution_expired(self) -> bool:
        """Whether execution expired or shared admission stopped."""
        ...

    def remaining_execution(self) -> float:
        """Require execution authority and return its remaining duration."""
        ...

    def remaining_cleanup(self) -> float:
        """Require an already selected, unexpired cleanup phase."""
        ...

    def io_deadline(self) -> float:
        """Current phase I/O bound; this grants no effect authority."""
        ...

    def require_effect(self) -> None:
        """Reject admitting a new effect after execution or admission closes."""
        ...

    def begin_cleanup(self) -> float:
        """Irreversibly select the single bounded cleanup deadline."""
        ...

    def stop(self) -> None:
        """Normal coordinator stop; signal handlers notify the shared signal."""
        ...
