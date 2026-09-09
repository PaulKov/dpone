"""Dependency-injected lifecycle boundary for one backfill chunk attempt.

The port is intentionally runtime-only.  It lets schedulers and acceptance
harnesses coordinate worker health and crash-window probes without turning
fault timing into manifest authoring or execution-policy identity.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(frozen=True, slots=True)
class BackfillChunkLifecycleContext:
    """Durable ownership context visible to a trusted runtime lifecycle port."""

    run_key: str
    chunk_index: int
    owner: str
    store: Any


class BackfillChunkLifecyclePort(Protocol):
    """Observe explicit crash boundaries around a governed chunk attempt."""

    def before_heartbeat(self, context: BackfillChunkLifecycleContext) -> None: ...

    def before_chunk_runner(self, context: BackfillChunkLifecycleContext) -> None: ...

    def before_ledger_completion(
        self,
        context: BackfillChunkLifecycleContext,
        result: dict[str, Any] | Any,
    ) -> None: ...


class DefaultBackfillChunkLifecycle:
    """Production lifecycle implementation with no external fault behavior."""

    def before_heartbeat(self, context: BackfillChunkLifecycleContext) -> None:
        del context

    def before_chunk_runner(self, context: BackfillChunkLifecycleContext) -> None:
        del context

    def before_ledger_completion(
        self,
        context: BackfillChunkLifecycleContext,
        result: dict[str, Any] | Any,
    ) -> None:
        del context, result


__all__ = [
    "BackfillChunkLifecycleContext",
    "BackfillChunkLifecyclePort",
    "DefaultBackfillChunkLifecycle",
]
