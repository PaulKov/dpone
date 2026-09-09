"""Worker-scoped runtime port for fixed parallel backfill lanes."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from dpone.backfill.process_lane_contracts import (
        BackfillProcessLaneBootstrap,
        ProcessLaneDispatch,
    )
    from dpone.backfill.runtime_execution import ChunkRunner


class BackfillWorkerChunkRunnerFactory(Protocol):
    """Open one isolated chunk runtime for a fixed executor lane."""

    def __call__(self, worker_id: int) -> AbstractContextManager[ChunkRunner]: ...


@dataclass(frozen=True, slots=True)
class BackfillProcessLaneRuntime:
    """Parent-only MSSQL composition; only ``bootstrap`` crosses spawn IPC."""

    bootstrap: BackfillProcessLaneBootstrap
    renew_operation_lease: Callable[[Any, Any], bool]
    validate_operation_binding: Callable[..., bool]
    receipt_recovery: Any
    parent_control_scope: Callable[[Any], AbstractContextManager[None]]
    issue_receipt_probe: Callable[..., tuple[Any, Any] | None] | None = None
    validate_replay_result: Callable[..., bool] | None = None
    prepare_dispatch: Callable[[ProcessLaneDispatch], ProcessLaneDispatch] | None = None
    portable_scope_column_resolver: Callable[[Any, str], Any] | None = None
    portable_scope_history_proof: Callable[..., None] | None = None
    process_chunk_execution_factory: Callable[..., Any] | None = None
    coordinator_tick: Callable[[], None] | None = None
    coordinator_reprove: Callable[[], None] | None = None

    def with_coordinator_lease(
        self,
        tick: Callable[[], None],
        reprove: Callable[[], None],
    ) -> BackfillProcessLaneRuntime:
        """Bind periodic and forced proofs for one acquired campaign lease."""

        return replace(self, coordinator_tick=tick, coordinator_reprove=reprove)


def parent_control_scope(runtime: BackfillProcessLaneRuntime, store: Any) -> AbstractContextManager[None]:
    """Require and open the bounded parent-side control connector scope."""

    scope = getattr(runtime, "parent_control_scope", None)
    if not callable(scope):
        raise RuntimeError("mssql_transaction.process_lane_control_timeout_scope_required")
    return scope(store)


__all__ = [
    "BackfillProcessLaneRuntime",
    "BackfillWorkerChunkRunnerFactory",
    "parent_control_scope",
]
