"""Composition and ownership values for native MSSQL preparation."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Any

from dpone.runtime.mssql_native_chunks_observations import delivery_session


@dataclass(frozen=True)
class NativeStageContext:
    """Composition-owned, fenced services for normal and source-free recovery."""

    plan: Any
    wire_contract: Any
    executor: Any
    lease: Any
    row_source: Callable[[], Iterable[Any]]
    verify_receipts: Callable[[tuple[Any, ...]], None]
    cleanup_receipts: Callable[[tuple[Any, ...]], None]
    capacity_check: Callable[[int], None]
    journal_factory: Callable[[], Any]
    preparation_scope: Callable[[], Any]
    interval: Any = None
    recover: bool = False
    completed_lifecycle: Any = None
    max_row_bytes: int = 1048576
    cancelled: Any = None
    observer: Any = field(default=None, kw_only=True)
    verification_identity: Any = field(default=None, kw_only=True)
    target_local_timeout_seconds: int = field(default=3600, kw_only=True)
    recovery_bindings: Callable[[Any], Any] | None = field(default=None, kw_only=True)
    persisted_hash_layout: bool = field(default=False, kw_only=True)

    def __post_init__(self) -> None:
        object.__setattr__(self, "observer", delivery_session(self.observer))


@dataclass(frozen=True)
class NativePreparedResources:
    """Exact target resources retained until publication is reconciled."""

    context: NativeStageContext
    receipts: tuple[Any, ...]
    verify_digest: str
    source_schema: tuple[tuple[str, str], ...]
    object_id: int
    planned: dict[str, str]
    mutation_watermark: int | None = None


def require_prepared_resources(prepared: Any) -> NativePreparedResources:
    """Return the single framework-owned preparation resource bundle."""

    if len(prepared.resources) != 1 or not isinstance(prepared.resources[0], NativePreparedResources):
        raise ValueError("mssql_native.prepared_ownership_required")
    return prepared.resources[0]


def strategy_for_prepared(sink: Any, prepared: Any) -> Any:
    """Resolve the sink strategy that owns an already prepared artifact."""

    for strategy in sink._strategy_map.values():
        if getattr(strategy, "connector", None) is sink.connector:
            return strategy
    raise ValueError("mssql_native.strategy_unavailable")


__all__ = [
    "NativePreparedResources",
    "NativeStageContext",
    "require_prepared_resources",
    "strategy_for_prepared",
]
