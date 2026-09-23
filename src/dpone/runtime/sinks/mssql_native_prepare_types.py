"""Immutable composition values for native stage preparation."""

from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Any

from dpone.runtime.mssql_native_chunks_observations import delivery_session


@dataclass(frozen=True)
class NativeStageContext:
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
    settle_parent: Callable[[], Any] | None = None
    abort_parent: Callable[[Any], None] | None = None
    terminal_result: Callable[[Any], Any] | None = None
    physical_stage: Callable[[Any], Any] | None = None
    observer: Any = field(default=None, kw_only=True)

    def __post_init__(self) -> None:
        object.__setattr__(self, "observer", delivery_session(self.observer))


@dataclass(frozen=True)
class PreparedResources:
    context: NativeStageContext
    receipts: tuple[Any, ...]
    verify_digest: str
    source_schema: tuple[tuple[str, str], ...]
    object_id: int
    planned: dict[str, str]
