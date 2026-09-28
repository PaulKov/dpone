"""Explicit durable replay capability; diagnostic DTOs never grant authority."""

from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import AbstractContextManager
from typing import Any

from dpone.contracts import quality_replay as contracts
from dpone.contracts.strict_json import canonical_json_bytes as canonical_json_bytes

QualityReplayCapsule = contracts.QualityReplayCapsule


class QualityReplayStore(ABC):
    """Trusted adapter selected at composition, with an exact generation guard.

    All managed writers must honor the guard and incomplete-governance fence.
    `complete` runs inside `committed` and returns the exact persisted capsule.
    Implementations must not obtain source data or dispatch target publication.
    """

    @abstractmethod
    def require_ready(self, load_config: Any) -> None: ...

    @abstractmethod
    def stage(self, load_config: Any, handle: Any, core: dict[str, Any]) -> None: ...

    @abstractmethod
    def committed(self, load_config: Any) -> AbstractContextManager[QualityReplayCapsule]: ...

    @abstractmethod
    def complete(self, load_config: Any, capsule: QualityReplayCapsule) -> QualityReplayCapsule: ...
