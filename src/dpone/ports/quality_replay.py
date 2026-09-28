"""Explicit durable replay capability; diagnostic DTOs never grant authority."""

from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import AbstractContextManager
from typing import Any

from dpone.contracts import quality_replay as contracts
from dpone.contracts import quality_replay_selection, target_acceptance
from dpone.contracts.strict_json import canonical_json_bytes as canonical_json_bytes

selection_contracts = quality_replay_selection
target_contracts = target_acceptance
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

    @property
    def target_reader(self) -> Any:
        """An explicitly injected bounded capability; v1 stores need none."""
        return None

    def reader_token(self, load_config: Any) -> str:
        raise contracts.ReplayQualityEvidenceError("UNSUPPORTED")

    def retain_guard(self, load_config: Any) -> None:
        """Prevent retirement after uncertain worker or authority outcomes."""
        raise contracts.ReplayQualityEvidenceError("UNSUPPORTED")

    def transition(
        self, load_config: Any, capsule: QualityReplayCapsule, state: str, *, target: dict[str, Any] | None = None
    ) -> QualityReplayCapsule:
        raise contracts.ReplayQualityEvidenceError("UNSUPPORTED")
