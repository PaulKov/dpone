"""Explicit bounded read capability; authority ownership remains with the caller."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dpone.contracts.quality_replay import TargetAcceptanceRequest


class BoundedTargetAcceptanceReader(ABC):
    """POSIX implementations must reject all output after the absolute deadline."""

    @abstractmethod
    def require_ready(self, *, cluster: str, database: str, table: str) -> None:
        """Prove lifecycle, strict settings and immutable engine before extraction."""

    def validate_plan(self, request: TargetAcceptanceRequest) -> None:
        """Reject invalid selections before publication; adapters may narrow types."""
        request.validate()

    @abstractmethod
    def collect(self, request: TargetAcceptanceRequest, *, deadline: float) -> dict[str, Any]:
        """One observation, including all replica checks, within one deadline."""

    @abstractmethod
    def verify_generation(self, request: TargetAcceptanceRequest, *, deadline: float) -> None:
        """Bounded metadata only; COMPLETE retries must not rescan target rows."""
