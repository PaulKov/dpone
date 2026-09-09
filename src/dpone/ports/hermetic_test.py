"""Port for connector-neutral hermetic strategy execution."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from dpone.contracts.hermetic_test import HermeticExecutionPlan, HermeticExecutionResult


class HermeticStrategyExecutor(Protocol):
    """Execute a negotiated plan without infrastructure or durable state."""

    def execute(
        self,
        plan: HermeticExecutionPlan,
        input_rows: Sequence[Mapping[str, Any]],
        initial_rows: Sequence[Mapping[str, Any]],
        *,
        cancelled: Callable[[], bool],
    ) -> HermeticExecutionResult: ...


__all__ = ["HermeticStrategyExecutor"]
