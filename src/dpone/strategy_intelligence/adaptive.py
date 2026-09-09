from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class BatchObservation:
    rows: int
    duration_seconds: float
    target_backpressure: float = 0.0


@dataclass(frozen=True, slots=True)
class BatchAdjustment:
    previous_batch_size: int
    next_batch_size: int
    rows_per_second: float
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class AdaptiveBatchController:
    """Adjust batch size from observed throughput and target backpressure."""

    def __init__(self, *, initial_batch_size: int, min_batch_size: int, max_batch_size: int) -> None:
        self._current = int(initial_batch_size)
        self._min = int(min_batch_size)
        self._max = int(max_batch_size)

    def observe(self, observation: BatchObservation) -> BatchAdjustment:
        previous = self._current
        rows_per_second = observation.rows / max(observation.duration_seconds, 0.001)
        if observation.target_backpressure >= 0.75:
            next_size = max(self._min, int(previous * 0.5))
            reason = "target_backpressure"
        elif rows_per_second >= previous * 0.75 and observation.target_backpressure <= 0.20:
            next_size = min(self._max, int(previous * 1.5))
            reason = "increase_throughput"
        else:
            next_size = previous
            reason = "hold"
        self._current = next_size
        return BatchAdjustment(
            previous_batch_size=previous,
            next_batch_size=next_size,
            rows_per_second=rows_per_second,
            reason=reason,
        )
