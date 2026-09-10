"""Observed phase concurrency; requested workers are never measured evidence."""

from __future__ import annotations

import os
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from threading import get_ident
from typing import TYPE_CHECKING, Any

from dpone.contracts.native_delivery_observations import NativeDeliveryObservation, ObservationMetric
from dpone.runtime.native_delivery_observations import BoundedNativeDeliveryObserver, NativeDeliveryRecorder

if TYPE_CHECKING:
    from dpone.ports.native_delivery_observer import NativeDeliveryObserver


def summarize_native_phases(observations: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    """Report phase span, worker-active time, peak overlap and effective parallelism.

    Durations overlap across phases and must not be summed into elapsed runtime.
    Failed imports count as active work. An absent phase has no available ratio.
    """
    result = {}
    for phase in ("encode", "import_verify"):
        selected = [row for row in observations if row["phase"] == phase]
        if not selected:
            result[phase] = dict(
                operations=0,
                failed_operations=0,
                wall_seconds=0.0,
                worker_active_seconds=0.0,
                peak_workers=0,
                effective_parallelism=None,
            )
            continue
        wall = max(row["end"] for row in selected) - min(row["start"] for row in selected)
        active = sum(row["end"] - row["start"] for row in selected)
        events = sorted((row[key], delta) for row in selected for key, delta in [("start", 1), ("end", -1)])
        current, peak = 0, 0
        for _, delta in events:
            current += delta
            peak = max(peak, current)
        result[phase] = dict(
            operations=len(selected),
            failed_operations=sum(row.get("outcome") == "failed" for row in selected),
            wall_seconds=wall,
            worker_active_seconds=active,
            peak_workers=peak,
            effective_parallelism=None if wall <= 0 else active / wall,
        )
    return result


class NativeDeliverySession:
    """Keep optional diagnostics bounded and separate from durable authorities.

    A composition shares this session across parent threads. Spawned workers
    receive only an enabled flag and return serialized records. Custom observer
    failures remain visible in ``snapshot`` even when that observer cannot write.
    """

    def __init__(self, observer: NativeDeliveryObserver | None = None) -> None:
        self.enabled = observer is not None
        self._sink = observer
        self._collector = (
            observer if isinstance(observer, BoundedNativeDeliveryObserver) else BoundedNativeDeliveryObserver()
        )

    def record(self, observation: NativeDeliveryObservation) -> None:
        self._collector.record(observation)
        if self._sink is not None and self._sink is not self._collector:
            self._sink.record(observation)

    def recorder(self, worker: str = "parent") -> NativeDeliveryRecorder:
        """Create a thread-owned recorder in this process's monotonic domain."""
        pid = os.getpid()
        return NativeDeliveryRecorder(
            self if self.enabled else None,
            clock_domain=f"process:{pid}",
            process_id=pid,
            worker_id=f"{worker}:{get_ident()}",
            diagnostics=self._collector.record_diagnostics,
        )

    def snapshot(self) -> dict[str, Any]:
        """Return a diagnostic sidecar; never insert it into a recovery journal."""
        if not self.enabled:
            self.recorder()
        return self._collector.snapshot()

    def source_rows(self, rows: Iterator[Any], adapt: Callable[[Iterator[Any]], Iterator[Any]]) -> Iterator[Any]:
        """Measure raw reads only when enabled; preserve the original adapter."""
        return ObservedNativeRows(rows, adapt) if self.enabled else adapt(rows)

    def accept_worker(self, report: dict[str, Any] | None) -> None:
        """Validate worker transport without allowing telemetry to reject data."""
        if not self.enabled or report is None:
            return
        try:
            for item in report["recorders"]:
                self._collector.record_diagnostics(item)
            for item in report["observations"]:
                self.record(NativeDeliveryObservation.from_dict(item))
        except Exception:
            # The collector makes malformed/missing diagnostic reports sticky.
            self._collector.record_diagnostics({})


def delivery_session(observer: NativeDeliveryObserver | NativeDeliverySession | None) -> NativeDeliverySession:
    """Reuse one explicitly supplied composition session, or create a bounded one."""
    return observer if isinstance(observer, NativeDeliverySession) else NativeDeliverySession(observer)


class _SourceReadWork(Iterator[Any]):
    """Accumulate actual driver next() work, excluding downstream suspension."""

    def __init__(self, rows: Iterator[Any]) -> None:
        self.rows = rows
        self.nanoseconds = 0
        self.available = True

    def __next__(self) -> Any:
        start = self._tick()
        try:
            return next(self.rows)
        finally:
            end = self._tick()
            if start is None or end is None or end < start:
                self.available = False
            else:
                self.nanoseconds += end - start

    @staticmethod
    def _tick() -> int | None:
        try:
            value = time.monotonic_ns()
            return value if type(value) is int and 0 <= value <= 2**63 - 1 else None
        except Exception:
            return None

    def close(self) -> None:
        close = getattr(self.rows, "close", None)
        if close is not None:
            close()


class ObservedNativeRows(Iterator[Any]):
    """Preserve adapter backpressure/closure with fixed-size source work counters."""

    def __init__(self, rows: Iterator[Any], adapt: Callable[[Iterator[Any]], Iterator[Any]]) -> None:
        self._source = _SourceReadWork(rows)
        self._rows = adapt(self._source)

    def __next__(self) -> Any:
        return next(self._rows)

    def close(self) -> None:
        close = getattr(self._rows, "close", None)
        if close is not None:
            close()

    def metrics(self) -> dict[str, ObservationMetric]:
        source = self._source
        metric = (
            ObservationMetric(source.nanoseconds / 1e9, "seconds", "measured", None, "sum_next_monotonic_spans")
            if source.available
            else ObservationMetric(None, "seconds", "unavailable", "clock_unavailable", "sum_next_monotonic_spans")
        )
        source.nanoseconds = 0
        return {
            "source_read_work_seconds": metric,
            "source_adapt_work_seconds": ObservationMetric(
                None, "seconds", "unavailable", "inclusive_source_boundary", "source_adapter"
            ),
        }


@contextmanager
def frame_observation(recorder: NativeDeliveryRecorder, rows: Iterator[Any], ordinal: int) -> Iterator[None]:
    """Measure frame building inclusively; source work is a separate work metric."""
    metrics: dict[str, ObservationMetric] = {}
    with recorder.phase("frame_build", reason="inclusive_frame_build", ordinal=ordinal, metrics=metrics):
        try:
            yield
        finally:
            if isinstance(rows, ObservedNativeRows):
                metrics.update(rows.metrics())
