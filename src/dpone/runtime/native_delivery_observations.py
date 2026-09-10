"""Bounded optional phase collection and injected-clock instrumentation.

No SQL, resource probing, journal mutation, or runtime call-site wiring occurs
here. Clock domains must be assigned by the composition owner, not inferred.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import copy
import threading
import time
from collections import Counter, defaultdict
from collections.abc import Callable, Iterable, Iterator, Mapping
from contextlib import contextmanager
from typing import Any, TypedDict

from dpone.contracts.native_delivery_observations import NativeDeliveryObservation, ObservationMetric, diagnostic_token
from dpone.ports.native_delivery_observer import NativeDeliveryObserver


class _RecorderIdentity(TypedDict):
    clock_domain: str
    process_id: int
    worker_id: str


_DIAGNOSTICS = frozenset(
    {
        "clock_unavailable",
        "observer_failed",
        "duration_unavailable",
        "observer_disabled",
        "diagnostic_sink_failed",
        "invalid_identity",
        "invalid_recorder_report",
    }
)
_SNAPSHOT_FIELDS = frozenset(
    "schema_version kind status limitations recorders capacity observations aggregates".split()
)


def _identity_valid(identity: Mapping[str, Any]) -> bool:
    try:
        diagnostic_token(identity["clock_domain"])
        diagnostic_token(identity["worker_id"])
        return type(identity["process_id"]) is int and 0 <= identity["process_id"] <= 2**63 - 1
    except (KeyError, TypeError, ValueError):
        return False


def _valid_report(report: dict[str, Any]) -> bool:
    try:
        if (
            set(report)
            != {"schema_version", "clock_domain", "process_id", "worker_id", "status", "limitations", "durations"}
            or type(report["schema_version"]) is not int
            or report["schema_version"] != 1
            or not _identity_valid(report)
            or report["status"] not in {"PASS", "UNVERIFIED"}
            or not isinstance(report["limitations"], list)
            or len(report["limitations"]) > len(_DIAGNOSTICS)
            or not set(report["limitations"]) <= _DIAGNOSTICS
            or not isinstance(report["durations"], dict)
            or set(report["durations"]) != {"delivery", "pipeline"}
        ):
            return False
        for value in report["durations"].values():
            metric = ObservationMetric(**value)
            if metric.unit != "seconds" or (metric.value is not None and metric.value < 0):
                return False
        return True
    except (KeyError, TypeError, ValueError):
        return False


def unavailable(reason: str, unit: str = "seconds", provenance: str = "monotonic_span") -> dict[str, Any]:
    """Produce explicit absence; configured limits are never measurements."""
    return ObservationMetric(None, unit, "unavailable", reason, provenance).to_dict()


class BoundedNativeDeliveryObserver:
    """Thread-safe bounded retention; overflow invalidates aggregate claims.

    Memory is O(max_observations), including all aggregation cardinalities.
    Producers forward process-local immutable records to this parent collector.
    No collector/client is implicitly shared across spawned processes.
    """

    def __init__(self, *, max_observations: int = 4096) -> None:
        if type(max_observations) is not int or not 1 <= max_observations <= 65536:
            raise ValueError("observation.invalid_capacity")
        self._capacity = max_observations
        self._observations: list[NativeDeliveryObservation] = []
        self._overflow = False
        self._invalid_report = False
        self._reports: dict[tuple[str, int, str], dict[str, Any]] = {}
        self._lock = threading.Lock()

    def record(self, observation: NativeDeliveryObservation) -> None:
        """Keep separate attempts; refuse unbounded retention without blocking work."""
        if not isinstance(observation, NativeDeliveryObservation):
            raise ValueError("observation.invalid_record")
        with self._lock:
            if len(self._observations) == self._capacity:
                self._overflow = True
            else:
                self._observations.append(observation)

    def recorder(
        self, *, clock_domain: str, process_id: int, worker_id: str, clock: Callable[[], int] = time.monotonic_ns
    ) -> NativeDeliveryRecorder:
        """Create a recorder with an explicit failure channel back to this sink."""
        return NativeDeliveryRecorder(
            self,
            clock=clock,
            clock_domain=clock_domain,
            process_id=process_id,
            worker_id=worker_id,
            diagnostics=self.record_diagnostics,
        )

    @classmethod
    def from_snapshot(cls, payload: dict[str, Any]) -> BoundedNativeDeliveryObserver:
        """Rebuild bounded typed records; consumers verify derived snapshot fields.

        Aggregates and status are recomputed, never taken as acceptance authority
        from serialized labels. Irrecoverable loss flags remain explicit.
        """
        if (
            set(payload) != _SNAPSHOT_FIELDS
            or type(payload["schema_version"]) is not int
            or payload["schema_version"] != 1
        ):
            raise ValueError("observation.invalid_snapshot")
        collector = cls(max_observations=payload["capacity"])
        if any(
            not isinstance(payload[field], list) or len(payload[field]) > collector._capacity
            for field in ("observations", "recorders")
        ) or not isinstance(payload["limitations"], list):
            raise ValueError("observation.invalid_snapshot")
        for item in payload["observations"]:
            collector.record(NativeDeliveryObservation.from_dict(item))
        for report in payload["recorders"]:
            if not _valid_report(report):
                raise ValueError("observation.invalid_recorder_report")
            collector.record_diagnostics(report)
        collector._overflow = "capacity_exceeded" in payload["limitations"]
        collector._invalid_report = "invalid_or_failed_recorder" in payload["limitations"]
        return collector

    def record_diagnostics(self, report: dict[str, Any]) -> None:
        """Merge the latest worker snapshot, including worker-process failures.

        Input is a recorder-produced snapshot. Identity cardinality is bounded by
        the same capacity as observations; overflow invalidates the sidecar.
        """
        if not _valid_report(report):
            with self._lock:
                self._invalid_report = True
            return
        key = (report["clock_domain"], report["process_id"], report["worker_id"])
        with self._lock:
            if key not in self._reports and len(self._reports) == self._capacity:
                self._overflow = True
                return
            previous = self._reports.get(key)
            merged = copy.deepcopy(report)
            if previous is not None and previous["status"] != "PASS":
                merged["status"] = "UNVERIFIED"
                merged["limitations"] = sorted(set(previous["limitations"] + merged["limitations"]))
            self._reports[key] = merged

    def snapshot(self, *, recorder_reports: Iterable[dict[str, Any]] = ()) -> dict[str, Any]:
        """Copy raw spans and per-phase/domain work; never sum domains as wall time."""
        for index, report in enumerate(recorder_reports):
            if index >= self._capacity:
                with self._lock:
                    self._overflow = True
                break
            self.record_diagnostics(report)
        with self._lock:
            observations, overflow = tuple(self._observations), self._overflow
            reports = copy.deepcopy(list(self._reports.values()))
            invalid_report = self._invalid_report
        limitations = set(code for report in reports for code in report["limitations"])
        if overflow:
            limitations.add("capacity_exceeded")
        if invalid_report or any(report["status"] != "PASS" for report in reports):
            limitations.add("invalid_or_failed_recorder")
        groups: dict[tuple[str, str], list[NativeDeliveryObservation]] = defaultdict(list)
        for item in observations:
            groups[item.phase, item.clock_domain].append(item)
        aggregates = []
        for (phase, domain), spans in sorted(groups.items()):
            work = sum(span.end_monotonic_ns - span.start_monotonic_ns for span in spans) / 1e9
            aggregates.append(
                {
                    "phase": phase,
                    "clock_domain": domain,
                    "attempts": dict(Counter(span.outcome for span in spans)),
                    "work_seconds": unavailable("capacity_exceeded")
                    if overflow
                    else ObservationMetric(work, "seconds", "measured", None, "sum_of_spans").to_dict(),
                    "observed_worker_overlap": unavailable("capacity_exceeded", "workers")
                    if overflow
                    else ObservationMetric(
                        _overlap(spans), "workers", "measured", None, "distinct_active_workers"
                    ).to_dict(),
                }
            )
        return {
            "schema_version": 1,
            "kind": "native-delivery-observations",
            "status": "UNVERIFIED" if limitations else "PASS",
            "limitations": sorted(limitations),
            "recorders": reports,
            "capacity": self._capacity,
            "observations": [item.to_dict() for item in observations],
            "aggregates": aggregates,
        }


def _overlap(spans: list[NativeDeliveryObservation]) -> int:
    events: list[tuple[int, int, tuple[int, str]]] = []
    for span in spans:
        if span.end_monotonic_ns > span.start_monotonic_ns:
            worker = (span.process_id, span.worker_id)
            events.extend(((span.start_monotonic_ns, 1, worker), (span.end_monotonic_ns, -1, worker)))
    active: Counter[tuple[int, str]] = Counter()
    peak = 0
    for _, delta, worker in sorted(events):  # End before start: half-open intervals.
        active[worker] += delta
        if active[worker] == 0:
            del active[worker]
        peak = max(peak, len(active))
    return peak


class NativeDeliveryRecorder:
    """Failure-isolated phase scopes with a separate diagnostic result channel.

    Omission is a true no-op, including no clock reads. One recorder belongs to
    one worker/clock domain. Use snapshot() even when the sink fails; never rely
    on successful record() calls alone to claim complete diagnostic coverage.
    """

    def __init__(
        self,
        observer: NativeDeliveryObserver | None = None,
        *,
        clock: Callable[[], int] = time.monotonic_ns,
        clock_domain: str,
        process_id: int,
        worker_id: str,
        diagnostics: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        self._observer, self._clock = observer, clock
        self._identity = _RecorderIdentity(clock_domain=clock_domain, process_id=process_id, worker_id=worker_id)
        self._diagnostics: set[str] = set()
        if not _identity_valid(self._identity):
            self._identity = _RecorderIdentity(clock_domain="unavailable", process_id=0, worker_id="unavailable")
            self._diagnostics.add("invalid_identity")
        self._durations: dict[str, dict[str, Any]] = {}
        self._invalid_durations: set[str] = set()
        self._diagnostic_sink = diagnostics
        self._publish_diagnostics()

    def _publish_diagnostics(self) -> None:
        if self._diagnostic_sink is not None:
            try:
                self._diagnostic_sink(self.snapshot())
            except Exception:
                self._diagnostics.add("diagnostic_sink_failed")

    def _tick(self) -> int | None:
        try:
            value = self._clock()
            if type(value) is not int or not 0 <= value <= 2**63 - 1:
                raise ValueError("invalid_clock")
            return value
        except Exception:
            self._diagnostics.add("clock_unavailable")
            return None

    @contextmanager
    def phase(
        self,
        phase: str,
        *,
        reason: str | None = None,
        ordinal: int | None = None,
        attempt_id: str | None = None,
        rows: int | None = None,
        encoded_bytes: int | None = None,
        metrics: Mapping[str, ObservationMetric] | None = None,
    ) -> Iterator[None]:
        """Record complete/failed/cancelled work without masking the business error."""
        if self._observer is None:
            yield
            return
        start, outcome = self._tick(), "completed"
        try:
            yield
        except (asyncio.CancelledError, concurrent.futures.CancelledError, KeyboardInterrupt):
            outcome = "cancelled"
            raise
        except BaseException:
            outcome = "failed"
            raise
        finally:
            end = self._tick()
            if start is not None and end is not None:
                try:
                    self._observer.record(
                        NativeDeliveryObservation(
                            phase=phase,
                            reason=reason,
                            ordinal=ordinal,
                            attempt_id=attempt_id,
                            start_monotonic_ns=start,
                            end_monotonic_ns=end,
                            outcome=outcome,
                            rows=rows,
                            encoded_bytes=encoded_bytes,
                            metrics=metrics or {},
                            **self._identity,
                        )
                    )
                except Exception:
                    self._diagnostics.add("observer_failed")
            self._publish_diagnostics()

    @contextmanager
    def duration(self, name: str) -> Iterator[None]:
        """Measure delivery through visibility, or pipeline through checkpoint.

        The caller places the boundaries. No inferred commit/visibility success
        occurs here; exceptions or repeated boundaries make this metric absent.
        """
        if name not in {"delivery", "pipeline"}:
            raise ValueError("observation.invalid_duration")
        if self._observer is None:
            yield
            return
        if name in self._durations:
            self._invalid_durations.add(name)
        self._durations[name] = unavailable("span_incomplete")
        start, complete = self._tick(), False
        try:
            yield
            complete = True
        finally:
            end = self._tick()
            if (
                complete
                and name not in self._invalid_durations
                and start is not None
                and end is not None
                and end >= start
            ):
                self._durations[name] = ObservationMetric(
                    (end - start) / 1e9, "seconds", "measured", None, "monotonic_span"
                ).to_dict()
            else:
                self._durations[name] = unavailable("span_failed_or_repeated")
                self._diagnostics.add("duration_unavailable")
            self._publish_diagnostics()

    def snapshot(self) -> dict[str, Any]:
        """Read this recorder's failure channel separately from its optional sink."""
        return {
            "schema_version": 1,
            **self._identity,
            "status": "UNVERIFIED" if self._diagnostics or self._observer is None else "PASS",
            "limitations": sorted(self._diagnostics) if self._observer is not None else ["observer_disabled"],
            "durations": {
                name: dict(self._durations.get(name, unavailable("not_observed"))) for name in ("delivery", "pipeline")
            },
        }
