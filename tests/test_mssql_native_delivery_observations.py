"""Contract coverage for opt-in diagnostics; no live-performance authority."""

from dataclasses import FrozenInstanceError

import pytest

from dpone.contracts.native_delivery_observations import NativeDeliveryObservation, ObservationMetric
from dpone.runtime.native_delivery_observations import BoundedNativeDeliveryObserver, NativeDeliveryRecorder


def observation(**changes):
    values = dict(
        phase="encode",
        reason=None,
        clock_domain="host-a",
        process_id=1,
        worker_id="worker-1",
        ordinal=0,
        attempt_id="attempt-1",
        start_monotonic_ns=0,
        end_monotonic_ns=10,
        outcome="completed",
        rows=2,
        encoded_bytes=16,
        metrics={},
    )
    return NativeDeliveryObservation(**(values | changes))


def test_immutable_observation_and_explicit_unavailable_metric():
    metric = ObservationMetric(None, "bytes", "unavailable", "provider_absent", "process_rss")
    metrics = {"rss": metric}
    event = observation(metrics=metrics)
    metrics.clear()
    assert event.to_dict()["metrics"]["rss"]["value"] is None
    with pytest.raises((FrozenInstanceError, AttributeError)):
        event.rows = 3
    with pytest.raises(TypeError):
        event.metrics["new"] = metric


def test_injected_nested_clock_and_separate_delivery_pipeline_spans():
    observer = BoundedNativeDeliveryObserver(max_observations=4)
    ticks = iter([0, 1, 2, 3, 5, 8, 9, 10])
    recorder = NativeDeliveryRecorder(
        observer, clock=lambda: next(ticks), clock_domain="host-a", process_id=1, worker_id="parent"
    )
    with recorder.duration("pipeline"):
        with recorder.duration("delivery"):
            with recorder.phase("prepare_insert"):
                with recorder.phase("metadata_project"):
                    pass
    report = recorder.snapshot()
    assert report["durations"]["delivery"]["value"] == 8e-9
    assert report["durations"]["pipeline"]["value"] == 1e-8
    assert len(observer.snapshot()["observations"]) == 2


def test_overlap_counts_distinct_workers_and_never_combines_clock_domains():
    observer = BoundedNativeDeliveryObserver()
    for event in [
        observation(start_monotonic_ns=2, end_monotonic_ns=8),
        observation(start_monotonic_ns=3, end_monotonic_ns=6),
        observation(worker_id="worker-2", start_monotonic_ns=5, end_monotonic_ns=10),
        observation(worker_id="worker-3", start_monotonic_ns=10, end_monotonic_ns=12),
        observation(clock_domain="host-b", worker_id="worker-4"),
    ]:
        observer.record(event)
    aggregates = observer.snapshot()["aggregates"]
    assert [(a["clock_domain"], a["observed_worker_overlap"]["value"]) for a in aggregates] == [
        ("host-a", 2),
        ("host-b", 1),
    ]
    assert "parallelism" not in str(aggregates)


def test_overflow_bounds_all_retention_and_invalidates_overlap():
    observer = BoundedNativeDeliveryObserver(max_observations=2)
    for index in range(100):
        observer.record(observation(worker_id=f"worker-{index}", clock_domain=f"host-{index}"))
    report = observer.snapshot()
    assert report["status"] == "UNVERIFIED"
    assert len(report["observations"]) == len(report["aggregates"]) == 2
    assert all(a["observed_worker_overlap"]["value"] is None for a in report["aggregates"])


def test_failures_cancellations_and_retries_are_separate_work():
    import asyncio

    ticks = iter(range(10))
    observer = BoundedNativeDeliveryObserver()
    recorder = NativeDeliveryRecorder(
        observer, clock=lambda: next(ticks), clock_domain="host", process_id=1, worker_id="worker"
    )
    for attempt, error in [("first", ValueError), ("second", asyncio.CancelledError)]:
        with pytest.raises(error), recorder.phase("bcp", ordinal=0, attempt_id=attempt):
            raise error("sensitive business failure")
    with recorder.phase("bcp", ordinal=0, attempt_id="third"):
        pass
    report = observer.snapshot()
    assert [o["outcome"] for o in report["observations"]] == ["failed", "cancelled", "completed"]
    assert [o["attempt_id"] for o in report["observations"]] == ["first", "second", "third"]
    assert "sensitive" not in str(report)


def test_observer_failure_and_missing_clock_do_not_mask_original_exception():
    class Broken:
        def record(self, event):
            raise ValueError("secret-connection-url")

    recorder = NativeDeliveryRecorder(Broken(), clock=lambda: 1, clock_domain="host", process_id=1, worker_id="worker")
    with pytest.raises(RuntimeError, match="business"), recorder.phase("bcp"):
        raise RuntimeError("business")
    assert recorder.snapshot()["limitations"] == ["observer_failed"]
    assert "secret" not in str(recorder.snapshot())
    recorder = NativeDeliveryRecorder(
        Broken(), clock=lambda: None, clock_domain="host", process_id=1, worker_id="worker"
    )
    with recorder.phase("bcp"):
        pass
    assert recorder.snapshot()["status"] == "UNVERIFIED"
    assert recorder.snapshot()["limitations"] == ["clock_unavailable"]


def test_disabled_observer_does_not_read_clock_or_change_results():
    def poison():
        raise AssertionError("clock must not be read")

    recorder = NativeDeliveryRecorder(clock=poison, clock_domain="host", process_id=1, worker_id="worker")
    with recorder.duration("delivery"), recorder.phase("bcp"):
        result = 7
    assert result == 7
    assert recorder.snapshot()["durations"]["delivery"]["value"] is None


@pytest.mark.parametrize(
    "changes",
    [
        dict(phase="sql"),
        dict(outcome="PASS"),
        dict(rows=-1),
        dict(rows=True),
        dict(schema_version=2),
        dict(end_monotonic_ns=-1),
        dict(reason="SELECT * FROM secret"),
        dict(worker_id="mssql://username:password@host"),
        dict(clock_domain="x" * 129),
    ],
)
def test_invalid_observation_is_rejected(changes):
    with pytest.raises(ValueError):
        observation(**changes)


@pytest.mark.parametrize(
    "value,availability,reason",
    [
        (0, "unavailable", "missing"),
        (None, "unavailable", None),
        (float("nan"), "measured", None),
        (float("inf"), "measured", None),
        (True, "measured", None),
        (3, "measured", "missing"),
    ],
)
def test_metric_availability_is_explicit(value, availability, reason):
    with pytest.raises(ValueError):
        ObservationMetric(value, "bytes", availability, reason, "process_rss")


def test_worker_serialization_roundtrip_and_recorder_failure_merge():
    event = observation(
        metrics={"rss": ObservationMetric(None, "bytes", "unavailable", "provider_absent", "parent_only")}
    )
    assert NativeDeliveryObservation.from_dict(event.to_dict()) == event
    observer = BoundedNativeDeliveryObserver()
    recorder = NativeDeliveryRecorder(
        observer, clock=lambda: None, clock_domain="host", process_id=1, worker_id="worker"
    )
    with recorder.phase("encode"):
        pass
    report = observer.snapshot(recorder_reports=[recorder.snapshot()])
    assert report["status"] == "UNVERIFIED"
    assert "clock_unavailable" in report["limitations"]


def test_invalid_recorder_identity_never_emits_values_or_changes_business_result():
    collector = BoundedNativeDeliveryObserver(max_observations=1)
    recorder = collector.recorder(clock_domain="host", process_id=1, worker_id="mssql://user:secret@host")
    with recorder.phase("bcp"):
        result = 5
    report = collector.snapshot()
    assert result == 5 and report["status"] == "UNVERIFIED"
    assert "secret" not in str(report)
    assert "invalid_identity" in report["limitations"]
    collector.record_diagnostics({"worker_id": "secret" * 100000})
    assert len(str(collector.snapshot())) < 3000


def test_nested_repeated_duration_stays_unavailable():
    collector = BoundedNativeDeliveryObserver()
    ticks = iter(range(4))
    recorder = collector.recorder(clock=lambda: next(ticks), clock_domain="host", process_id=1, worker_id="worker")
    with recorder.duration("delivery"), recorder.duration("delivery"):
        pass
    assert recorder.snapshot()["durations"]["delivery"]["value"] is None
    assert collector.snapshot()["status"] == "UNVERIFIED"
