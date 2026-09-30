"""Method choice and one-shot recovery; fakes are not backend certification."""

from contextlib import contextmanager
from dataclasses import replace

import pytest

from dpone.contracts.clickhouse_publication import (
    PublicationObservation,
    PublicationRecord,
    PublicationState,
    PublicationTable,
    choose_publication,
)
from dpone.runtime.sinks.clickhouse_guarded_publication import GuardedClickHousePublication, PublicationUnknown


def table(uuid="old", digest="a", **kwargs):
    values = dict(uuid=uuid, design_digest="d" * 64, content_digest=digest * 64, rows=2, partitions=("all",))
    values.update(kwargs)
    return PublicationTable(**values)


def observation(**kwargs):
    values = dict(
        subject=("endpoint", "database", "target", "stage"),
        database_engine="Atomic",
        target=table(),
        candidate=table("new", "b"),
        catalog_complete=True,
        side_effects_safe=True,
    )
    values.update(kwargs)
    return PublicationObservation(**values)


@pytest.mark.parametrize(
    "changes,method,reason",
    [
        ({}, "replace_partition", "single_complete_partition"),
        ({"target": None}, "rename", "target_absent"),
        ({"candidate": table("new", "b", rows=0, partitions=())}, "exchange", "empty_snapshot"),
        ({"target": table(partitions=("all", "stale"))}, "exchange", "multiple_or_different_partitions"),
        ({"candidate": table("new", "b", partitions=("one", "two"))}, "exchange", "multiple_or_different_partitions"),
        ({"candidate": table("new", "b", design_digest="e" * 64)}, "exchange", "design_changed"),
        ({"candidate": table("new", "a")}, "noop", "identical_snapshot"),
    ],
)
def test_selection(changes, method, reason):
    intent = choose_publication("op", observation(**changes))
    assert (intent.method, intent.reason) == (method, reason)


@pytest.mark.parametrize(
    "changes",
    [
        {"catalog_complete": False},
        {"side_effects_safe": False},
        {"database_engine": "Shared"},
        {"candidate": table("old", "b")},
        {"candidate": table("new", "b", engine="ReplicatedMergeTree")},
    ],
)
def test_incomplete_or_unsupported_observation_blocks(changes):
    with pytest.raises(ValueError):
        choose_publication("op", observation(**changes))


class Backend:
    """Unit-only protected backend simulator, never production authority."""

    def __init__(self, observed=None):
        self.observed = observed or observation()
        self.record = None
        self.dispatches = 0
        self.closed = False
        self.fault = None

    @contextmanager
    def hold(self, operation_id):
        yield

    def read(self, operation_id):
        return self.record

    def observe(self, operation_id):
        return self.observed

    def prepare(self, intent):
        self.record = PublicationRecord(intent, PublicationState.PREPARED)
        return self.record

    def claim(self, record):
        assert self.record == record
        self.record = replace(record, state=PublicationState.CLAIMED, claim_granted=True)
        return self.record

    def execute_once(self, intent):
        assert self.record.state == PublicationState.CLAIMED
        self.dispatches += 1
        if self.fault == "before":
            raise OSError("lost reply before effect")
        old, new = self.observed.target, self.observed.candidate
        if intent.method == "replace_partition":
            self.observed = replace(self.observed, target=replace(new, uuid=old.uuid))
        elif intent.method == "exchange":
            self.observed = replace(self.observed, target=new, candidate=old)
        elif intent.method == "rename":
            self.observed = replace(self.observed, target=new, candidate=None)
        if self.fault == "after":
            raise OSError("lost reply after effect")

    def close_and_drain(self, intent):
        if self.fault == "closure":
            raise OSError("closure not proven")
        self.closed = True

    def resolve(self, record, state, observed):
        assert self.closed
        assert self.record == record
        self.record = replace(record, state=state)
        return self.record


@pytest.mark.parametrize("fault,expected", [(None, "committed"), ("after", "committed"), ("before", "not_published")])
@pytest.mark.parametrize("changes", [{}, {"target": table(partitions=("all", "old"))}, {"target": None}])
def test_one_shot_and_source_free_recovery(fault, expected, changes):
    backend = Backend(observation(**changes))
    backend.fault = fault
    service = GuardedClickHousePublication(backend)
    assert service.publish("op").state == expected
    assert service.recover("op").state == expected
    assert service.publish("op").state == expected
    assert backend.dispatches == 1


def test_noop_does_not_execute_ddl():
    backend = Backend(observation(candidate=table("new", "a")))
    result = GuardedClickHousePublication(backend).publish("op")
    assert result.state == "committed"
    assert backend.dispatches == 0


def test_prepared_recovery_never_dispatches():
    backend = Backend()
    backend.prepare(choose_publication("op", backend.observed))
    assert GuardedClickHousePublication(backend).recover("op").state == "not_published"
    assert backend.dispatches == 0


def test_unknown_closure_retains_record_and_never_retries():
    backend = Backend()
    backend.fault = "closure"
    service = GuardedClickHousePublication(backend)
    with pytest.raises(PublicationUnknown):
        service.publish("op")
    assert backend.record.state == "claimed"
    backend.fault = None
    assert service.recover("op").state == "committed"
    assert backend.dispatches == 1


def test_changed_candidate_is_not_accepted_as_commit():
    backend = Backend()
    backend.fault = "closure"
    service = GuardedClickHousePublication(backend)
    with pytest.raises(PublicationUnknown):
        service.publish("op")
    backend.fault = None
    backend.observed = replace(backend.observed, candidate=table("new", "c"))
    with pytest.raises(PublicationUnknown):
        service.recover("op")
    assert backend.record.state == "unknown"
    assert backend.dispatches == 1


@pytest.mark.parametrize("after", [False, True])
def test_cancellation_closes_publisher_and_preserves_primary(after):
    class Cancelled(Backend):
        def execute_once(self, intent):
            if after:
                super().execute_once(intent)
            raise KeyboardInterrupt("cancelled")

    backend = Cancelled()
    with pytest.raises(KeyboardInterrupt, match="cancelled"):
        GuardedClickHousePublication(backend).publish("op")
    assert backend.closed
    assert backend.record.state == ("committed" if after else "not_published")


def test_losing_claim_never_dispatches():
    class Losing(Backend):
        def claim(self, record):
            super().claim(record)
            return None

    backend = Losing()
    assert GuardedClickHousePublication(backend).publish("op").state == "not_published"
    assert backend.dispatches == 0


@pytest.mark.parametrize("boundary", ["prepare", "claim", "resolve"])
def test_ambiguous_durable_ack_can_only_recover(boundary):
    backend = Backend()
    original = getattr(backend, boundary)

    def lose_ack(*args):
        original(*args)
        raise OSError("lost durable ACK")

    setattr(backend, boundary, lose_ack)
    service = GuardedClickHousePublication(backend)
    with pytest.raises((OSError, PublicationUnknown)):
        service.publish("op")
    setattr(backend, boundary, original)
    result = service.recover("op")
    assert result.state == ("committed" if boundary == "resolve" else "not_published")
    assert backend.dispatches == (1 if boundary == "resolve" else 0)


def test_pre_dispatch_drift_closes_without_ddl():
    class Drift(Backend):
        def claim(self, record):
            result = super().claim(record)
            self.observed = replace(self.observed, candidate=table("new", "c"))
            return result

    backend = Drift()
    with pytest.raises(PublicationUnknown):
        GuardedClickHousePublication(backend).publish("op")
    assert backend.closed
    assert backend.dispatches == 0


def test_unclaimed_unknown_never_becomes_committed():
    backend = Backend()
    intent = choose_publication("op", backend.observed)
    backend.prepare(intent)
    backend.observed = replace(backend.observed, target=table("old", "c"))
    service = GuardedClickHousePublication(backend)
    with pytest.raises(PublicationUnknown):
        service.recover("op")
    backend.observed = replace(backend.observed, target=table("old", "b"))
    with pytest.raises(PublicationUnknown):
        service.recover("op")
    assert not backend.record.claim_granted
    assert backend.dispatches == 0


@pytest.mark.parametrize("fault", ["operation", "version", "subject"])
def test_foreign_record_or_observation_never_dispatches(fault):
    backend = Backend()
    intent = choose_publication("op", backend.observed)
    backend.prepare(intent)
    if fault == "operation":
        backend.record = replace(backend.record, intent=replace(intent, operation_id="foreign"))
    elif fault == "version":
        backend.record = replace(backend.record, schema_version="unknown")
    else:
        backend.observed = replace(backend.observed, subject=("foreign", "database", "target", "stage"))
    with pytest.raises(PublicationUnknown):
        GuardedClickHousePublication(backend).recover("op")
    assert backend.dispatches == 0
