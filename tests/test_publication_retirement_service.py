"""Application policy at observation/storage boundaries, without live claims."""

from contextlib import contextmanager
from dataclasses import replace
from importlib import import_module

import pytest

from tests.test_publication_retirement import observation


class Observer:
    def __init__(self):
        self.value = observation()
        self.held = False
        self.valid = True
        self.observed = 0

    @contextmanager
    def hold(self):
        self.held = True
        try:
            yield self
        finally:
            self.held = False

    def observe(self):
        self.observed += 1
        self.require_held()
        return self.value

    def require_held(self):
        if not self.held or not self.valid:
            raise RuntimeError("synthetic freeze invalidated")


class Store:
    """No ClickHouse capability; record SQL-boundary calls while exclusion held."""

    def __init__(self, observer):
        self.observer = observer
        self.calls = []
        self.before = "absent"
        self.after = "exact"
        self.write = "acknowledged"

    def inspect(self, plan):
        assert self.observer.held
        self.calls.append(("inspect", plan.digest))
        return self.after if any(item[0] == "retire" for item in self.calls) else self.before

    def retire_if_absent(self, plan):
        assert self.observer.held
        self.calls.append(("retire", plan.digest))
        return self.write


class Attempts:
    def __init__(self):
        self.claimed = set()

    def claim(self, digest):
        if digest in self.claimed:
            return False
        self.claimed.add(digest)
        return True


def setup(attempts=None):
    service = import_module("dpone.services.publication_retirement")
    observer = Observer()
    store = Store(observer)
    return (
        service.PublicationRetirementService(
            observer=observer, store=store, attempts=attempts or Attempts(), clock=lambda: 220
        ),
        observer,
        store,
    )


def test_plan_reads_deployment_observations_without_sql_writes():
    service, observer, store = setup()
    plan = service.plan()
    assert plan.original.record.operation_id == "operation"
    assert observer.observed == 1 and not observer.held
    assert not store.calls


def test_apply_revalidates_then_inserts_once_and_reads_back_under_held_freeze():
    service, observer, store = setup()
    plan = service.plan()
    result = service.apply(plan, confirmation_digest=plan.digest)
    assert result.status == "retired_unpublished"
    assert result.plan_digest == plan.digest
    assert [kind for kind, _ in store.calls] == ["inspect", "retire", "inspect"]
    assert observer.observed == 2 and not observer.held
    assert not hasattr(result, "permit")


def test_wrong_confirmation_does_not_observe_or_write():
    service, observer, store = setup()
    plan = service.plan()
    with pytest.raises(ValueError):
        service.apply(plan, confirmation_digest="0" * 64)
    assert observer.observed == 1 and store.calls == []


def test_change_between_plan_and_apply_blocks_before_store_io():
    service, observer, store = setup()
    plan = service.plan()
    observer.value = replace(observer.value, source_location_digest="4" * 64)
    with pytest.raises(ValueError):
        service.apply(plan, confirmation_digest=plan.digest)
    assert not store.calls


def test_changed_writer_exclusion_blocks_before_store_io():
    service, observer, store = setup()
    plan = service.plan()
    observer.value = replace(observer.value, freeze=replace(observer.value.freeze, excluded_writers=("worker",)))
    with pytest.raises(ValueError):
        service.apply(plan, confirmation_digest=plan.digest)
    assert not store.calls


def test_existing_exact_retirement_is_read_only_idempotent_success():
    service, _, store = setup()
    plan = service.plan()
    store.before = "exact"
    assert service.apply(plan, confirmation_digest=plan.digest).status == "retired_unpublished"
    assert [kind for kind, _ in store.calls] == ["inspect"]


@pytest.mark.parametrize(
    "before,expected", [("conflict", "blocked"), ("unknown", "outcome_unknown"), ("invalid", "outcome_unknown")]
)
def test_uncertain_or_conflicting_slot_never_mutates(before, expected):
    service, _, store = setup()
    plan = service.plan()
    store.before = before
    assert service.apply(plan, confirmation_digest=plan.digest).status == expected
    assert [kind for kind, _ in store.calls] == ["inspect"]


@pytest.mark.parametrize(
    "after,expected",
    [
        ("exact", "retired_unpublished"),
        ("absent", "outcome_unknown"),
        ("unknown", "outcome_unknown"),
        ("conflict", "blocked"),
    ],
)
def test_lost_ack_is_resolved_only_by_exact_readback_never_write_retry(after, expected):
    service, _, store = setup()
    plan = service.plan()
    store.write, store.after = "unknown", after
    assert service.apply(plan, confirmation_digest=plan.digest).status == expected
    assert [kind for kind, _ in store.calls] == ["inspect", "retire", "inspect"]


def test_freeze_lost_during_insert_returns_unknown_without_retry():
    service, observer, store = setup()
    plan = service.plan()
    original = store.retire_if_absent

    def write(value):
        result = original(value)
        observer.valid = False
        return result

    store.retire_if_absent = write
    assert service.apply(plan, confirmation_digest=plan.digest).status == "outcome_unknown"
    assert [kind for kind, _ in store.calls] == ["inspect", "retire"]


def test_verify_is_read_only_after_prior_unknown_outcome():
    service, _, store = setup()
    plan = service.plan()
    store.before = "exact"
    assert service.verify(plan, confirmation_digest=plan.digest).status == "retired_unpublished"
    assert [kind for kind, _ in store.calls] == ["inspect"]


def test_verify_absent_is_not_permission_to_repeat_insert():
    service, _, store = setup()
    plan = service.plan()
    assert service.verify(plan, confirmation_digest=plan.digest).status == "outcome_unknown"
    assert [kind for kind, _ in store.calls] == ["inspect"]


def test_expiry_during_idempotent_readback_is_not_verified_success():
    service, _, store = setup()
    plan = service.plan()
    store.before = "exact"
    service._clock = lambda: 300 if store.calls else 220
    with pytest.raises(ValueError):
        service.apply(plan, confirmation_digest=plan.digest)
    assert [kind for kind, _ in store.calls] == ["inspect"]


def test_prior_unknown_attempt_cannot_repeat_insert_even_if_readback_is_absent(tmp_path):
    adapter = import_module("dpone.adapters.publication_retirement_attempts")
    service, _, store = setup(adapter.PrivateRetirementAttempts(tmp_path))
    plan = service.plan()
    store.write, store.after = "unknown", "absent"
    assert service.apply(plan, confirmation_digest=plan.digest).status == "outcome_unknown"
    restarted, _, new_store = setup(adapter.PrivateRetirementAttempts(tmp_path))
    result = restarted.apply(plan, confirmation_digest=plan.digest)
    assert result.status == "outcome_unknown"
    assert [kind for kind, _ in new_store.calls] == ["inspect"]


def test_unacknowledged_attempt_claim_prevents_sql_write():
    class FailingAttempts:
        def claim(self, digest):
            raise OSError("synthetic fsync failure")

    service, _, store = setup(FailingAttempts())
    plan = service.plan()
    assert service.apply(plan, confirmation_digest=plan.digest).status == "outcome_unknown"
    assert [kind for kind, _ in store.calls] == ["inspect"]


def test_replanning_same_legacy_operation_cannot_bypass_unknown_attempt(tmp_path):
    adapter = import_module("dpone.adapters.publication_retirement_attempts")
    service, _, store = setup(adapter.PrivateRetirementAttempts(tmp_path))
    plan = service.plan()
    store.write, store.after = "unknown", "absent"
    assert service.apply(plan, confirmation_digest=plan.digest).status == "outcome_unknown"

    restarted, observer, later_store = setup(adapter.PrivateRetirementAttempts(tmp_path))
    observer.value = replace(observer.value, freeze=replace(observer.value.freeze, receipt_digest="f" * 64))
    later_plan = restarted.plan()
    assert later_plan.digest != plan.digest
    assert restarted.apply(later_plan, confirmation_digest=later_plan.digest).status == "outcome_unknown"
    assert [kind for kind, _ in later_store.calls] == ["inspect"]


def test_expired_plan_can_be_verified_read_only_under_new_valid_freeze():
    service, observer, store = setup()
    original = service.plan()
    observer.value = replace(
        observer.value,
        freeze=replace(
            observer.value.freeze, receipt_digest="e" * 64, established_at=300, drained_at=310, expires_at=400
        ),
        replicas=tuple(
            replace(item, history_through=310, history_receipt_digest="f" * 64) for item in observer.value.replicas
        ),
    )
    service._clock = lambda: 320
    store.before = "exact"
    result = service.verify(original, confirmation_digest=original.digest)
    assert result.status == "retired_unpublished"
    assert [kind for kind, _ in store.calls] == ["inspect"]
    assert store.calls[0][1] == original.digest


def test_readback_new_freeze_cannot_substitute_original_legacy_bytes():
    service, observer, store = setup()
    original = service.plan()
    observer.value = replace(observer.value, source_location_digest="a" * 64)
    store.before = "exact"
    with pytest.raises(ValueError):
        service.verify(original, confirmation_digest=original.digest)
    assert not store.calls
