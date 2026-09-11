"""Offline publisher faults and concurrency; these doubles do not certify SQL."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from threading import Barrier

import pytest

from dpone.contracts.composition_activation import CompositionAdmissionError
from tests.composition_snapshot_helpers import NEW, OLD, digest, intent, observation, occurrence, rig


def prepare(publisher, value):
    return publisher.prepare(value.attempt, value.generation.record_sha256)


@pytest.mark.parametrize("rows", [0, 2])
def test_complete_snapshot_publishes_once_after_durable_claim(rows):
    value = intent(rows=rows)
    publisher, store, authority, _, executor = rig(value)
    assert prepare(publisher, value).state == "PREPARED"
    result = publisher.publish(value.intent_sha256)
    assert result.state == "PUBLISHED" and result.observation.generation_rows == rows
    assert result.closure and authority.closed
    assert executor.calls == [value.exchange_query_id]
    assert store.events == ["prepare", "read", "read", "claim", "read", "read", "resolve", "read"]
    assert publisher.publish(value.intent_sha256) == result
    assert executor.calls == [value.exchange_query_id]


@pytest.mark.parametrize("published", [False, True])
def test_terminal_publish_replays_history_but_reconcile_requires_retained_owner(published):
    value = intent()
    publisher, store, authority, _, executor = rig(value)
    prepare(publisher, value)
    executor.no_effect = not published
    result = publisher.publish(value.intent_sha256)
    assert result.state == ("PUBLISHED" if published else "NOT_PUBLISHED")
    authority.parent = occurrence("RETIRED")
    calls = list(authority.calls)
    assert publisher.publish(value.intent_sha256) == result
    assert authority.calls == calls
    with pytest.raises(CompositionAdmissionError, match="snapshot_occurrence_state"):
        publisher.reconcile(value.intent_sha256)
    assert store.records[value.intent_sha256] == result
    assert executor.calls == [value.exchange_query_id]


def test_lost_claim_ack_cannot_dispatch_even_when_claim_is_durable():
    value = intent()
    publisher, store, _, _, executor = rig(value)
    prepare(publisher, value)
    store.claim_error = True
    with pytest.raises(CompositionAdmissionError) as caught:
        publisher.publish(value.intent_sha256)
    assert "secret" not in str(caught.value)
    assert not executor.calls
    assert store.records[value.intent_sha256].state == "EXCHANGE_INTENT"
    assert publisher.reconcile(value.intent_sha256).state == "NOT_PUBLISHED"


def test_lost_exchange_ack_resolves_published_pair_without_second_exchange():
    value = intent()
    publisher, _, _, _, executor = rig(value)
    prepare(publisher, value)
    executor.error = True
    assert publisher.publish(value.intent_sha256).state == "PUBLISHED"
    assert publisher.reconcile(value.intent_sha256).state == "PUBLISHED"
    assert len(executor.calls) == 1


def test_exact_prepare_is_idempotent_but_changed_attempt_slot_document_rejects():
    value = intent()
    publisher, store, authority, _, executor = rig(value)
    first = prepare(publisher, value)
    assert prepare(publisher, value) == first
    authority.value = replace(value, closed_ingest_sha256=digest("other closed ingest"))
    with pytest.raises(CompositionAdmissionError):
        prepare(publisher, authority.value)
    assert len(store.records) == 1 and not executor.calls


@pytest.mark.parametrize("fault", ["missing", "advanced_revision", "exception"])
def test_independent_claim_readback_failure_never_dispatches(fault, monkeypatch):
    value = intent()
    publisher, store, _, _, executor = rig(value)
    prepare(publisher, value)
    original = store.read

    def read(key):
        row = original(key)
        if row.state == "EXCHANGE_INTENT":
            if fault == "exception":
                raise RuntimeError("secret fresh readback unavailable")
            if fault == "missing":
                return None
            return row.transition("COMMIT_UNKNOWN")
        return row

    monkeypatch.setattr(store, "read", read)
    with pytest.raises(CompositionAdmissionError) as caught:
        publisher.publish(value.intent_sha256)
    assert "secret" not in str(caught.value)
    assert not executor.calls
    assert store.records[value.intent_sha256].state == "EXCHANGE_INTENT"


def test_failed_claim_does_not_fabricate_durable_claim_or_dispatch(monkeypatch):
    value = intent()
    publisher, store, authority, _, executor = rig(value)
    prepare(publisher, value)

    def unavailable(expected):
        raise RuntimeError("secret failed transaction")

    monkeypatch.setattr(store, "claim_exchange", unavailable)
    with pytest.raises(CompositionAdmissionError):
        publisher.publish(value.intent_sha256)
    assert store.records[value.intent_sha256].state == "PREPARED"
    assert authority.parent.receipt.state == "ACTIVE" and not executor.calls
    assert publisher.reconcile(value.intent_sha256).state == "NOT_PUBLISHED"
    assert authority.closed


def test_claim_loser_cannot_dispatch_or_close_winning_publisher(monkeypatch):
    value = intent()
    publisher, store, authority, _, executor = rig(value)
    prepare(publisher, value)
    original, barrier = store.claim_exchange, Barrier(2)

    def claim(expected):
        barrier.wait(timeout=5)
        return original(expected)

    def publish():
        try:
            return publisher.publish(value.intent_sha256).state
        except CompositionAdmissionError as error:
            return error.reason

    monkeypatch.setattr(store, "claim_exchange", claim)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: publish(), range(2)))
    assert sorted(results) == ["PUBLISHED", "snapshot_dispatch_conflict"]
    assert len(executor.calls) == 1 and authority.closed


def test_claimed_replay_does_not_close_or_dispatch_before_explicit_recovery():
    value = intent()
    publisher, store, authority, _, executor = rig(value)
    row = prepare(publisher, value)
    store.claim_exchange(row)
    with pytest.raises(CompositionAdmissionError, match="already_claimed"):
        publisher.publish(value.intent_sha256)
    assert not authority.closed and not executor.calls
    assert publisher.reconcile(value.intent_sha256).state == "NOT_PUBLISHED"


@pytest.mark.parametrize("fault", ["authority", "catalog"])
def test_missing_prepared_dependencies_cannot_persist_or_dispatch(fault):
    value = intent()
    publisher, store, authority, catalog, executor = rig(value)
    if fault == "authority":
        authority.current_error = True
    else:
        catalog.error = True
    with pytest.raises(CompositionAdmissionError) as caught:
        prepare(publisher, value)
    assert "secret" not in str(caught.value)
    assert not store.records and not executor.calls


@pytest.mark.parametrize("change", ["epoch", "parent", "cell", "write", "service"])
def test_parent_scope_change_blocks_before_claim(change):
    value = intent()
    publisher, store, authority, _, executor = rig(value)
    prepare(publisher, value)
    parent = authority.parent
    if change == "epoch":
        authority.parent = replace(
            parent,
            receipt=replace(
                parent.receipt, guard_epochs=tuple((guard, epoch + 1) for guard, epoch in parent.receipt.guard_epochs)
            ),
        )
    elif change == "parent":
        request = replace(parent.request, source_subject_sha256=digest("changed source"))
        authority.parent = replace(
            parent, request=request, receipt=replace(parent.receipt, request_sha256=request.request_sha256)
        )
    else:
        replacement = {"cell": "sqlserver_dbt_v1", "write": digest("foreign"), "service": NEW}[change]
        if change == "cell":
            request = replace(
                parent.request,
                workloads=(
                    replace(parent.request.workloads[0], execution_cell=replacement),
                    parent.request.workloads[1],
                ),
            )
            authority.parent = replace(
                parent, request=request, receipt=replace(parent.receipt, request_sha256=request.request_sha256)
            )
        elif change == "write":
            bad = replace(value, target=replace(value.target, write_subject_sha256=replacement))
            store.records = {bad.intent_sha256: replace(store.records[value.intent_sha256], intent=bad)}
            value = bad
        else:
            authority.current_error = True  # protected service verifier fails before returning an occurrence
    with pytest.raises(CompositionAdmissionError):
        publisher.publish(value.intent_sha256)
    assert "claim" not in store.events and not executor.calls


def test_changed_epoch_after_claim_cannot_dispatch_or_resolve_old_owner(monkeypatch):
    value = intent()
    publisher, store, authority, _, executor = rig(value)
    prepare(publisher, value)
    original = store.claim_exchange

    def claim(expected):
        row = original(expected)
        parent = authority.parent
        authority.parent = replace(
            parent,
            receipt=replace(
                parent.receipt, guard_epochs=tuple((guard, epoch + 1) for guard, epoch in parent.receipt.guard_epochs)
            ),
        )
        return row

    monkeypatch.setattr(store, "claim_exchange", claim)
    with pytest.raises(CompositionAdmissionError):
        publisher.publish(value.intent_sha256)
    assert not executor.calls and store.records[value.intent_sha256].state == "EXCHANGE_INTENT"


def test_retiring_allows_existing_recovery_but_never_new_publication():
    value = intent()
    publisher, store, authority, _, executor = rig(value)
    prepare(publisher, value)
    authority.parent = occurrence("RETIRING")
    with pytest.raises(CompositionAdmissionError, match="occurrence_state"):
        publisher.publish(value.intent_sha256)
    assert publisher.reconcile(value.intent_sha256).state == "NOT_PUBLISHED"
    assert authority.closed and not executor.calls
    assert store.records[value.intent_sha256].state == "NOT_PUBLISHED"


@pytest.mark.parametrize("state", ["PREPARED", "RETIRED"])
def test_recovery_requires_exact_retained_active_or_retiring_owner(state):
    value = intent()
    publisher, store, authority, _, executor = rig(value)
    row = prepare(publisher, value)
    store.claim_exchange(row)
    authority.parent = occurrence(state)
    with pytest.raises(CompositionAdmissionError):
        publisher.reconcile(value.intent_sha256)
    assert not authority.closed and not executor.calls


def test_unclosed_publisher_cannot_resolve_observed_success_then_recovery_reopens_proof():
    value = intent()
    publisher, store, authority, _, executor = rig(value)
    prepare(publisher, value)
    authority.close_error = True
    result = publisher.publish(value.intent_sha256)
    assert result.state == "COMMIT_UNKNOWN" and result.closure is None
    assert len(executor.calls) == 1
    with pytest.raises(CompositionAdmissionError, match="already_claimed"):
        publisher.publish(value.intent_sha256)
    authority.close_error = False
    assert publisher.reconcile(value.intent_sha256).state == "PUBLISHED"
    assert len(executor.calls) == 1 and store.records[value.intent_sha256].state == "PUBLISHED"


def test_wrong_closure_subject_stays_unknown_despite_known_published_pair(monkeypatch):
    value = intent()
    publisher, _, authority, _, executor = rig(value)
    prepare(publisher, value)
    original = authority.close_publisher
    monkeypatch.setattr(
        authority, "close_publisher", lambda value: replace(original(value), intent_sha256=digest("other"))
    )
    row = publisher.publish(value.intent_sha256)
    assert row.state == "COMMIT_UNKNOWN" and row.closure is None and len(executor.calls) == 1


def test_fresh_catalog_unavailable_after_exchange_cannot_assume_success(monkeypatch):
    value = intent()
    publisher, _, _, catalog, executor = rig(value)
    prepare(publisher, value)
    original = executor.exchange_once

    def exchange(value):
        original(value)
        catalog.error = True

    monkeypatch.setattr(executor, "exchange_once", exchange)
    row = publisher.publish(value.intent_sha256)
    assert row.state == "COMMIT_UNKNOWN" and row.observation is None
    catalog.error = False
    assert publisher.reconcile(value.intent_sha256).state == "PUBLISHED"
    assert len(executor.calls) == 1


@pytest.mark.parametrize(
    "changes",
    [
        {"target_uuid": None},
        {"generation_uuid": None},
        {"target_uuid": OLD, "generation_uuid": OLD},
        {"generation_content_sha256": digest("tampered")},
        {"generation_rows": 99},
        {"unsupported_features": ("late_materialized_view",)},
        {"generation_bytes": None},
    ],
)
def test_post_dispatch_ambiguous_or_changed_observation_retains_unknown(changes, monkeypatch):
    value = intent()
    publisher, store, _, catalog, executor = rig(value)
    prepare(publisher, value)
    original = executor.exchange_once

    def exchange(value):
        original(value)
        catalog.value = replace(catalog.value, **changes)

    monkeypatch.setattr(executor, "exchange_once", exchange)
    row = publisher.publish(value.intent_sha256)
    assert row.state == "COMMIT_UNKNOWN" and row.closure
    assert store.records[value.intent_sha256].state == "COMMIT_UNKNOWN" and len(executor.calls) == 1


def test_no_effect_exchange_resolves_unpublished_without_blind_retry():
    value = intent()
    publisher, _, _, _, executor = rig(value)
    prepare(publisher, value)
    executor.no_effect = executor.error = True
    assert publisher.publish(value.intent_sha256).state == "NOT_PUBLISHED"
    assert publisher.publish(value.intent_sha256).state == "NOT_PUBLISHED"
    assert len(executor.calls) == 1


def test_outcome_commit_ack_loss_keeps_exact_recoverable_record_without_reexchange():
    value = intent()
    publisher, store, _, _, executor = rig(value)
    prepare(publisher, value)
    store.resolution_error = True
    with pytest.raises(CompositionAdmissionError) as caught:
        publisher.publish(value.intent_sha256)
    assert "secret" not in str(caught.value)
    assert store.records[value.intent_sha256].state == "PUBLISHED"
    assert publisher.reconcile(value.intent_sha256).state == "PUBLISHED" and len(executor.calls) == 1


def test_prepared_cancellation_without_proven_closure_does_not_fabricate_outcome():
    value = intent()
    publisher, store, authority, _, executor = rig(value)
    prepare(publisher, value)
    authority.close_error = True
    with pytest.raises(CompositionAdmissionError):
        publisher.reconcile(value.intent_sha256)
    assert store.records[value.intent_sha256].state == "PREPARED" and not executor.calls


def test_recovery_reads_persisted_generation_not_new_source_or_caller_plan(monkeypatch):
    value = intent()
    publisher, store, authority, catalog, executor = rig(value)
    row = prepare(publisher, value)
    store.claim_exchange(row)
    catalog.value = observation(value, published=True)

    def no_producer_reexecution(*args):
        raise AssertionError("recovery must not reacquire current source snapshot")

    monkeypatch.setattr(authority, "load_prepared", no_producer_reexecution)
    assert publisher.reconcile(value.intent_sha256).state == "PUBLISHED" and not executor.calls


@pytest.mark.parametrize("phase", ["prepare", "resolve"])
def test_acknowledged_commit_without_exact_fresh_readback_is_not_returned(phase, monkeypatch):
    value = intent()
    publisher, store, _, _, executor = rig(value)
    original = getattr(store, phase)

    def commit_then_read_unavailable(*args, **kwargs):
        result = original(*args, **kwargs)
        store.read_error = True
        return result

    monkeypatch.setattr(store, phase, commit_then_read_unavailable)
    if phase == "resolve":
        prepare(publisher, value)
    with pytest.raises(CompositionAdmissionError) as caught:
        if phase == "prepare":
            prepare(publisher, value)
        else:
            publisher.publish(value.intent_sha256)
    assert "secret" not in str(caught.value)
    assert len(executor.calls) == int(phase == "resolve")
    assert store.records[value.intent_sha256].state == ("PREPARED" if phase == "prepare" else "PUBLISHED")


def test_changed_B_after_claim_is_never_dispatched_or_resolved_as_success(monkeypatch):
    value = intent()
    publisher, store, _, catalog, executor = rig(value)
    prepare(publisher, value)
    original = store.claim_exchange

    def claim(expected):
        result = original(expected)
        catalog.value = replace(catalog.value, generation_rows=3)
        return result

    monkeypatch.setattr(store, "claim_exchange", claim)
    assert publisher.publish(value.intent_sha256).state == "COMMIT_UNKNOWN"
    assert not executor.calls


def test_missing_intent_cannot_create_recovery_or_dispatch_authority():
    publisher, store, authority, _, executor = rig()
    with pytest.raises(CompositionAdmissionError):
        publisher.reconcile(digest("absent"))
    with pytest.raises(CompositionAdmissionError):
        publisher.publish(digest("absent"))
    assert not authority.closed and not authority.calls and not store.records and not executor.calls


def test_exact_attempt_bytes_must_match_protected_generation_even_for_legacy_hash_alias(monkeypatch):
    value = intent()
    protected = replace(value, attempt=replace(value.attempt, dag_run_id="run\\attempt"))
    caller = replace(value.attempt, dag_run_id="run/attempt")
    assert protected.attempt.attempt_sha256 == caller.attempt_sha256
    publisher, store, authority, _, executor = rig(protected)
    monkeypatch.setattr(authority, "load_prepared", lambda *args: protected)
    with pytest.raises(CompositionAdmissionError, match="prepared_subject"):
        publisher.prepare(caller, protected.generation.record_sha256)
    assert not store.records and not executor.calls
