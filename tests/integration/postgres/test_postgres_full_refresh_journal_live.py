from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from dpone.runtime.full_refresh_attempt import (
    AttemptState,
    AuthenticatedResourceReadback,
    EffectKind,
    EffectState,
    InvocationKey,
    ResourceKind,
    ResourceState,
)
from dpone.runtime.full_refresh_effect_authority import CreateTransition, EffectRecoveryReceipt
from dpone.runtime.state.postgres_full_refresh_journal import (
    ConcurrentJournalUpdateError,
    PostgresFullRefreshJournal,
)
from tests.integration.postgres.postgres_live_support import postgres_connector, postgres_enabled

pytestmark = [pytest.mark.integration, pytest.mark.integration_live, pytest.mark.integration_postgres]


def key() -> InvocationKey:
    suffix = uuid4().hex
    return InvocationKey(
        release_id=f"release-{suffix}",
        deployment_id="deployment",
        workflow_id="workflow",
        workload_id="workload",
        dag_id="dag",
        airflow_dag_run_id=f"manual__{suffix}",
        task_id="load",
        task_instance_try_number=1,
        map_index=-1,
    )


@pytest.fixture(autouse=True)
def require_postgres() -> None:
    if not postgres_enabled() and os.environ.get("DPONE_RUN_FULL_REFRESH_JOURNAL_DOCKER") != "1":
        pytest.skip("requires disposable PostgreSQL")


def plan(journal: PostgresFullRefreshJournal, attempt, *, name: str | None = None):
    return journal.plan_create(
        attempt,
        expected_owner_id="worker-a",
        resource_kind=ResourceKind.CLICKHOUSE_SLICE_TABLE,
        authority="clickhouse-test",
        planned_name=name or f"slice_{uuid4().hex}",
        schema_hash="a" * 64,
    )


def grant(journal: PostgresFullRefreshJournal, planned, *, grant_until: datetime):
    return journal.compare_and_advance_create(
        planned,
        expected_owner_id="worker-a",
        transition=CreateTransition(
            ResourceState.CREATE_PLANNED,
            ResourceState.CREATE_GRANTED,
            EffectState.PLANNED,
            EffectState.GRANTED,
            credential_version_ref="credential-v1",
            grant_until=grant_until,
        ),
    )


def dispatch(journal: PostgresFullRefreshJournal, granted):
    return journal.compare_and_advance_create(
        granted,
        expected_owner_id="worker-a",
        transition=CreateTransition(
            ResourceState.CREATE_GRANTED,
            ResourceState.CREATE_DISPATCHED,
            EffectState.GRANTED,
            EffectState.DISPATCHED,
            external_query_id=f"query-{uuid4().hex}",
        ),
    )


def mark_not_applied(journal: PostgresFullRefreshJournal, dispatched):
    return journal.compare_and_advance_create(
        dispatched,
        expected_owner_id="worker-a",
        transition=CreateTransition(
            ResourceState.CREATE_DISPATCHED,
            ResourceState.CREATE_NOT_APPLIED,
            EffectState.DISPATCHED,
            EffectState.NOT_APPLIED,
        ),
    )


def expire_lease(journal: PostgresFullRefreshJournal, attempt) -> None:
    journal._connector.execute_query(  # noqa: SLF001 - exercise PostgreSQL's authoritative clock
        "UPDATE dpone_full_refresh_v1.attempt SET lease_until = clock_timestamp() - interval '1 second' "
        "WHERE attempt_id = %s",
        (attempt.identity.attempt_id,),
    )


def test_full_create_grant_dispatch_readback_and_bind() -> None:
    journal = PostgresFullRefreshJournal(postgres_connector())
    attempt = journal.create_or_get(key(), owner_id="worker-a", lease_seconds=30)
    planned = plan(journal, attempt)
    granted = grant(journal, planned, grant_until=datetime.now(UTC) + timedelta(minutes=1))
    dispatched = dispatch(journal, granted)
    readback = AuthenticatedResourceReadback(
        effect_id=planned.effect_id,
        credential_version_ref="credential-v1",
        authority=planned.resource.authority,
        planned_name=planned.resource.planned_name,
        schema_hash=planned.resource.schema_hash,
        exact_identity={"uuid": str(uuid4())},
        server_receipt_ref="receipt-v1",
    )
    observed = journal.compare_and_advance_create(
        dispatched,
        expected_owner_id="worker-a",
        transition=CreateTransition(
            ResourceState.CREATE_DISPATCHED,
            ResourceState.CREATE_OBSERVED,
            EffectState.DISPATCHED,
            EffectState.APPLIED,
            readback=readback,
        ),
    )

    bound = journal.bind_resource(observed, readback=readback, expected_owner_id="worker-a")

    assert bound.resource_version == 4
    assert bound.effect_version == 3


def test_two_instances_reject_different_owner_stale_cas_and_roll_back_conflict() -> None:
    first = PostgresFullRefreshJournal(postgres_connector())
    second = PostgresFullRefreshJournal(postgres_connector())
    invocation = key()
    created = first.create_or_get(invocation, owner_id="worker-a", lease_seconds=30)
    observed = second.create_or_get(invocation, owner_id="worker-a", lease_seconds=30)

    with pytest.raises(ConcurrentJournalUpdateError, match="explicit epoch acquisition"):
        second.create_or_get(invocation, owner_id="worker-b", lease_seconds=30)
    advanced = first.compare_and_advance(
        created,
        expected_owner_id="worker-a",
        next_state=AttemptState.FROZEN_SOURCE,
        facts={"generation": "g1"},
    )
    with pytest.raises(ConcurrentJournalUpdateError):
        second.compare_and_advance(
            observed,
            expected_owner_id="worker-a",
            next_state=AttemptState.FROZEN_SOURCE,
            facts={"generation": "g1"},
        )
    assert first.read(created.identity).version == advanced.version

    planned = plan(first, advanced)
    before_conflict = first.read(created.identity)
    with pytest.raises(Exception):
        plan(first, planned.attempt, name=planned.resource.planned_name)
    assert first.read(created.identity).version == before_conflict.version


def test_takeover_waits_for_attempt_lock_then_fences_stale_owner() -> None:
    first_connector = postgres_connector()
    first = PostgresFullRefreshJournal(first_connector)
    second = PostgresFullRefreshJournal(postgres_connector())
    created = first.create_or_get(key(), owner_id="worker-a", lease_seconds=30)
    expire_lease(first, created)

    with ThreadPoolExecutor(max_workers=1) as executor:
        with first_connector.connection.transaction(), first_connector.connection.cursor() as cursor:
            cursor.execute(
                "SELECT 1 FROM dpone_full_refresh_v1.attempt WHERE attempt_id = %s FOR UPDATE",
                (created.identity.attempt_id,),
            )
            future = executor.submit(
                second.acquire_next_epoch,
                created,
                expected_owner_id="worker-a",
                new_owner_id="worker-b",
                lease_seconds=30,
            )
            with pytest.raises(TimeoutError):
                future.result(timeout=0.2)
        acquired = future.result(timeout=5)

    assert acquired.owner_id == "worker-b"
    assert acquired.lease_epoch == created.lease_epoch + 1
    with pytest.raises(ConcurrentJournalUpdateError):
        plan(first, created)


def test_takeover_and_planning_reject_terminal_or_live_granted_effect() -> None:
    first = PostgresFullRefreshJournal(postgres_connector())
    second = PostgresFullRefreshJournal(postgres_connector())
    terminal = first.create_or_get(key(), owner_id="worker-a", lease_seconds=30)
    terminal = first.compare_and_advance(
        terminal,
        expected_owner_id="worker-a",
        next_state=AttemptState.FAILED,
        facts={},
    )
    with pytest.raises((ValueError, ConcurrentJournalUpdateError), match="terminal"):
        plan(first, terminal)
    with pytest.raises(ConcurrentJournalUpdateError, match="terminal"):
        second.acquire_next_epoch(
            terminal,
            expected_owner_id="worker-a",
            new_owner_id="worker-b",
            lease_seconds=30,
        )


@pytest.mark.parametrize("dispatched", [False, True])
def test_collector_reconciles_expired_effect_then_takeover_fences_old_worker(dispatched: bool) -> None:
    first = PostgresFullRefreshJournal(postgres_connector())
    second = PostgresFullRefreshJournal(postgres_connector())
    created = first.create_or_get(key(), owner_id="worker-a", lease_seconds=30)
    current = grant(first, plan(first, created), grant_until=datetime.now(UTC) + timedelta(minutes=1))
    if dispatched:
        current = dispatch(first, current)
    expire_lease(first, current.attempt)
    with pytest.raises(ConcurrentJournalUpdateError, match="takeover"):
        second.acquire_next_epoch(
            current.attempt,
            expected_owner_id="worker-a",
            new_owner_id="worker-b",
            lease_seconds=30,
        )
    if dispatched:
        with pytest.raises(ValueError, match="final exact CREATE outcome"):
            EffectRecoveryReceipt(
                attempt_id=current.attempt.identity.attempt_id,
                lease_epoch=current.attempt.lease_epoch,
                effect_id=current.effect_id,
                resource_id=current.resource.resource_id,
                effect_kind=EffectKind.CREATE,
                expected_resource_state=ResourceState.CREATE_DISPATCHED,
                next_resource_state=ResourceState.CREATE_DISPATCHED,
                expected_effect_state=EffectState.DISPATCHED,
                next_effect_state=EffectState.TERMINATED,
                authority_receipt_ref=f"termination-only-{uuid4().hex}",
            )
        with pytest.raises(ConcurrentJournalUpdateError, match="takeover"):
            second.acquire_next_epoch(
                current.attempt,
                expected_owner_id="worker-a",
                new_owner_id="worker-b",
                lease_seconds=30,
            )
    receipt = EffectRecoveryReceipt(
        attempt_id=current.attempt.identity.attempt_id,
        lease_epoch=current.attempt.lease_epoch,
        effect_id=current.effect_id,
        resource_id=current.resource.resource_id,
        effect_kind=EffectKind.CREATE,
        expected_resource_state=(ResourceState.CREATE_DISPATCHED if dispatched else ResourceState.CREATE_GRANTED),
        next_resource_state=ResourceState.CREATE_NOT_APPLIED,
        expected_effect_state=EffectState.DISPATCHED if dispatched else EffectState.GRANTED,
        next_effect_state=EffectState.NOT_APPLIED if dispatched else EffectState.REVOKED,
        authority_receipt_ref=f"collector-receipt-{uuid4().hex}",
    )
    with pytest.raises(ValueError, match="exact attempt/effect/resource/epoch"):
        first.reconcile_expired_create(
            current,
            receipt=replace(receipt, resource_id=uuid4()),
            expected_owner_id="worker-a",
        )
    recovered = first.reconcile_expired_create(current, receipt=receipt, expected_owner_id="worker-a")
    acquired = second.acquire_next_epoch(
        recovered.attempt,
        expected_owner_id="worker-a",
        new_owner_id="worker-b",
        lease_seconds=30,
    )

    assert acquired.lease_epoch == current.attempt.lease_epoch + 1
    assert acquired.owner_id == "worker-b"
    retried = second.retry_not_applied(
        replace(recovered, attempt=acquired),
        effect_kind=EffectKind.CREATE,
        expected_owner_id="worker-b",
    )
    assert retried.fence_epoch == acquired.lease_epoch
    assert retried.effect_id != recovered.effect_id
    with pytest.raises(ConcurrentJournalUpdateError):
        first.compare_and_advance(
            recovered.attempt,
            expected_owner_id="worker-a",
            next_state=AttemptState.FROZEN_SOURCE,
            facts={},
        )


def test_grant_rejects_expired_database_time_and_naive_timestamp() -> None:
    journal = PostgresFullRefreshJournal(postgres_connector())
    planned = plan(journal, journal.create_or_get(key(), owner_id="worker-a", lease_seconds=30))

    with pytest.raises(ValueError, match="PostgreSQL"):
        grant(journal, planned, grant_until=datetime.now(UTC) - timedelta(seconds=1))
    with pytest.raises(ValueError, match="timezone-aware"):
        grant(journal, planned, grant_until=datetime.now())

    granted = grant(journal, planned, grant_until=datetime.now(UTC) + timedelta(minutes=1))
    journal._connector.execute_query(  # noqa: SLF001 - force persisted grant expiry by PostgreSQL clock
        "UPDATE dpone_full_refresh_v1.effect SET grant_until = clock_timestamp() - interval '1 second' "
        "WHERE effect_id = %s",
        (granted.effect_id,),
    )
    with pytest.raises(ConcurrentJournalUpdateError, match="paired CREATE"):
        dispatch(journal, granted)


def test_not_applied_create_retries_with_a_fresh_fenced_effect() -> None:
    journal = PostgresFullRefreshJournal(postgres_connector())
    planned = plan(journal, journal.create_or_get(key(), owner_id="worker-a", lease_seconds=30))
    granted = grant(journal, planned, grant_until=datetime.now(UTC) + timedelta(minutes=1))
    not_applied = mark_not_applied(journal, dispatch(journal, granted))

    retried = journal.retry_not_applied(
        not_applied,
        effect_kind=EffectKind.CREATE,
        expected_owner_id="worker-a",
    )

    assert retried.effect_id != not_applied.effect_id
    assert retried.effect_version == 0
    assert retried.resource_version == not_applied.resource_version + 1


def test_not_applied_retry_rejects_mixed_resource_effect_and_wrong_kind() -> None:
    journal = PostgresFullRefreshJournal(postgres_connector())
    created = journal.create_or_get(key(), owner_id="worker-a", lease_seconds=30)
    first = plan(journal, created)
    second = plan(journal, first.attempt)
    first = replace(first, attempt=second.attempt)
    first = mark_not_applied(
        journal,
        dispatch(journal, grant(journal, first, grant_until=datetime.now(UTC) + timedelta(minutes=1))),
    )
    second = mark_not_applied(
        journal,
        dispatch(journal, grant(journal, second, grant_until=datetime.now(UTC) + timedelta(minutes=1))),
    )

    mixed = replace(first, effect_id=second.effect_id, effect_version=second.effect_version)
    with pytest.raises(ConcurrentJournalUpdateError, match="retry CAS"):
        journal.retry_not_applied(mixed, effect_kind=EffectKind.CREATE, expected_owner_id="worker-a")
    with pytest.raises(ConcurrentJournalUpdateError, match="retry CAS"):
        journal.retry_not_applied(first, effect_kind=EffectKind.RENAME, expected_owner_id="worker-a")


def test_normal_create_advance_and_bind_reject_mixed_resource_effect() -> None:
    journal = PostgresFullRefreshJournal(postgres_connector())
    created = journal.create_or_get(key(), owner_id="worker-a", lease_seconds=30)
    first = plan(journal, created)
    second = plan(journal, first.attempt)
    first = replace(first, attempt=second.attempt)

    with pytest.raises(ConcurrentJournalUpdateError, match="paired CREATE"):
        grant(
            journal,
            replace(first, effect_id=second.effect_id),
            grant_until=datetime.now(UTC) + timedelta(minutes=1),
        )

    first = dispatch(journal, grant(journal, first, grant_until=datetime.now(UTC) + timedelta(minutes=1)))
    second = dispatch(journal, grant(journal, second, grant_until=datetime.now(UTC) + timedelta(minutes=1)))
    first_readback = AuthenticatedResourceReadback(
        effect_id=first.effect_id,
        credential_version_ref="credential-v1",
        authority=first.resource.authority,
        planned_name=first.resource.planned_name,
        schema_hash=first.resource.schema_hash,
        exact_identity={"uuid": str(uuid4())},
        server_receipt_ref="receipt-first",
    )
    second_readback = replace(
        first_readback,
        effect_id=second.effect_id,
        planned_name=second.resource.planned_name,
        exact_identity={"uuid": str(uuid4())},
        server_receipt_ref="receipt-second",
    )
    first = journal.compare_and_advance_create(
        first,
        expected_owner_id="worker-a",
        transition=CreateTransition(
            ResourceState.CREATE_DISPATCHED,
            ResourceState.CREATE_OBSERVED,
            EffectState.DISPATCHED,
            EffectState.APPLIED,
            readback=first_readback,
        ),
    )
    second = journal.compare_and_advance_create(
        second,
        expected_owner_id="worker-a",
        transition=CreateTransition(
            ResourceState.CREATE_DISPATCHED,
            ResourceState.CREATE_OBSERVED,
            EffectState.DISPATCHED,
            EffectState.APPLIED,
            readback=second_readback,
        ),
    )
    mixed = replace(first, effect_id=second.effect_id, effect_version=second.effect_version)
    mixed_readback = replace(first_readback, effect_id=second.effect_id)

    with pytest.raises(ConcurrentJournalUpdateError, match="identity bind CAS"):
        journal.bind_resource(mixed, readback=mixed_readback, expected_owner_id="worker-a")
