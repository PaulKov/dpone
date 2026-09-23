"""Attempt observers cannot acquire writer authority by reading durable state."""

import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from uuid import uuid4

import pytest

from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore
from dpone.adapters.mssql_tds_lifecycle import TdsAttemptJournal
from dpone.contracts.bounded_window import WindowContractError, WindowOutcomeUnknown
from dpone.contracts.mssql_tds_worker import (
    ContainmentRequired,
    Prepared,
    TdsAttemptError,
    TdsAttemptIdentity,
    TdsAttemptPhase,
    TdsObjectIdentity,
)


def identity(**changes):
    values = dict(
        target_key="target",
        run_id="run",
        ordinal=0,
        attempt=0,
        plan_sha256="1" * 64,
        policy_sha256="2" * 64,
        implementation_sha256="3" * 64,
        file_sha256="4" * 64,
        database="synthetic",
        schema="dbo",
        table="tds_owned",
        owner_binding="5" * 64,
    )
    return TdsAttemptIdentity(**dict(values, **changes))


def environment(tmp_path):
    store = SQLiteWindowStore(tmp_path / "journal.db", clock=lambda: 1.0)
    lease = store.acquire("target", "owner", 60)
    return store, lease, TdsAttemptJournal(store)


def test_create_is_not_idempotent_writer_acquisition(tmp_path):
    store, lease, journal = environment(tmp_path)
    token = str(uuid4())
    writer = journal.create(identity(), lease, supervisor_token=token)
    observed = journal.read(identity())
    assert observed == writer.snapshot
    with pytest.raises(WindowContractError):
        journal.create(identity(), lease, supervisor_token=token)
    assert journal.read(identity()) == observed
    writer.assert_authority()


def test_changed_policy_finds_original_record_instead_of_new_attempt(tmp_path):
    store, lease, journal = environment(tmp_path)
    journal.create(identity(), lease, supervisor_token=str(uuid4()))
    for field in ("policy_sha256", "implementation_sha256", "file_sha256"):
        with pytest.raises(WindowContractError, match="identity_changed"):
            journal.read(identity(**{field: "a" * 64}))


def test_takeover_requires_new_fence_and_keeps_evidence(tmp_path):
    store, lease, journal = environment(tmp_path)
    old = journal.create(identity(), lease, supervisor_token=str(uuid4()))
    snapshot = old.snapshot
    with pytest.raises(WindowContractError):
        journal.take_over(snapshot, lease, supervisor_token=str(uuid4()))
    store.release(lease)
    newer = store.acquire("target", "recovery", 60)
    current = journal.take_over(snapshot, newer, supervisor_token=str(uuid4()))
    assert current.snapshot.state.identity == snapshot.state.identity
    assert current.snapshot.state.phase == snapshot.state.phase
    assert current.snapshot.state.sequence == snapshot.state.sequence + 1
    with pytest.raises(WindowContractError):
        old.assert_authority()
    with pytest.raises(WindowContractError):
        journal.take_over(snapshot, newer, supervisor_token=str(uuid4()))


def test_fabricated_snapshot_cannot_overwrite_record_on_takeover(tmp_path):
    store, lease, journal = environment(tmp_path)
    writer = journal.create(identity(), lease, supervisor_token=str(uuid4()))
    fake = replace(writer.snapshot, state=replace(writer.snapshot.state, identity=identity(file_sha256="a" * 64)))
    store.release(lease)
    newer = store.acquire("target", "recovery", 60)
    with pytest.raises(WindowContractError):
        journal.take_over(fake, newer, supervisor_token=str(uuid4()))


def test_unknown_save_acknowledgement_permanently_poisons_writer(tmp_path):
    store, lease, _ = environment(tmp_path)

    class LostAcknowledgement:
        fail = False

        def __getattr__(self, name):
            return getattr(store, name)

        def save(self, *args):
            result = store.save(*args)
            if self.fail:
                raise ConnectionError("simulated lost acknowledgement")
            return result

    wrapper = LostAcknowledgement()
    journal = TdsAttemptJournal(wrapper)
    writer = journal.create(identity(), lease, supervisor_token=str(uuid4()))
    initial = writer.snapshot
    wrapper.fail = True
    with pytest.raises(WindowOutcomeUnknown, match="storage_outcome_unknown") as failure:
        writer.advance(ContainmentRequired(TdsAttemptError.CONNECTION), expected_phase=TdsAttemptPhase.CREATION_INTENT)
    assert isinstance(failure.value.__cause__, ConnectionError)
    assert writer.snapshot == initial
    assert journal.read(identity()).state.phase == TdsAttemptPhase.CONTAINMENT_REQUIRED
    wrapper.fail = False
    with pytest.raises(WindowContractError, match="writer_poisoned"):
        writer.assert_authority()
    with pytest.raises(WindowContractError, match="writer_poisoned"):
        writer.advance(ContainmentRequired(TdsAttemptError.CONNECTION), expected_phase=TdsAttemptPhase.CREATION_INTENT)


def test_recovery_cannot_continue_create_spawn_or_import(tmp_path):
    store, lease, journal = environment(tmp_path)
    writer = journal.create(identity(), lease, supervisor_token=str(uuid4()))
    store.release(lease)
    newer = store.acquire("target", "recovery", 60)
    recovery = journal.take_over(writer.snapshot, newer, supervisor_token=str(uuid4()))
    with pytest.raises(WindowContractError, match="recovery_requires_settlement"):
        recovery.advance(
            Prepared(TdsObjectIdentity(1, "a" * 64), "b" * 64), expected_phase=TdsAttemptPhase.CREATION_INTENT
        )
    recovery.advance(
        ContainmentRequired(TdsAttemptError.COORDINATOR_LOST), expected_phase=TdsAttemptPhase.CREATION_INTENT
    )


def test_writer_is_confined_to_creating_supervisor_thread(tmp_path):
    store, lease, journal = environment(tmp_path)
    writer = journal.create(identity(), lease, supervisor_token=str(uuid4()))
    with ThreadPoolExecutor(max_workers=1) as pool:
        with pytest.raises(WindowContractError, match="supervisor_thread_mismatch"):
            pool.submit(writer.assert_authority).result()
    writer.assert_authority()


@pytest.mark.parametrize("operation", ["create", "take_over"])
def test_factory_unknown_commit_does_not_allow_same_fence_reacquisition(tmp_path, operation):
    store, lease, journal = environment(tmp_path)
    observed = None
    if operation == "take_over":
        observed = journal.create(identity(), lease, supervisor_token=str(uuid4())).snapshot
        store.release(lease)
        lease = store.acquire("target", "recovery", 60)

    class LostReply:
        def __getattr__(self, name):
            return getattr(store, name)

        def save(self, *args):
            store.save(*args)
            raise ConnectionError("simulated lost acknowledgement")

    unknown = TdsAttemptJournal(LostReply())
    with pytest.raises(WindowOutcomeUnknown, match="storage_outcome_unknown"):
        if observed is None:
            unknown.create(identity(), lease, supervisor_token=str(uuid4()))
        else:
            unknown.take_over(observed, lease, supervisor_token=str(uuid4()))
    persisted = journal.read(identity())
    assert persisted.state.ownership.fence == lease.fence
    with pytest.raises(WindowContractError):
        journal.create(identity(), lease, supervisor_token=str(uuid4()))
    with pytest.raises(WindowContractError):
        journal.take_over(persisted, lease, supervisor_token=str(uuid4()))


@pytest.mark.skipif(not hasattr(os, "fork"), reason="POSIX fork required")
def test_forked_process_cannot_use_inherited_writer(tmp_path):
    # Use a fresh interpreter so the test itself never forks a pytest worker's
    # unrelated threads. The subprocess bound also detects inherited-lock hangs.
    script = """
import os,sys
from pathlib import Path
from uuid import uuid4
from tests.test_mssql_tds_lifecycle import environment,identity
from dpone.contracts.bounded_window import WindowContractError
from dpone.contracts.mssql_tds_worker import ContainmentRequired,TdsAttemptError,TdsAttemptPhase
store,lease,journal=environment(Path(sys.argv[1]))
writer=journal.create(identity(),lease,supervisor_token=str(uuid4()))
original=writer.snapshot
pid=os.fork()
if pid==0:
    for operation in (writer.assert_authority,lambda: writer.advance(
        ContainmentRequired(TdsAttemptError.CONNECTION),expected_phase=TdsAttemptPhase.CREATION_INTENT)):
        try:
            operation()
        except WindowContractError as error:
            if 'supervisor_process_mismatch' not in str(error):
                os._exit(2)
        else:
            os._exit(3)
    os._exit(0)
_,status=os.waitpid(pid,0)
assert os.waitstatus_to_exitcode(status)==0
writer.assert_authority()
assert journal.read(identity())==original
"""
    result = subprocess.run([sys.executable, "-c", script, str(tmp_path)], capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr
