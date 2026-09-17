"""Once-only normal return, original authentication and close/run exclusion."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from threading import Event

import pytest

from dpone.contracts.native_generation_invocation import decode_trusted_dbt_invocation_completion
from dpone.contracts.native_source_custody import NativeSourceCustodyError
from dpone.ports.dbt_publishing import DbtCommandResult
from tests.native_trusted_dbt_fixtures import InvocationFixture


def test_complete_sequence_publishes_once_without_outputs_or_redactions(tmp_path):
    fixture = InvocationFixture(tmp_path)
    reference = fixture.complete()
    payload = fixture.store.documents[reference.locator]
    assert b"unit-secret" not in payload
    completion = decode_trusted_dbt_invocation_completion(payload)
    assert completion.command_count == 3
    assert completion.executor == fixture.executor
    fixture.recorder.close(deadline_monotonic=fixture.now)
    assert fixture.recorder.require_completion() == reference
    assert fixture.store.publications == 1
    assert len(fixture.delegate.calls) == 3


def test_quality_is_one_test_command(tmp_path):
    fixture = InvocationFixture(tmp_path, "QUALITY")
    reference = fixture.complete()
    assert decode_trusted_dbt_invocation_completion(fixture.store.documents[reference.locator]).command_count == 1
    assert len(fixture.delegate.calls) == 1


def test_close_before_admission_prevents_delegate_call(tmp_path):
    fixture = InvocationFixture(tmp_path)
    fixture.recorder.close(deadline_monotonic=fixture.now)
    with pytest.raises(NativeSourceCustodyError):
        fixture.run(0)
    assert not fixture.delegate.calls


def test_close_timeout_latches_unknown_despite_late_zero_return(tmp_path):
    fixture = InvocationFixture(tmp_path)
    entered, release = Event(), Event()

    def blocked():
        entered.set()
        assert release.wait(5)
        return DbtCommandResult(0)

    fixture.delegate.action = blocked
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(fixture.run, 0)
        assert entered.wait(5)
        try:
            with pytest.raises(NativeSourceCustodyError, match="UNKNOWN"):
                fixture.recorder.close(deadline_monotonic=fixture.now)
        finally:
            release.set()
        with pytest.raises(NativeSourceCustodyError):
            future.result(timeout=5)
    with pytest.raises(NativeSourceCustodyError):
        fixture.recorder.require_completion()
    assert fixture.store.publications == 0
    assert len(fixture.delegate.calls) == 1


def test_close_between_commands_excludes_next_slot(tmp_path):
    fixture = InvocationFixture(tmp_path)
    fixture.run(0)
    fixture.recorder.close(deadline_monotonic=fixture.now)
    with pytest.raises(NativeSourceCustodyError):
        fixture.run(1)
    assert len(fixture.delegate.calls) == 1


@pytest.mark.parametrize("failure", ["exception", "nonzero", "entry-timeout", "clock-reversed"])
def test_failure_consumes_slot_and_cannot_be_replayed(tmp_path, failure):
    fixture = InvocationFixture(tmp_path)

    def action():
        if failure == "exception":
            raise OSError("synthetic execution error")
        if failure == "entry-timeout":
            fixture.now += 13
        if failure == "clock-reversed":
            fixture.now -= 1
        return DbtCommandResult(1 if failure == "nonzero" else 0)

    fixture.delegate.action = action
    with pytest.raises((NativeSourceCustodyError, OSError)):
        fixture.run(0)
    with pytest.raises(NativeSourceCustodyError):
        fixture.run(0)
    assert len(fixture.delegate.calls) == 1
    assert fixture.store.publications == 0


def test_absolute_allowance_includes_time_between_commands(tmp_path):
    fixture = InvocationFixture(tmp_path)
    fixture.run(0)
    fixture.now += 37
    with pytest.raises(NativeSourceCustodyError):
        fixture.run(1)
    assert len(fixture.delegate.calls) == 1


@pytest.mark.parametrize("changed", ["args", "timeout", "project", "profile"])
def test_changed_call_never_reaches_delegate(tmp_path, changed):
    fixture = InvocationFixture(tmp_path)
    args, cwd, timeout = fixture.args(0), fixture.project, 10
    if changed == "args":
        args = (*args, "--full-refresh")
    if changed == "timeout":
        timeout = 9
    if changed == "project":
        cwd = fixture.target
    if changed == "profile":
        args = tuple(str(fixture.target) if value == str(fixture.profile) else value for value in args)
    with pytest.raises(NativeSourceCustodyError):
        fixture.recorder.run(args, cwd=cwd, timeout_seconds=timeout, redactions=())
    assert not fixture.delegate.calls


def test_original_failure_before_credentials_and_before_next_call(tmp_path):
    fixture = InvocationFixture(tmp_path)
    fixture.recorder.validate_before_credentials()
    fixture.run(0)
    fixture.store.fail_reads = True
    with pytest.raises(OSError):
        fixture.run(1)
    assert len(fixture.delegate.calls) == 1
    with pytest.raises(NativeSourceCustodyError):
        fixture.recorder.require_completion()


def test_lost_publication_ack_reconciles_without_second_publication(tmp_path):
    fixture = InvocationFixture(tmp_path)
    fixture.store.lose_ack = True
    for index in range(3):
        fixture.run(index)
    with pytest.raises(OSError):
        fixture.recorder.require_completion()
    reference = fixture.recorder.require_completion()
    assert fixture.recorder.require_completion() == reference
    assert fixture.store.publications == 1
    assert len(fixture.delegate.calls) == 3


@pytest.mark.parametrize(
    "kind",
    [
        "trusted_dbt_invocation_completion_v1",
        "trusted_dbt_toolchain_v1",
        "trusted_dbt_qualification_v1",
        "trusted_dbt_owned_root_v1",
    ],
)
def test_original_kind_substitution_is_rejected_before_execution(tmp_path, kind):
    fixture = InvocationFixture(tmp_path)
    locator = fixture.executor.command.locator
    fixture.store.bound[locator] = replace(fixture.store.bound[locator], kind=kind)
    with pytest.raises(NativeSourceCustodyError):
        fixture.recorder.validate_before_credentials()
    assert not fixture.delegate.calls


def test_replaced_profile_directory_cannot_reuse_same_path(tmp_path):
    fixture = InvocationFixture(tmp_path)
    fixture.run(0)
    fixture.profile.rename(fixture.profile.with_name("old-profile"))
    fixture.profile.mkdir()
    with pytest.raises(NativeSourceCustodyError, match="identity changed"):
        fixture.run(1)
    assert len(fixture.delegate.calls) == 1


def test_symlink_profile_rejected_before_dispatch(tmp_path):
    fixture = InvocationFixture(tmp_path)
    fixture.profile.rmdir()
    fixture.profile.symlink_to(fixture.target, target_is_directory=True)
    with pytest.raises(NativeSourceCustodyError):
        fixture.run(0)
    assert not fixture.delegate.calls


def test_two_concurrent_same_slot_calls_execute_once(tmp_path):
    fixture = InvocationFixture(tmp_path)
    entered, release = Event(), Event()

    def blocked():
        entered.set()
        assert release.wait(5)
        return DbtCommandResult(0)

    fixture.delegate.action = blocked
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(fixture.run, 0)
        assert entered.wait(5)
        second = pool.submit(fixture.run, 0)
        release.set()
        assert first.result(timeout=5).exit_code == 0
        with pytest.raises(NativeSourceCustodyError):
            second.result(timeout=5)
    assert len(fixture.delegate.calls) == 1


def test_concurrent_completion_callers_publish_one_immutable_payload(tmp_path):
    fixture = InvocationFixture(tmp_path)
    for index in range(3):
        fixture.run(index)
    with ThreadPoolExecutor(max_workers=4) as pool:
        references = list(pool.map(lambda _: fixture.recorder.require_completion(), range(4)))
    assert len(set(references)) == 1
    assert fixture.store.publications == 1


def test_close_during_authentication_cannot_return_credentials_admission(tmp_path):
    fixture = InvocationFixture(tmp_path)
    entered, release = Event(), Event()
    read = fixture.store.read

    def blocked(*args, **kwargs):
        entered.set()
        assert release.wait(5)
        return read(*args, **kwargs)

    fixture.store.read = blocked
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(fixture.recorder.validate_before_credentials)
        assert entered.wait(5)
        try:
            with pytest.raises(NativeSourceCustodyError, match="UNKNOWN"):
                fixture.recorder.close(deadline_monotonic=fixture.now)
        finally:
            release.set()
        with pytest.raises(NativeSourceCustodyError):
            future.result(timeout=5)
    assert not fixture.delegate.calls


def test_short_close_deadline_is_enforced_even_if_waiter_observes_return_first(tmp_path):
    fixture = InvocationFixture(tmp_path, "QUALITY")
    entered, release, completed = Event(), Event(), Event()

    def blocked():
        entered.set()
        assert release.wait(5)
        fixture.now += 2
        return DbtCommandResult(0)

    fixture.delegate.action = blocked
    original_lock = fixture.recorder._sequence

    class DelayedCloseWaiter:
        def acquire(self, *, timeout):
            # Deterministically delay the close waiter while the admitted call
            # returns after its shorter close deadline, within the total budget.
            release.set()
            assert completed.wait(5)
            return original_lock.acquire(timeout=timeout)

        def release(self):
            original_lock.release()

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(fixture.run, 0)
        future.add_done_callback(lambda _: completed.set())
        assert entered.wait(5)
        fixture.recorder._sequence = DelayedCloseWaiter()
        with pytest.raises(NativeSourceCustodyError, match="UNKNOWN"):
            fixture.recorder.close(deadline_monotonic=fixture.now + 1)
        with pytest.raises(NativeSourceCustodyError, match="UNKNOWN"):
            future.result(timeout=5)
    with pytest.raises(NativeSourceCustodyError):
        fixture.recorder.require_completion()
    assert fixture.store.publications == 0
