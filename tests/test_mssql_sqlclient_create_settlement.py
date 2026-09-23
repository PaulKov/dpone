"""Original composed CREATE/departure and actual two-ACK SQLite settlement."""

from time import monotonic
from uuid import uuid4

import pytest

from tests.test_mssql_sqlclient_stage_locator_wiring import TracedHarness


def test_original_departure_settles_before_next_observe_reservation(tmp_path):
    from dpone.app.mssql_sqlclient_create_settlement import settle_sqlclient_create_departure
    from dpone.contracts.mssql_tds_directory import TdsCoordinatorCommand

    h = TracedHarness(tmp_path)
    try:
        original = h.run()
        assert not h.attempt.directory.state.slots[0].settled
        result = settle_sqlclient_create_departure(
            h.attempt, admitted_factory=h.factory, pool=h.pool, deadline=monotonic() + 5
        )
        assert result.state.slots[0].settled
        assert h.attempt._create_departure_outcome is original
        next_slot = h.attempt.reserve_operation(
            uuid4(), TdsCoordinatorCommand.OBSERVE, "a" * 64, deadline=monotonic() + 5
        )
        assert len(next_slot.state.slots) == 2
    finally:
        h.cleanup()


@pytest.mark.parametrize("fault", ["replacement", "receipt", "wrong_slot", "process_alias"])
def test_forged_or_mutated_registered_origin_cannot_settle(tmp_path, fault):
    from copy import deepcopy

    from dpone.app.mssql_sqlclient_create_settlement import settle_sqlclient_create_departure

    h = TracedHarness(tmp_path)
    mutated = None
    try:
        outcome = h.run()
        before = h.attempt.directory
        if fault == "replacement":
            h.attempt._create_settlement.outcome = deepcopy(outcome)
        elif fault == "receipt":
            object.__setattr__(outcome.helper_outcome.receipts[-1], "payload_sha256", "f" * 64)
        elif fault == "wrong_slot":
            object.__setattr__(outcome.helper_outcome.plan.create_operation, "slot_index", 1)
        else:
            mutated = outcome.create_outcome.local_exit.identity, outcome.create_outcome.local_exit.identity.pid
            object.__setattr__(mutated[0], "pid", True)
        with pytest.raises(Exception):
            settle_sqlclient_create_departure(
                h.attempt, admitted_factory=h.factory, pool=h.pool, deadline=monotonic() + 5
            )
        assert h.attempt._directory.observation.snapshot == before
        assert h.attempt._poisoned
    finally:
        if mutated is not None:
            object.__setattr__(mutated[0], "pid", mutated[1])
        h.cleanup()


@pytest.mark.parametrize("fault", ["local_lost", "remote_lost", "wrong_ack", "caught_reentry", "final_assert"])
def test_directory_uncertainty_retains_partial_originals_and_blocks_reservation(tmp_path, monkeypatch, fault):
    from dataclasses import replace

    from dpone.app.mssql_sqlclient_create_settlement import settle_sqlclient_create_departure
    from dpone.contracts.mssql_tds_directory import TdsCoordinatorCommand
    from dpone.ports.mssql_tds_directory import RecordDirectoryContainment, RecordDirectorySettlement

    h = TracedHarness(tmp_path)
    try:
        h.run()
        original = h.attempt._directory.execute
        calls = []
        remote_saved = False

        def execute(request, *, deadline):
            nonlocal remote_saved
            if type(request) in (RecordDirectoryContainment, RecordDirectorySettlement):
                calls.append(type(request))
                if fault == "caught_reentry":
                    with pytest.raises(Exception):
                        h.attempt.reserve_operation(uuid4(), TdsCoordinatorCommand.OBSERVE, "a" * 64, deadline=deadline)
            observed = original(request, deadline=deadline)
            if type(request) is RecordDirectorySettlement:
                remote_saved = True
            if (
                fault == "local_lost"
                and type(request) is RecordDirectoryContainment
                or fault == "remote_lost"
                and type(request) is RecordDirectorySettlement
            ):
                raise OSError("lost directory ACK after actual save")
            if fault == "wrong_ack" and type(request) is RecordDirectoryContainment:
                return replace(observed, revision=observed.revision + 1)
            return observed

        monkeypatch.setattr(h.attempt._directory, "execute", execute)
        authority = h.attempt._lifecycle.assert_authority

        def assert_authority(*, deadline):
            if fault == "final_assert" and remote_saved:
                raise OSError("post-mutation authority lost")
            return authority(deadline=deadline)

        monkeypatch.setattr(h.attempt._lifecycle, "assert_authority", assert_authority)
        with pytest.raises(Exception):
            settle_sqlclient_create_departure(
                h.attempt, admitted_factory=h.factory, pool=h.pool, deadline=monotonic() + 5
            )
        retained = h.attempt._create_settlement
        assert retained.failed and not retained.complete and retained.attempt is h.attempt
        expected = 2 if fault in ("remote_lost", "final_assert") else 1
        assert len(calls) == expected
        if fault in ("remote_lost", "final_assert"):
            assert retained.local_ack is not None
        with pytest.raises(Exception):
            settle_sqlclient_create_departure(
                h.attempt, admitted_factory=h.factory, pool=h.pool, deadline=monotonic() + 5
            )
        with pytest.raises(Exception):
            h.attempt.reserve_operation(uuid4(), TdsCoordinatorCommand.OBSERVE, "a" * 64, deadline=monotonic() + 5)
        assert len(calls) == expected
    finally:
        h.cleanup()


def test_process_identity_domain_has_fixed_ascii_vector_and_rejects_alias():
    from dataclasses import asdict

    from dpone.contracts.mssql_tds_directory_codec import process_identity_digest
    from dpone.contracts.mssql_tds_worker import TdsProcessIdentity
    from dpone.contracts.strict_json import canonical_json_bytes

    process = TdsProcessIdentity("a" * 64, "11111111-1111-4111-8111-111111111111", 123, 456)
    assert canonical_json_bytes(asdict(process)) == (
        b'{"boot_id":"11111111-1111-4111-8111-111111111111",'
        b'"host_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","pid":123,"start_ticks":456}'
    )
    assert process_identity_digest(process) == "72173b1530fb18595d0c9de57a00df21c0c6e942646dd03cc2fba42532a63137"
    object.__setattr__(process, "pid", True)
    with pytest.raises(ValueError):
        process_identity_digest(process)
