"""Owned direct-node transport evidence; synthetic registration is not sealing."""

import importlib
import multiprocessing
import os
from dataclasses import replace

import pytest

from dpone.adapters.clickhouse_authority_execution_lock import LocalPublicationExclusion
from dpone.adapters.clickhouse_authority_publisher import AuthorityPublicationPublisher
from dpone.adapters.clickhouse_authority_sqlite import SQLitePublicationAuthority
from dpone.adapters.clickhouse_native_publication import DirectNativePublicationTransport
from dpone.contracts.clickhouse_authority import AuthorityConflict, TransportState
from dpone.runtime.sinks.clickhouse_guarded_publication import PublicationUnknown

pytestmark = [pytest.mark.integration_live, pytest.mark.integration_clickhouse]


@pytest.fixture
def case(tmp_path, request):
    if os.environ.get("DPONE_NATIVE_PUBLICATION_LIVE") != "1":
        pytest.skip("Requires explicitly owned direct ClickHouse fixture")
    support = importlib.import_module("tests.integration.clickhouse_native_publication_support")
    with support.PublicationCase(tmp_path, request.node.name) as fixture:
        yield fixture


@pytest.mark.parametrize(
    "mode,expected_method",
    [
        ("partition", "replace_partition"),
        ("unpartitioned", "replace_partition"),
        ("empty", "exchange"),
        ("stale", "exchange"),
        ("absent", "rename"),
        ("same", "noop"),
    ],
)
def test_actual_method_rows_uuid_and_restart_closure(case, mode, expected_method):
    operation, grant, before = case.prepare(mode)
    assert case.store.read(operation).record.intent.method == expected_method
    case.publisher.execute_once(grant)
    after = case.snapshot()
    assert after["target_rows"] == before["candidate_rows"]
    if expected_method == "replace_partition":
        assert after["target_uuid"] == before["target_uuid"]
        assert after["candidate_uuid"] == before["candidate_uuid"]
        assert after["candidate_rows"] == before["candidate_rows"]
    elif expected_method == "exchange":
        assert (after["target_uuid"], after["candidate_uuid"]) == (before["candidate_uuid"], before["target_uuid"])
        assert after["candidate_rows"] == before["target_rows"]
    elif expected_method == "rename":
        assert after["target_uuid"] == before["candidate_uuid"]
        assert after["candidate_uuid"] is None
    else:
        assert after == before
    expected = TransportState.CLOSED_WITHOUT_SEND if mode == "same" else TransportState.CLOSED_TERMINAL
    assert case.store.transport_state(operation) == expected
    assert _recover_in_spawn(case.path, operation) == "closed"
    assert case.snapshot() == after
    with pytest.raises(AuthorityConflict):
        case.store.acquire("deployment:successor", case.binding.subject, "next_candidate")
    case.evidence(operation, before, after, {"method": expected_method})


def test_tuple_fixture_probe_equivalent_to_all_canonical_id(case):
    operation, _, before = case.prepare("unpartitioned")
    case.client.execute(
        f"ALTER TABLE `{case.database}`.target REPLACE PARTITION tuple() FROM `{case.database}`.candidate"
    )
    after = case.snapshot()
    assert after["target_rows"] == before["candidate_rows"]
    assert after["target_uuid"] == before["target_uuid"]
    assert case.store.transport_state(operation) == TransportState.NOT_STARTED
    case.evidence(operation, before, after, {"fixture_only_tuple_probe": True})


def test_actual_server_exception_is_not_terminal_proof(case):
    operation, grant, before = case.prepare("partition")
    case.client.execute(f"DROP TABLE `{case.database}`.candidate")
    with pytest.raises(PublicationUnknown):
        case.publisher.execute_once(grant)
    assert case.store.transport_state(operation) == TransportState.MAY_HAVE_SENT
    assert _recover_in_spawn(case.path, operation) == "unknown"
    after = case.snapshot()
    assert after["target_rows"] == before["target_rows"]
    case.evidence(operation, before, after, {"actual_server_exception": True})


@pytest.mark.parametrize("fault", ["lost_response", "truncated_packet"])
def test_exchange_effect_then_response_loss_is_never_replayed(case, fault):
    from tests.integration.clickhouse_native_publication_support import ResponseFaultRelay

    operation, grant, before = case.prepare("stale")
    query_id = case.store.read(operation).record.intent.query_id

    def effect_visible():
        return case.snapshot()["target_uuid"] == before["candidate_uuid"]

    with ResponseFaultRelay(case.endpoint.host, case.endpoint.port, query_id, fault, effect_visible) as relay:
        endpoint = replace(case.endpoint, host="127.0.0.1", port=relay.port)
        service = AuthorityPublicationPublisher(
            case.store, LocalPublicationExclusion(case.store), DirectNativePublicationTransport(endpoint)
        )
        with pytest.raises(PublicationUnknown):
            service.execute_once(grant)
        assert relay.activated.wait(10), "Fault must hit actual native server response"
        relay.check()
        assert relay.query_occurrences == 1
        assert relay.held_response_bytes > 0
    after = case.snapshot()
    assert (after["target_uuid"], after["candidate_uuid"]) == (before["candidate_uuid"], before["target_uuid"])
    assert after["target_rows"] == before["candidate_rows"]
    assert case.store.transport_state(operation) == TransportState.MAY_HAVE_SENT
    assert _recover_in_spawn(case.path, operation) == "unknown"
    with pytest.raises(PublicationUnknown):
        case.publisher.execute_once(grant)
    assert case.snapshot() == after
    case.evidence(
        operation,
        before,
        after,
        {
            "fault": fault,
            "activated": True,
            "query_occurrences": relay.query_occurrences,
            "held_response_bytes": relay.held_response_bytes,
        },
    )


class _NeverSend:
    def execute(self, request):
        raise AssertionError("Recovery must not call transport")


def _recover(path, operation, results):
    store = SQLitePublicationAuthority(path, "deployment")
    service = AuthorityPublicationPublisher(store, LocalPublicationExclusion(store), _NeverSend())
    try:
        service.close_and_drain(operation)
    except PublicationUnknown:
        results.put("unknown")
    else:
        results.put("closed")


def _recover_in_spawn(path, operation):
    ctx = multiprocessing.get_context("spawn")
    results = ctx.Queue()
    process = ctx.Process(target=_recover, args=(path, operation, results))
    process.start()
    try:
        outcome = results.get(timeout=20)
        process.join(20)
        assert process.exitcode == 0
        return outcome
    finally:
        if process.is_alive():
            process.kill()
            process.join()
        results.close()


def _crash_after_send_entry(path, grant):
    store = SQLitePublicationAuthority(path, "deployment")
    with LocalPublicationExclusion(store).hold(grant.operation_id):
        store.begin_send(grant)
        os._exit(19)


def test_process_death_after_send_entry_before_bytes_remains_unknown(case):
    operation, grant, before = case.prepare("stale")
    process = multiprocessing.get_context("spawn").Process(target=_crash_after_send_entry, args=(case.path, grant))
    process.start()
    process.join(20)
    if process.is_alive():
        process.kill()
        process.join()
    assert process.exitcode == 19
    assert _recover_in_spawn(case.path, operation) == "unknown"
    assert case.snapshot() == before
    with pytest.raises(PublicationUnknown):
        case.publisher.execute_once(grant)
    case.evidence(operation, before, case.snapshot(), {"crash_exit": 19, "network_calls": 0})
