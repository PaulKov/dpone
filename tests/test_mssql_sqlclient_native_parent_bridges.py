from concurrent.futures import ThreadPoolExecutor
from threading import Lock

import pytest

from dpone.app.mssql_sqlclient_native_parent_bridges import (
    SqlClientParentInputCustody,
    SqlClientParentJournalBridge,
    terminal_parent_result,
)
from dpone.contracts.mssql_native_parent_journal import (
    NativeCheckpointReceipt,
    NativeChunkRetirementReceipt,
    NativeParentAuthority,
    NativeParentRetirementReceipt,
    canonical_digest,
)
from dpone.ports.mssql_native_route_backend import NativeInputCustodyRequest


class Journal:
    def __init__(self):
        self.data = {"version": 4, "identity": {"target_id": "t", "window_fingerprint": "w"}, "chunks": {"0": {}}}
        self.phase = "prepared"
        self.calls = []

    def state(self):
        return {"phase": self.phase}

    def authority(self):
        return NativeParentAuthority("aborted", "a" * 64, 7) if self.phase == "aborted" else None

    def abort_required(self):
        self.calls.append("intent")
        self.phase = "abort_required"

    def abort_confirmed(self, receipt):
        self.calls.append(("confirmed", receipt))
        self.phase = "aborted"
        return self.authority()


def test_parent_bridge_binds_v4_identity_and_orders_abort():
    journal = Journal()
    bridge = SqlClientParentJournalBridge(
        journal, fence=lambda: 7, rollback_no_commit=lambda: journal.calls.append("observe") or {"proof": "ok"}
    )
    assert bridge.settlement_binding().chunk_count == 1
    assert bridge.abort().kind == "aborted"
    assert journal.calls == ["intent", "observe", ("confirmed", {"proof": "ok"})]


class DurableCustody:
    def __init__(self):
        self._lock = Lock()
        self.receipts = {}
        self.calls = []

    def observe_or_advance(self, request_sha, advance):
        with self._lock:
            self.calls.append(request_sha)
            prior = self.receipts.get(request_sha)
            if prior is not None:
                return prior
            receipt = advance()
            self.receipts[request_sha] = receipt
            return receipt


def test_parent_input_release_is_request_bound_and_durable_across_restart():
    chunk = NativeChunkRetirementReceipt(0, "run-0-0", "a" * 64, *(["b" * 64] * 8))
    retirement = NativeParentRetirementReceipt("a" * 64, (chunk,))
    calls = []
    durable = DurableCustody()
    custody = SqlClientParentInputCustody(
        lambda: retirement,
        lambda value: calls.append(value) or "c" * 64,
        durable,
    )
    request = NativeInputCustodyRequest(retirement.digest, "t", "w", 7)
    first = custody.release(request)
    restarted = SqlClientParentInputCustody(
        lambda: retirement,
        lambda value: calls.append(value) or "d" * 64,
        durable,
    )
    assert restarted.release(request) == first
    assert calls == [retirement]


def test_parent_input_release_reconciles_lost_ack_without_releasing_again():
    chunk = NativeChunkRetirementReceipt(0, "run-0-0", "a" * 64, *(["b" * 64] * 8))
    retirement = NativeParentRetirementReceipt("a" * 64, (chunk,))
    calls = []

    class LostAckCustody(DurableCustody):
        def __init__(self):
            super().__init__()
            self.lost = True

        def observe_or_advance(self, request_sha, advance):
            receipt = super().observe_or_advance(request_sha, advance)
            if self.lost:
                self.lost = False
                raise TimeoutError("lost acknowledgement")
            return receipt

    durable = LostAckCustody()
    request = NativeInputCustodyRequest(retirement.digest, "t", "w", 7)
    with pytest.raises(RuntimeError, match="input_custody_outcome_unknown"):
        SqlClientParentInputCustody(
            lambda: retirement,
            lambda value: calls.append(value) or "c" * 64,
            durable,
        ).release(request)
    receipt = SqlClientParentInputCustody(
        lambda: retirement,
        lambda value: calls.append(value) or "d" * 64,
        durable,
    ).release(request)
    assert receipt.release_sha256 == "c" * 64
    assert calls == [retirement]


def test_parent_input_release_is_serialized_by_durable_capability():
    chunk = NativeChunkRetirementReceipt(0, "run-0-0", "a" * 64, *(["b" * 64] * 8))
    retirement = NativeParentRetirementReceipt("a" * 64, (chunk,))
    calls = []
    durable = DurableCustody()
    request = NativeInputCustodyRequest(retirement.digest, "t", "w", 7)

    def release():
        return SqlClientParentInputCustody(
            lambda: retirement,
            lambda value: calls.append(value) or "c" * 64,
            durable,
        ).release(request)

    with ThreadPoolExecutor(max_workers=2) as pool:
        receipts = tuple(pool.map(lambda _: release(), range(2)))
    assert receipts[0] == receipts[1]
    assert calls == [retirement]


def test_parent_input_release_rejects_mismatched_durable_receipt():
    chunk = NativeChunkRetirementReceipt(0, "run-0-0", "a" * 64, *(["b" * 64] * 8))
    retirement = NativeParentRetirementReceipt("a" * 64, (chunk,))
    request = NativeInputCustodyRequest(retirement.digest, "t", "w", 7)

    class Mismatch:
        def observe_or_advance(self, _request_sha, _advance):
            from dpone.ports.mssql_native_route_backend import NativeInputCustodyReceipt

            return NativeInputCustodyReceipt("d" * 64, "c" * 64)

    custody = SqlClientParentInputCustody(lambda: retirement, lambda _: "c" * 64, Mismatch())
    with pytest.raises(ValueError, match="input_custody_receipt_invalid"):
        custody.release(request)


def test_terminal_result_requires_digest_bound_succeeded_receipt():
    receipt = NativeCheckpointReceipt("a" * 64, "t", "w", 7, 1, "b" * 64)
    state = {
        "phase": "succeeded",
        "checkpoint_receipt": receipt.to_dict(),
        "checkpoint_receipt_digest": canonical_digest(receipt.to_dict()),
    }
    assert terminal_parent_result(state) == receipt
