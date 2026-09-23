import pytest

from dpone.adapters.mssql_sqlclient_checkpoint import SqlClientCheckpointCas
from dpone.app.mssql_sqlclient_parent_checkpoint_bridge import SqlClientParentCheckpointBridge
from dpone.contracts.mssql_native_parent_journal import (
    NativeChunkRetirementReceipt,
    NativeParentRetirementReceipt,
)
from dpone.ports.mssql_native_route_backend import (
    NativeCheckpointRequest,
    NativeParentSettlementBinding,
)

H = "a" * 64


def _retirement() -> NativeParentRetirementReceipt:
    chunk = NativeChunkRetirementReceipt(0, "attempt", H, H, H, H, H, H, H, H, H)
    return NativeParentRetirementReceipt(H, (chunk,))


class Journal:
    def __init__(
        self,
        retirement: NativeParentRetirementReceipt | None,
        binding: NativeParentSettlementBinding,
    ) -> None:
        self.retirement = retirement
        self.binding = binding
        self.retirement_reads = 0

    def retirement_receipt(self) -> NativeParentRetirementReceipt | None:
        self.retirement_reads += 1
        return self.retirement

    def settlement_binding(self) -> NativeParentSettlementBinding:
        return self.binding


def _binding() -> NativeParentSettlementBinding:
    return NativeParentSettlementBinding("target", "window", 3, 1)


def test_checkpoint_bridge_reconciles_lost_ack_against_exact_parent_evidence() -> None:
    retirement = _retirement()
    proof = SqlClientCheckpointCas.proof(
        retirement,
        target_id="target",
        window_fingerprint="window",
        fence=3,
        revision=8,
    )
    cas_calls: list[tuple[object, ...]] = []

    def cas(*args: object) -> tuple[int, str]:
        cas_calls.append(args)
        raise TimeoutError

    checkpoint = SqlClientCheckpointCas(cas=cas, observe=lambda *_: (8, proof))
    journal = Journal(retirement, _binding())
    bridge = SqlClientParentCheckpointBridge(journal=journal, checkpoint=checkpoint)

    receipt = bridge.advance(NativeCheckpointRequest.bind(retirement, _binding()))

    assert receipt.parent_retirement_digest == retirement.digest
    assert receipt.checkpoint_cas_revision == 8
    assert journal.retirement_reads == 1
    assert cas_calls == [("target", "window", 3, retirement.digest)]


@pytest.mark.parametrize(
    ("retirement", "binding", "checkpoint_request"),
    [
        (None, _binding(), NativeCheckpointRequest(H, "target", "window", 3)),
        (_retirement(), NativeParentSettlementBinding("other", "window", 3, 1), None),
        (_retirement(), NativeParentSettlementBinding("target", "other", 3, 1), None),
        (_retirement(), NativeParentSettlementBinding("target", "window", 4, 1), None),
        (_retirement(), _binding(), NativeCheckpointRequest(H, "target", "window", 3)),
    ],
)
def test_checkpoint_bridge_rejects_missing_or_changed_parent_facts_before_cas(
    retirement: NativeParentRetirementReceipt | None,
    binding: NativeParentSettlementBinding,
    checkpoint_request: NativeCheckpointRequest | None,
) -> None:
    cas_calls: list[tuple[object, ...]] = []
    observe_calls: list[tuple[object, ...]] = []
    checkpoint = SqlClientCheckpointCas(
        cas=lambda *args: cas_calls.append(args) or (1, H),
        observe=lambda *args: observe_calls.append(args) or None,
    )
    journal = Journal(retirement, binding)
    bridge = SqlClientParentCheckpointBridge(journal=journal, checkpoint=checkpoint)
    if checkpoint_request is None:
        exact = _retirement()
        checkpoint_request = NativeCheckpointRequest.bind(exact, _binding())

    with pytest.raises(RuntimeError, match="parent_checkpoint_evidence_changed"):
        bridge.advance(checkpoint_request)

    assert journal.retirement_reads == 1
    assert cas_calls == []
    assert observe_calls == []


def test_checkpoint_bridge_rejects_non_request_without_read_or_effect() -> None:
    journal = Journal(_retirement(), _binding())
    checkpoint = SqlClientCheckpointCas(cas=lambda *_: (1, H), observe=lambda *_: None)
    bridge = SqlClientParentCheckpointBridge(journal=journal, checkpoint=checkpoint)

    with pytest.raises(ValueError, match="checkpoint_request_invalid"):
        bridge.advance(object())  # type: ignore[arg-type]

    assert journal.retirement_reads == 0
