from dataclasses import asdict

import pytest

from dpone.contracts.mssql_native_parent_journal import (
    NativeCheckpointReceipt,
    NativeChunkRetirementReceipt,
    NativeParentAuthority,
    NativeParentRetirementReceipt,
)
from dpone.contracts.mssql_tds_directory import TdsDirectoryLimits
from dpone.ports.mssql_native_route_backend import (
    NativeInputCustodyReceipt,
    NativeParentSettlementBinding,
    custody_request_digest,
)
from dpone.runtime.mssql_native_parent_settlement import SqlClientNativeParentSettlement

H = "a" * 64


def _chunk(ordinal: int, authority: NativeParentAuthority) -> NativeChunkRetirementReceipt:
    return NativeChunkRetirementReceipt(ordinal, f"attempt-{ordinal}", authority.digest, *(H for _ in range(8)))


def _checkpoint(retirement: NativeParentRetirementReceipt) -> NativeCheckpointReceipt:
    return NativeCheckpointReceipt(retirement.digest, "target", "window", 7, 9, H)


class Journal:
    def __init__(self, phase="published", prefix=0, total=2):
        self.parent = NativeParentAuthority("published", H, 7)
        self.total = total
        self.chunks = [_chunk(i, self.parent) for i in range(prefix)]
        self.retirement = None
        self.checkpoint = None
        self.phase = phase
        self.calls = []
        if phase in {"retired", "checkpoint_required", "succeeded"}:
            self.chunks = [_chunk(i, self.parent) for i in range(total)]
            self.retirement = NativeParentRetirementReceipt(self.parent.digest, tuple(self.chunks))
        if phase == "succeeded":
            self.checkpoint = _checkpoint(self.retirement)

    def state(self):
        return {
            "phase": self.phase,
            "chunk_retirements": [asdict(value) for value in self.chunks],
            "checkpoint_receipt": None if self.checkpoint is None else self.checkpoint.to_dict(),
        }

    def authority(self):
        return self.parent

    def settlement_binding(self):
        return NativeParentSettlementBinding("target", "window", 7, self.total)

    def retirement_required(self, authority):
        assert authority == self.parent
        self.calls.append("retirement_required")
        self.phase = "retirement_required"

    def retiring(self):
        self.calls.append("retiring")
        self.phase = "retiring"

    def chunk_retired(self, receipt):
        self.calls.append(f"chunk:{receipt.ordinal}")
        self.chunks.append(receipt)

    def retired(self):
        self.calls.append("retired")
        self.retirement = NativeParentRetirementReceipt(self.parent.digest, tuple(self.chunks))
        self.phase = "retired"
        return self.retirement

    def retirement_receipt(self):
        return self.retirement

    def checkpoint_required(self):
        self.calls.append("checkpoint_required")
        self.phase = "checkpoint_required"

    def succeeded(self, receipt):
        self.calls.append("succeeded")
        self.checkpoint = receipt
        self.phase = "succeeded"


class Inspector:
    def __init__(self, calls):
        self.calls = calls

    def inspect(self, ordinal, limits):
        assert isinstance(limits, TdsDirectoryLimits)
        self.calls.append(f"inspect:{ordinal}")
        return f"projection:{ordinal}"


class Retirer:
    def __init__(self, journal, calls):
        self.journal, self.calls = journal, calls

    def retire(self, projection, authority):
        ordinal = int(projection.rsplit(":", 1)[1])
        assert authority == self.journal.parent
        self.calls.append(f"retire:{ordinal}")
        return _chunk(ordinal, authority)


class Custody:
    def __init__(self, calls):
        self.calls = calls

    def release(self, request):
        self.calls.append(f"custody:{request.retirement_digest}")
        return NativeInputCustodyReceipt(custody_request_digest(request), H)


class Checkpoint:
    def __init__(self, calls):
        self.calls = calls

    def advance(self, request):
        self.calls.append(f"checkpoint:{request.retirement_digest}")
        return NativeCheckpointReceipt(
            request.retirement_digest,
            request.target_id,
            request.window_fingerprint,
            request.fence,
            9,
            H,
        )


def _service(journal):
    effects = []
    return (
        SqlClientNativeParentSettlement(
            journal=journal,
            inspector=Inspector(effects),
            retirer=Retirer(journal, effects),
            custody=Custody(effects),
            checkpoint=Checkpoint(effects),
            directory_limits=TdsDirectoryLimits(4, 2, 4096, 1024),
        ),
        effects,
    )


def test_published_settles_ordered_missing_suffix_then_custody_then_checkpoint():
    journal = Journal()
    service, effects = _service(journal)
    result = service.settle()
    assert journal.calls == [
        "retirement_required",
        "retiring",
        "chunk:0",
        "chunk:1",
        "retired",
        "checkpoint_required",
        "succeeded",
    ]
    assert effects[:4] == ["inspect:0", "retire:0", "inspect:1", "retire:1"]
    assert effects[4].startswith("custody:")
    assert effects[5].startswith("checkpoint:")
    assert result.checkpoint == journal.checkpoint


def test_aborted_authority_uses_the_same_exact_retirement_path():
    journal = Journal()
    journal.parent = NativeParentAuthority("aborted", H, 7)
    service, effects = _service(journal)
    result = service.settle()
    assert result.authority.kind == "aborted"
    assert effects[:2] == ["inspect:0", "retire:0"]


def test_retiring_recovery_only_executes_missing_ordered_suffix():
    journal = Journal("retiring", prefix=1)
    service, effects = _service(journal)
    service.settle()
    assert effects[:2] == ["inspect:1", "retire:1"]
    assert "inspect:0" not in effects


@pytest.mark.parametrize(
    ("phase", "effect_prefix"),
    (("retired", ["custody:", "checkpoint:"]), ("checkpoint_required", ["checkpoint:"]), ("succeeded", [])),
)
def test_terminal_recovery_is_stage_and_source_free(phase, effect_prefix):
    journal = Journal(phase)
    service, effects = _service(journal)
    first = service.settle()
    assert len(effects) == len(effect_prefix)
    assert all(value.startswith(prefix) for value, prefix in zip(effects, effect_prefix, strict=True))
    assert first.retirement == journal.retirement


@pytest.mark.parametrize("phase", ("preparing", "prepared", "publishing", "abort_required", "unknown"))
def test_unsettled_or_unknown_parent_never_advances_or_resends(phase):
    journal = Journal(phase)
    service, effects = _service(journal)
    with pytest.raises(RuntimeError, match="parent_settlement_unavailable"):
        service.settle()
    assert journal.calls == []
    assert effects == []


def test_invalid_checkpoint_result_is_not_persisted():
    journal = Journal("checkpoint_required")
    service, effects = _service(journal)
    service._checkpoint = type("Bad", (), {"advance": lambda self, request: object()})()
    with pytest.raises(ValueError, match="checkpoint_receipt_invalid"):
        service.settle()
    assert journal.calls == []
    assert effects == []


def test_checkpoint_from_stale_fence_is_not_persisted():
    journal = Journal("checkpoint_required")
    service, _ = _service(journal)
    stale = NativeCheckpointReceipt(journal.retirement.digest, "target", "window", 8, 9, H)
    service._checkpoint = type("Stale", (), {"advance": lambda self, request: stale})()
    with pytest.raises(ValueError, match="checkpoint_receipt_invalid"):
        service.settle()
    assert journal.calls == []


def test_out_of_order_chunk_receipt_is_not_persisted():
    journal = Journal("retiring")
    service, _ = _service(journal)
    service._retirer = type(
        "WrongOrder",
        (),
        {"retire": lambda self, projection, authority: _chunk(1, authority)},
    )()
    with pytest.raises(ValueError, match="chunk_retirement_receipt_invalid"):
        service.settle()
    assert journal.chunks == []


def test_lost_custody_ack_replays_same_identity_before_checkpoint():
    journal = Journal("retired")
    service, effects = _service(journal)
    requests = []

    class LostAck:
        def release(self, request):
            requests.append(request)
            if len(requests) == 1:
                raise RuntimeError("lost ack")
            return NativeInputCustodyReceipt(custody_request_digest(request), H)

    service._custody = LostAck()
    with pytest.raises(RuntimeError, match="lost ack"):
        service.settle()
    assert journal.phase == "retired"
    assert effects == []
    service.settle()
    assert requests[0] == requests[1]
    assert journal.phase == "succeeded"


def test_checkpoint_effect_receives_exact_journal_identity():
    journal = Journal("checkpoint_required")
    service, _ = _service(journal)
    seen = []

    class ExactCheckpoint:
        def advance(self, request):
            seen.append(request)
            return NativeCheckpointReceipt(
                request.retirement_digest,
                request.target_id,
                request.window_fingerprint,
                request.fence,
                9,
                H,
            )

    service._checkpoint = ExactCheckpoint()
    service.settle()
    assert (seen[0].target_id, seen[0].window_fingerprint, seen[0].fence) == ("target", "window", 7)


def test_lost_checkpoint_ack_replays_the_identical_cas_request():
    journal = Journal("checkpoint_required")
    service, _ = _service(journal)
    requests = []

    class LostAck:
        def advance(self, request):
            requests.append(request)
            if len(requests) == 1:
                raise RuntimeError("lost checkpoint ack")
            return NativeCheckpointReceipt(
                request.retirement_digest,
                request.target_id,
                request.window_fingerprint,
                request.fence,
                9,
                H,
            )

    service._checkpoint = LostAck()
    with pytest.raises(RuntimeError, match="lost checkpoint ack"):
        service.settle()
    assert journal.phase == "checkpoint_required"
    service.settle()
    assert requests[0] == requests[1]
