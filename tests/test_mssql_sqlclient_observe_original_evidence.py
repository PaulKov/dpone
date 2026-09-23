"""Three original nonsecret writes remain distinct from attempted/lost ACKs."""

import pytest

from dpone.contracts.mssql_tds_coordinator_evidence import TdsCoordinatorEvidenceKind as Kind
from tests.test_mssql_sqlclient_observe_composition import observe_parent as observe_parent
from tests.test_mssql_sqlclient_preparation_composition import composed_preparation as composed_preparation


def test_original_three_actual_ack_pairs(composed_preparation):
    h = composed_preparation
    original = h.handle.continuation.retained_evidence()
    assert tuple(record.kind for record, _, _ in original) == (Kind.ADMISSION, Kind.REGISTRATION, Kind.AUTHORITY)
    for record, receipt, observation in original:
        assert receipt == observation.receipt
        assert record.payload == (h.evidence_root / receipt.relative_name).read_bytes()
        assert receipt == record.receipt


def test_consumed_original_write_cannot_replay(composed_preparation):
    c = composed_preparation.handle.continuation
    original = c.retained_evidence()
    with pytest.raises(Exception):
        c._persist(Kind.AUTHORITY, original[-1][0].payload)
    assert c._failed


@pytest.mark.parametrize("fault", ["lost", "late", "alias"])
def test_actual_write_uncertainty_never_becomes_original_ack(observe_parent, tmp_path, monkeypatch, fault):
    from contextlib import contextmanager
    from time import monotonic

    from dpone.adapters.filesystem_evidence import DescriptorPinnedCreateOnlyEvidenceWriter
    from dpone.adapters.mssql_tds_coordinator_evidence_actor import TdsCoordinatorEvidenceActor
    from dpone.contracts.strict_json import canonical_json_bytes
    from tests.test_mssql_sqlclient_observe_composition import retained_parent
    from tests.test_mssql_tds_coordinator_evidence import OUTERS

    h = retained_parent(observe_parent)
    c = h.continuation

    @contextmanager
    def factory():
        yield DescriptorPinnedCreateOnlyEvidenceWriter(tmp_path)

    actor = h.pool.open(
        lambda d, clock: TdsCoordinatorEvidenceActor(factory, c.operation_sha256, d, clock), deadline=c.deadline
    )
    h.evidence = actor
    original = actor.write
    calls = []

    def write(*args, **kwargs):
        calls.append(1)
        result = original(*args, **kwargs)
        if fault == "lost":
            raise OSError("lost ACK after actual write")
        if fault == "late":
            c.deadline = monotonic() - 1
        else:
            object.__setattr__(result, "byte_count", float(result.byte_count))
        return result

    monkeypatch.setattr(actor, "write", write)
    payload = canonical_json_bytes(OUTERS[Kind.ADMISSION])
    with pytest.raises(Exception):
        c.bind_evidence(actor, payload)
    assert len(calls) == 1 and c._failed and c._evidence_acks == ()
    pending = c._evidence_attempts[Kind.ADMISSION]
    assert pending[0].payload is payload
    assert (pending[1] is None) is (fault == "lost")
    with pytest.raises(Exception):
        c._persist(Kind.ADMISSION, payload)
    assert len(calls) == 1
