"""Closed canonical seal and exact additive lineage, never repaired originals."""

import json
from dataclasses import replace

import pytest

from dpone.contracts.mssql_sqlclient_create_evidence import (
    MAX_CREATE_SEAL_BYTES,
    decode_create_seal,
    encode_create_seal,
    validate_create_lineage,
)
from dpone.contracts.mssql_sqlclient_stage_locator import decode_stage_locator_record
from dpone.contracts.mssql_tds_coordinator import coordinator_key
from dpone.contracts.strict_json import canonical_json_bytes
from tests.test_mssql_sqlclient_stage_locator_wiring import TracedHarness


@pytest.fixture
def sealed(tmp_path):
    h = TracedHarness(tmp_path)
    try:
        h.run()
        locator = decode_stage_locator_record(h.store.load(h.locator_key)).locator
        payload = h.store.load(coordinator_key(h.identity) + "/create-evidence/v1").payload.encode()
        yield locator, decode_create_seal(payload, locator), payload
    finally:
        h.cleanup()


def test_canonical_seal_and_cap_before_parse(sealed):
    locator, seal, payload = sealed
    assert encode_create_seal(seal) == payload
    assert len(payload) < 16384 + 6 * 1024 + 1024 < MAX_CREATE_SEAL_BYTES
    with pytest.raises(ValueError):
        decode_create_seal(b" " * (MAX_CREATE_SEAL_BYTES + 1), locator)
    with pytest.raises(ValueError):
        decode_create_seal(payload + b" ", locator)


@pytest.mark.parametrize(
    "fault",
    [
        "duplicate_key",
        "unknown_key",
        "wrong_domain",
        "locator_hash",
        "receipt_order",
        "missing_receipt",
        "duplicate_receipt",
        "wrong_revision",
        "receipt_size",
        "original_error",
    ],
)
def test_closed_seal_rejects_ambiguous_or_unbound_originals(sealed, fault):
    locator, _, payload = sealed
    body = json.loads(payload)
    if fault == "duplicate_key":
        payload = b'{"schema":"bad",' + payload[1:]
    else:
        if fault == "unknown_key":
            body["unknown"] = None
        elif fault == "wrong_domain":
            body["state_domain_id"] = "00000000-0000-0000-0000-000000000099"
        elif fault == "locator_hash":
            body["locator_sha256"] = "0" * 64
        elif fault == "receipt_order":
            body["receipts"].reverse()
        elif fault == "missing_receipt":
            body["receipts"].pop()
        elif fault == "duplicate_receipt":
            body["receipts"][1] = body["receipts"][0]
        elif fault == "wrong_revision":
            body["original_revision"] = True
        elif fault == "receipt_size":
            body["receipts"][0]["byte_count"] = 131073
        elif fault == "original_error":
            body["original"]["error"] = "unknown"
        payload = canonical_json_bytes(body)
    with pytest.raises(ValueError):
        decode_create_seal(payload, locator)


@pytest.mark.parametrize("takeovers", [0, 1, 2, 5])
def test_exact_lineage_accepts_only_possible_additive_takeovers(sealed, takeovers):
    _, seal, _ = sealed
    original = seal.original
    state = original.state
    owner = (
        state.ownership
        if not takeovers
        else replace(
            state.ownership,
            fence=state.ownership.fence + takeovers,
            supervisor_id=state.ownership.supervisor_id if takeovers > 1 else "00000000-0000-0000-0000-000000000099",
        )
    )
    current = replace(
        original,
        revision=original.revision + takeovers + 1,
        state=replace(state, ownership=owner, sequence=6 + takeovers),
    )
    validate_create_lineage(original, current)
    if takeovers:
        with pytest.raises(ValueError):
            validate_create_lineage(original, replace(current, revision=original.revision))
    with pytest.raises(ValueError):
        validate_create_lineage(original, replace(current, revision=original.revision - 1))


def test_original_nested_scalar_alias_is_not_normalized(sealed):
    _, seal, _ = sealed

    class Alias(str):
        pass

    object.__setattr__(
        seal.original.state.identity.parent, "database", Alias(seal.original.state.identity.parent.database)
    )
    with pytest.raises(ValueError):
        encode_create_seal(seal)


def test_seal_size_upper_bound_with_maximum_receipt_fields():
    from dataclasses import asdict

    from dpone.contracts.mssql_tds_coordinator_codec import MAX_COORDINATOR_RECORD_BYTES
    from dpone.contracts.mssql_tds_coordinator_evidence import TdsCoordinatorEvidenceKind, TdsCoordinatorEvidenceReceipt

    caps = (131072, 16384, 32768, 16384, 262144, 16384)
    receipt_bytes = 0
    for kind, cap in zip(TdsCoordinatorEvidenceKind, caps, strict=True):
        receipt = TdsCoordinatorEvidenceReceipt(
            "f" * 64, kind, f"tds-coordinator-{'f' * 64}-{kind.value}-{'f' * 64}.json", "f" * 64, cap
        )
        encoded = canonical_json_bytes(asdict(receipt))
        assert len(encoded) < 1024
        receipt_bytes += len(encoded) + 1
        with pytest.raises(ValueError):
            replace(receipt, byte_count=cap + 1)
    envelope = canonical_json_bytes(
        dict(
            schema="dpone.sqlclient.create-evidence.v1",
            state_domain_id="ffffffff-ffff-ffff-ffff-ffffffffffff",
            locator_sha256="f" * 64,
            original_revision=2**63 - 1,
            original={},
            receipts=[],
        )
    )
    assert len(envelope) < 1024
    assert MAX_COORDINATOR_RECORD_BYTES + receipt_bytes + len(envelope) < MAX_CREATE_SEAL_BYTES
