"""Durable directory parsing rejects drift, ambiguity and unbounded input."""

import json
from dataclasses import replace
from uuid import UUID

import pytest

from dpone.contracts.mssql_tds_directory_codec import decode_directory
from tests.test_mssql_tds_directory import (
    LIMITS,
    PARENT,
    authority,
    authorize_retirement,
    close_admission,
    encode_directory,
    initial,
    recover,
    reserve,
    seal_work,
    settled,
)


def decode(payload, **overrides):
    return decode_directory(payload, parent=overrides.get("parent", PARENT), limits=overrides.get("limits", LIMITS))


def test_roundtrip_pending_chain_and_retirement_authority():
    pending = recover(reserve(initial()), 2, 2)
    assert decode(encode_directory(pending)) == pending
    complete = close_admission(authorize_retirement(seal_work(settled(settled(pending, 1))), authority(True)))
    assert decode(encode_directory(complete)) == complete
    assert encode_directory(
        decode(json.dumps(json.loads(encode_directory(complete)), indent=2).encode())
    ) == encode_directory(complete)


@pytest.mark.parametrize(
    "field",
    ["parent", "limits", "slots", "sequence", "schema", "work_sealed", "retirement_authority", "admission_closed"],
)
def test_missing_or_extra_fields(field):
    raw = json.loads(encode_directory(initial()))
    del raw[field]
    with pytest.raises(ValueError):
        decode(json.dumps(raw).encode())
    raw["private"] = "private-canary"
    with pytest.raises(ValueError) as caught:
        decode(json.dumps(raw).encode())
    assert str(caught.value) == "mssql_native.tds_directory_record_invalid"


@pytest.mark.parametrize(
    "mutation",
    [
        lambda d: d.update(sequence=True),
        lambda d: d.update(schema="v2"),
        lambda d: d.update(work_sealed=1),
        lambda d: d.update(slots={}),
        lambda d: d["parent"].update(extra="private-canary"),
        lambda d: d["parent"].update(ordinal=False),
        lambda d: d["limits"].update(max_entries=True),
        lambda d: d["slots"][0].update(index=1),
        lambda d: d["slots"][0].update(operation_id="{00000000-0000-0000-0000-000000000001}"),
        lambda d: d["slots"][0].update(command="arbitrary_sql"),
        lambda d: d["slots"][0].update(local_containment={"private": "private-canary"}),
        lambda d: d.update(slots=d["slots"] * (LIMITS.max_entries + 1)),
    ],
)
def test_invalid_nested_structure_and_aliases(mutation):
    raw = json.loads(encode_directory(reserve(initial())))
    mutation(raw)
    with pytest.raises(ValueError, match="mssql_native.tds_directory_record_invalid"):
        decode(json.dumps(raw).encode())


def test_full_parent_and_resource_policy_are_bound():
    body = encode_directory(initial())
    with pytest.raises(ValueError):
        decode(body, parent=replace(PARENT, file_sha256="b" * 64))
    with pytest.raises(ValueError):
        decode(body, limits=replace(LIMITS, max_entries=LIMITS.max_entries + 1))


@pytest.mark.parametrize(
    "body", [b"[]", b"null", b"\xff", b"{}", b'{"x":1,"x":2}', b'{"x":NaN}', b"[" * 2000, "text", bytearray(b"{}")]
)
def test_malformed_input_fails_with_static_error(body):
    with pytest.raises(ValueError) as caught:
        decode(body)
    assert str(caught.value) == "mssql_native.tds_directory_record_invalid"


def test_size_and_count_reject_before_nested_construction(monkeypatch):
    import dpone.contracts.mssql_tds_directory_codec as codec

    def forbidden(*args, **kwargs):
        pytest.fail("parser called for oversized record")

    monkeypatch.setattr(codec, "strict_json_object", forbidden)
    with pytest.raises(ValueError):
        decode(b"x" * (LIMITS.max_encoded_bytes + 1))


def test_changed_proof_binding_and_retirement_parent_are_rejected():
    state = authorize_retirement(seal_work(settled(reserve(initial()))), authority(True))
    raw = json.loads(encode_directory(state))
    raw["slots"][0]["remote_settlement"]["operation_id"] = str(UUID(int=50))
    with pytest.raises(ValueError):
        decode(json.dumps(raw).encode())
    raw = json.loads(encode_directory(state))
    raw["retirement_authority"]["identity"]["run_id"] = "different-run"
    with pytest.raises(ValueError):
        decode(json.dumps(raw).encode())


def test_entry_limit_precedes_slot_construction(monkeypatch):
    import dpone.contracts.mssql_tds_directory_codec as codec

    raw = json.loads(encode_directory(reserve(initial())))
    raw["slots"] *= LIMITS.max_entries + 1

    def forbidden(*args, **kwargs):
        pytest.fail("slot constructed before checking admitted count")

    monkeypatch.setattr(codec, "_slot", forbidden)
    with pytest.raises(ValueError):
        decode(json.dumps(raw).encode())
