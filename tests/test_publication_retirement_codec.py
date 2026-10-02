"""Persisted provenance decoding; parsing alone never authenticates a freeze."""

import json
from dataclasses import replace
from importlib import import_module

import pytest

from dpone.contracts.clickhouse_cluster_publication import AuthorityPhase, canonical_json
from tests.test_publication_retirement import observation, plan


def decode(raw):
    return import_module("dpone.contracts.publication_retirement_codec").decode_retirement_plan(raw)


def test_decode_preserves_original_bytes_versions_and_historical_observations():
    value = plan()
    decoded = decode(value.payload.encode())
    assert decoded == value
    assert decoded.original_payload == value.original_payload
    assert decoded.original.version == 1
    assert decoded.original.record.phase is AuthorityPhase.PREPARED
    assert decoded.observation.freeze.expires_at == 300
    assert decoded.payload.encode() == value.payload.encode()
    assert not hasattr(decoded, "permit")


def test_decode_allows_original_first_publication_without_predecessor():
    value = observation()
    replicas = tuple(
        replace(
            item,
            authority=replace(item.authority, record=replace(item.authority.record, predecessor=None)),
            original_payload=replace(item.authority.record, predecessor=None).payload.encode(),
            generation=replace(item.generation, target=None),
        )
        for item in value.replicas
    )
    decoded = decode(plan(replace(value, replicas=replicas)).payload.encode())
    assert decoded.original.record.predecessor is None
    assert all(item.generation.target is None for item in decoded.observation.replicas)


@pytest.mark.parametrize("raw", [b"", b"x" * (1024 * 1024 + 1), b"\xff", b"[]", b"null", b"{}", "not bytes"])
def test_decode_rejects_invalid_bounded_bytes(raw):
    with pytest.raises(ValueError):
        decode(raw)


@pytest.mark.parametrize("variant", ["pretty", "duplicate", "trailing", "unknown_contract", "extra", "missing"])
def test_decode_rejects_noncanonical_or_different_contract(variant):
    data = json.loads(plan().payload)
    if variant == "unknown_contract":
        data["contract"] = "dpone.publication-retirement-plan.v2"
    elif variant == "extra":
        data["frozen"] = True
    elif variant == "missing":
        del data["observation"]["replicas"][0]["pending_requests"]
    raw = canonical_json(data).encode()
    if variant == "pretty":
        raw = json.dumps(data, indent=2).encode()
    elif variant == "duplicate":
        raw = b'{"contract":"ignored",' + raw[1:]
    elif variant == "trailing":
        raw += b"\n"
    with pytest.raises(ValueError):
        decode(raw)


@pytest.mark.parametrize(
    "variant", ["version_bool", "history_gap", "phase", "bytes", "freeze_scope", "replica_missing", "replica_empty"]
)
def test_decode_revalidates_historical_policy_not_only_json_shape(variant):
    data = json.loads(plan().payload)
    observed = data["observation"]
    first = observed["replicas"][0]
    if variant == "version_bool":
        first["authority"]["version"] = True
    elif variant == "history_gap":
        first["history_gaps"] = [[110, 120]]
    elif variant == "phase":
        first["authority"]["record"]["phase"] = "COMPLETED"
    elif variant == "bytes":
        first["original_payload"] = "00"
    elif variant == "freeze_scope":
        observed["freeze"]["excluded_writers"] = ["scheduler"]
    elif variant == "replica_missing":
        observed["replicas"].pop()
    else:
        observed["replicas"] = []
    with pytest.raises(ValueError):
        decode(canonical_json(data).encode())
