"""Only valid bodies acquire byte hashes; RESULT retains exact original bytes."""

import json
from dataclasses import replace
from hashlib import sha256

import pytest

from dpone.contracts import mssql_sqlclient_evidence as module
from dpone.contracts.mssql_sqlclient_evidence_types import SqlClientEvidenceKind as Kind
from tests.mssql_sqlclient_evidence_fixtures import evidence_record


@pytest.mark.parametrize("kind", list(Kind))
def test_every_kind_exact_hash_and_attempt(kind):
    record = evidence_record(kind)
    receipt = record.receipt
    assert receipt.payload_sha256 == sha256(record.payload).hexdigest()
    assert receipt.relative_name == f"tds-sqlclient-{record.attempt_sha256}-{kind.value}-{receipt.payload_sha256}.json"
    assert receipt.byte_count == len(record.payload)
    with pytest.raises(ValueError):
        replace(record, attempt_sha256="1" * 64)


@pytest.mark.parametrize("kind", list(Kind))
def test_noncanonical_only_result(kind):
    record = evidence_record(kind)
    raw = json.dumps(json.loads(record.payload), indent=1).encode()
    if kind is Kind.RESULT:
        value = replace(record, payload=raw)
        assert value.payload == raw and value.receipt.payload_sha256 == sha256(raw).hexdigest()
        assert value.receipt != record.receipt
    else:
        with pytest.raises(ValueError):
            replace(record, payload=raw)


@pytest.mark.parametrize("kind", list(Kind))
def test_invalid_body_never_hashes_or_echoes(monkeypatch, kind):
    record = evidence_record(kind)
    called = []
    monkeypatch.setattr(module, "sha256", lambda *a: called.append(a))
    object.__setattr__(record, "payload", b'{"secret-canary":"secret-canary"}')
    with pytest.raises(ValueError, match=r"^mssql_native.sqlclient_evidence_invalid$"):
        record.receipt
    assert not called and "secret-canary" not in repr(record)


def test_result_context_required_only_for_result_and_revalidated():
    result = evidence_record(Kind.RESULT)
    with pytest.raises(ValueError):
        replace(result, result_context=None)
    with pytest.raises(ValueError):
        replace(evidence_record(Kind.LOCAL_EXIT), result_context=result.result_context)
    object.__setattr__(result.result_context.expected_input, "rows", True)
    with pytest.raises(ValueError):
        result.receipt


@pytest.mark.parametrize("mutation", ["extra", "duplicate", "missing", "trailing"])
@pytest.mark.parametrize("kind", list(Kind))
def test_closed_envelope_payload(kind, mutation):
    record = evidence_record(kind)
    raw = record.payload
    if mutation == "extra":
        data = json.loads(raw)
        data["unknown"] = 1
        raw = json.dumps(data).encode()
    elif mutation == "duplicate":
        data = json.loads(raw)
        key = next(iter(data))
        raw = raw[:-1] + b"," + json.dumps(key).encode() + b":" + json.dumps(data[key]).encode() + b"}"
    elif mutation == "missing":
        data = json.loads(raw)
        del data[next(iter(data))]
        raw = json.dumps(data).encode()
    else:
        raw += b"{}"
    with pytest.raises(ValueError):
        replace(record, payload=raw)
