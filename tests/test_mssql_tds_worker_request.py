"""Private worker job validation does not inspect files or access optional SDKs."""

from __future__ import annotations

import json
from dataclasses import replace
from hashlib import sha256
from pathlib import Path

import pytest

from dpone.app.mssql_tds_worker_request import TdsWorkerJob, decode_job, encode_job
from dpone.contracts.mssql_native_chunks import EncodedNativeFile, NativeBulkTransportPolicy
from dpone.contracts.mssql_tds_worker import TdsAttemptIdentity
from dpone.contracts.strict_json import canonical_json_bytes
from dpone.runtime.native_wire_mssql import build_mssql_bcp_native_contract


def job():
    policy = NativeBulkTransportPolicy("mssql_python", "rows", 64 << 20)
    identity = TdsAttemptIdentity(
        "target",
        "run",
        0,
        0,
        "a" * 64,
        sha256(canonical_json_bytes(policy.to_dict())).hexdigest(),
        "b" * 64,
        "c" * 64,
        "db",
        "test",
        "owned",
        "d" * 64,
    )
    file = EncodedNativeFile(Path("/private/retained.native"), 0, 10, 80, "c" * 64, "sha256:" + "e" * 64)
    wire = build_mssql_bcp_native_contract(schema=[("value", "bigint")], query="SELECT value")
    return TdsWorkerJob(identity, policy, file, wire, "Server=synthetic;Password=private-token", 16 << 20)


def test_roundtrip_canonical_and_hidden_repr():
    request = job()
    encoded = encode_job(request)
    assert decode_job(encoded) == request
    assert encode_job(decode_job(encoded)) == encoded
    assert "private-token" not in repr(request)
    assert "/private/" not in repr(request)


@pytest.mark.parametrize("body", [b"{}", b"[]", b'{"version":1,"version":1}', b"NaN", b"\xff", b"{}" * (1 << 20)])
def test_malformed_private_requests_are_sanitized(body):
    with pytest.raises(ValueError, match="^tds_worker_request_invalid$"):
        decode_job(body)


@pytest.mark.parametrize(
    "field,value",
    [("ordinal", 1), ("file_sha256", "f" * 64), ("rows", True), ("encoded_bytes", -1), ("path", "relative")],
)
def test_file_binding_and_strict_types(field, value):
    payload = json.loads(encode_job(job()))
    payload["file"][field] = value
    with pytest.raises(ValueError, match="^tds_worker_request_invalid$"):
        decode_job(json.dumps(payload).encode())


def test_changed_policy_cannot_reuse_identity():
    with pytest.raises(ValueError, match="^tds_worker_request_invalid$"):
        replace(job(), policy=replace(job().policy, batch_rows=1))


@pytest.mark.parametrize("section", ["identity", "policy", "file", "wire"])
@pytest.mark.parametrize("change", ["unknown", "missing"])
def test_nested_objects_are_closed_and_have_no_defaults(section, change):
    payload = json.loads(encode_job(job()))
    if change == "unknown":
        payload[section]["private-unknown"] = "private-token"
    else:
        del payload[section][next(iter(payload[section]))]
    with pytest.raises(ValueError, match="^tds_worker_request_invalid$"):
        decode_job(json.dumps(payload).encode())


@pytest.mark.parametrize(
    "section,field,value",
    [
        ("identity", "ordinal", True),
        ("identity", "attempt", 1.0),
        ("identity", "database", 123),
        ("policy", "batch_rows", 1.0),
        ("policy", "backend", False),
        ("policy", "input", []),
        ("file", "path", 123),
        ("file", "rows", "10"),
        ("file", "encoded_bytes", 2**63),
        ("file", "typed_digest", "x" * 257),
        ("file", "typed_digest", None),
        ("wire", "columns", []),
        ("wire", "warnings", "bad"),
        ("wire", "bcp_version", 123),
        ("wire", "source_system", True),
        ("wire", "query_hash", 123),
        ("wire", "blockers", ["private-token"]),
    ],
)
def test_strict_incoming_types_and_limits(section, field, value):
    payload = json.loads(encode_job(job()))
    payload[section][field] = value
    with pytest.raises(ValueError, match="^tds_worker_request_invalid$"):
        decode_job(json.dumps(payload).encode())


@pytest.mark.parametrize(
    "field,value",
    [
        ("nullable", 1),
        ("prefix_width", False),
        ("fixed_length", 8.0),
        ("precision", "8"),
        ("scale", False),
        ("encoding", 42),
        ("name", "bad\nname"),
        ("name", "x" * 129),
        ("target_type", []),
        ("storage_type", None),
    ],
)
def test_column_fields_are_strict_before_contract_construction(field, value):
    payload = json.loads(encode_job(job()))
    payload["wire"]["columns"][0][field] = value
    with pytest.raises(ValueError, match="^tds_worker_request_invalid$"):
        decode_job(json.dumps(payload).encode())


@pytest.mark.parametrize("change", ["extra", "missing", "duplicate", "too_many", "unsupported"])
def test_column_closed_shape_and_layout_admission(change):
    payload = json.loads(encode_job(job()))
    columns = payload["wire"]["columns"]
    if change == "extra":
        columns[0]["plugin"] = "private-token"
    elif change == "missing":
        del columns[0]["scale"]
    elif change == "duplicate":
        columns.append(columns[0].copy())
    elif change == "too_many":
        payload["wire"]["columns"] = columns * 1025
    else:
        unsupported = build_mssql_bcp_native_contract(schema=[("value", "int")], query="SELECT value")
        payload["wire"] = unsupported.to_dict()
    with pytest.raises(ValueError, match="^tds_worker_request_invalid$"):
        decode_job(json.dumps(payload).encode())


@pytest.mark.parametrize("value", ["", None, 123, "x" * 16385, "has\0nul", "\ud800"])
def test_connection_strings_never_leak_in_failures(value):
    payload = json.loads(encode_job(job()))
    payload["connection_string"] = value
    with pytest.raises(ValueError, match="^tds_worker_request_invalid$") as caught:
        decode_job(json.dumps(payload).encode())
    assert caught.value.__suppress_context__


@pytest.mark.parametrize(
    "body",
    [
        b'{"version":NaN}',
        b'{"version":1e400}',
        b'{"version":Infinity}',
        b'{"version":1,"identity":{"x":1,"x":2}}',
        b'{"private-token":',
        b"{}\x00",
        b" " * ((1 << 20) + 1),
        "{}",
        bytearray(b"{}"),
    ],
)
def test_ambiguous_nonfinite_or_nonbytes_rejected(body):
    with pytest.raises(ValueError, match="^tds_worker_request_invalid$"):
        decode_job(body)


def test_oversize_body_rejected_before_json(monkeypatch):
    calls = []

    def forbidden(*args, **kwargs):
        calls.append(True)
        raise AssertionError("parser called for oversized input")

    monkeypatch.setattr("dpone.app.mssql_tds_worker_request.strict_json_object", forbidden)
    with pytest.raises(ValueError, match="^tds_worker_request_invalid$"):
        decode_job(b"x" * ((1 << 20) + 1))
    assert calls == []


def test_codec_has_no_file_or_vendor_effects(monkeypatch):
    import builtins

    original = builtins.__import__
    request = job()

    def guarded(name, *args, **kwargs):
        if name.split(".")[0] in {"mssql_python", "mssql_py_core", "pyarrow"}:
            raise AssertionError("optional SDK imported")
        return original(name, *args, **kwargs)

    def forbidden(*args, **kwargs):
        raise AssertionError("file inspected")

    monkeypatch.setattr(builtins, "__import__", guarded)
    monkeypatch.setattr(Path, "open", forbidden)
    monkeypatch.setattr(Path, "stat", forbidden)
    monkeypatch.setattr(Path, "resolve", forbidden)
    assert decode_job(encode_job(request)) == request


def test_arrow_policy_roundtrips_without_arrow():
    request = job()
    policy = replace(request.policy, input="arrow")
    identity = replace(request.identity, policy_sha256=sha256(canonical_json_bytes(policy.to_dict())).hexdigest())
    request = replace(request, policy=policy, identity=identity)
    assert decode_job(encode_job(request)) == request


def test_model_immutability():
    from dataclasses import FrozenInstanceError

    request = job()
    with pytest.raises(FrozenInstanceError):
        request.connection_string = "changed"


@pytest.mark.parametrize("limit", [None, True, 0, -1, 1.0, "10", 2**63])
def test_row_byte_bound_is_required_strict_and_never_inferred(limit):
    payload = json.loads(encode_job(job()))
    payload["max_row_bytes"] = limit
    with pytest.raises(ValueError, match="^tds_worker_request_invalid$"):
        decode_job(json.dumps(payload).encode())


def test_missing_row_bound_rejected():
    payload = json.loads(encode_job(job()))
    del payload["max_row_bytes"]
    with pytest.raises(ValueError, match="^tds_worker_request_invalid$"):
        decode_job(json.dumps(payload).encode())


@pytest.mark.parametrize("limit", [1, 2**63 - 1])
def test_row_bound_is_preserved_exactly(limit):
    request = replace(job(), max_row_bytes=limit)
    assert decode_job(encode_job(request)).max_row_bytes == limit
