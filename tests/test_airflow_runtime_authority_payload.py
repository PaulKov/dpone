"""Fail-closed immutable runtime-authority payload contracts."""

from __future__ import annotations

import base64
import hashlib
import os
from pathlib import Path

import pytest

from dpone.readiness.airflow_runtime_authority_input import (
    immutable_runtime_authority_payload_from_file,
)
from dpone.runtime.init_fetch_contract import InitFetchError
from dpone.runtime.runtime_init_fetch_payload import (
    MAX_RUNTIME_AUTHORITY_PAYLOAD_BYTES,
    ImmutableRuntimeAuthorityPayload,
    materialize_runtime_authority_payload,
    verify_materialized_runtime_authority_payload,
)


def _payload(raw: bytes = b'{"mode":"synthetic"}\n') -> ImmutableRuntimeAuthorityPayload:
    return ImmutableRuntimeAuthorityPayload(
        mode="immutable_payload",
        encoding="base64",
        payload_b64=base64.b64encode(raw).decode("ascii"),
        bytes=len(raw),
        sha256="sha256:" + hashlib.sha256(raw).hexdigest(),
    )


def test_payload_validates_exact_opaque_bytes() -> None:
    raw = b"\x00\xffsynthetic\n"
    payload = _payload(raw)

    assert payload.decode() == raw
    assert payload.to_dict()["bytes"] == len(raw)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("mode", "inline"),
        ("encoding", "urlsafe-base64"),
        ("payload_b64", "***"),
        ("bytes", 0),
        ("sha256", "sha256:" + "A" * 64),
    ],
)
def test_payload_rejects_malformed_fields(field: str, value: object) -> None:
    values = _payload().to_dict()
    values[field] = value

    with pytest.raises(ValueError, match="runtime authority payload"):
        ImmutableRuntimeAuthorityPayload.from_mapping(values)


def test_payload_rejects_noncanonical_base64_size_digest_and_limit() -> None:
    valid = _payload(b"synthetic!")
    mutations = [
        {**valid.to_dict(), "payload_b64": valid.payload_b64.rstrip("=")},
        {**valid.to_dict(), "bytes": valid.bytes + 1},
        {**valid.to_dict(), "sha256": "sha256:" + "0" * 64},
        _payload(b"x" * MAX_RUNTIME_AUTHORITY_PAYLOAD_BYTES).to_dict()
        | {
            "payload_b64": base64.b64encode(b"x" * (MAX_RUNTIME_AUTHORITY_PAYLOAD_BYTES + 1)).decode("ascii"),
            "bytes": MAX_RUNTIME_AUTHORITY_PAYLOAD_BYTES + 1,
        },
    ]

    for mutation in mutations:
        with pytest.raises(ValueError, match="runtime authority payload"):
            ImmutableRuntimeAuthorityPayload.from_mapping(mutation)


def test_init_materializes_atomically_and_base_verifies_without_following_symlink(tmp_path: Path) -> None:
    payload = _payload()
    target = tmp_path / "authority"

    materialize_runtime_authority_payload(payload, target)

    assert target.read_bytes() == payload.decode()
    assert target.stat().st_mode & 0o777 == 0o400
    verify_materialized_runtime_authority_payload(payload, target)
    target.unlink()
    target.symlink_to(tmp_path / "missing")
    with pytest.raises(InitFetchError) as exc_info:
        verify_materialized_runtime_authority_payload(payload, target)
    assert exc_info.value.code == "DPONE_RUNTIME_AUTHORITY_PAYLOAD_INVALID"


def test_materializer_rejects_tamper_without_echoing_payload(tmp_path: Path) -> None:
    payload = _payload(b"synthetic-private-looking-value")
    target = tmp_path / "authority"
    materialize_runtime_authority_payload(payload, target)
    os.chmod(target, 0o600)
    target.write_bytes(b"tampered")

    with pytest.raises(InitFetchError) as exc_info:
        verify_materialized_runtime_authority_payload(payload, target)

    assert exc_info.value.code == "DPONE_RUNTIME_AUTHORITY_PAYLOAD_INVALID"
    assert "synthetic-private-looking-value" not in str(exc_info.value)


def test_build_reader_rejects_symlink_and_limit_plus_one(tmp_path: Path) -> None:
    source = tmp_path / "source"
    link = tmp_path / "link"
    source.write_bytes(b"synthetic")
    link.symlink_to(source)
    digest = "sha256:" + hashlib.sha256(b"synthetic").hexdigest()

    with pytest.raises(ValueError, match="regular non-symlink"):
        immutable_runtime_authority_payload_from_file(link, expected_sha256=digest)

    source.write_bytes(b"x" * (MAX_RUNTIME_AUTHORITY_PAYLOAD_BYTES + 1))
    oversized_digest = "sha256:" + hashlib.sha256(source.read_bytes()).hexdigest()
    with pytest.raises(ValueError, match="outside the limit"):
        immutable_runtime_authority_payload_from_file(source, expected_sha256=oversized_digest)
