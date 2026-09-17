"""Offline fixture identity regressions; no SQL or storage qualification claims."""

from dataclasses import replace
from hashlib import sha256
from uuid import UUID

import pytest

from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_originals import (
    NativeOriginalBinding,
    decode_native_original_binding,
    encode_native_original_binding,
)
from tests.support import dbt_mssql_physical_source_authority as source_module
from tests.support.dbt_mssql_physical_catalog_v2_live import ArchiveAuthority, TimedSourceFixture
from tests.test_native_generation_admission import generation_request


def binding(authority, generation, *, requested_bytes=100):
    base = generation_request()
    request = generation_request(
        subject=replace(base.subject, generation_id=generation), requested_bytes=requested_bytes
    )
    payload = request.request_bytes()
    reference = authority.retain("reservation", payload)
    assert reference.locator == f"fixture/reservation/{generation}"
    with authority.reservation_producer():
        stored = source_module.ArtifactObjectRef(
            "fixture/reservation",
            "fixture-owner-v1",
            len(payload),
            reference.sha256,
            "fixture-memory-only",
            "2099-01-01T00:00:00Z",
        )
    value = NativeOriginalBinding(
        request.subject,
        "generation_stored_file_v1",
        OriginalRef("fixture/storage-policy", "sha256:" + "b" * 64),
        stored,
        reference.sha256,
        reference.locator,
    )
    assert value.object_ref.key == reference.locator
    assert value.object_ref.size_bytes == len(authority.payloads[reference.locator])
    assert value.object_ref.sha256 == "sha256:" + sha256(authority.payloads[reference.locator]).hexdigest()
    assert authority.get("reservation") == reference
    assert decode_native_original_binding(encode_native_original_binding(value)) == value
    return value


def test_successive_generations_have_coherent_distinct_complete_bindings(tmp_path, monkeypatch):
    first = binding(ArchiveAuthority(tmp_path / "a", monkeypatch, 1), UUID(int=1))
    second = binding(ArchiveAuthority(tmp_path / "b", monkeypatch, 2), UUID(int=2))
    replay = binding(ArchiveAuthority(tmp_path / "replay", monkeypatch, 1), UUID(int=1))
    assert first == replay
    assert encode_native_original_binding(first) == encode_native_original_binding(replay)
    assert first.subject.generation_id != second.subject.generation_id
    assert first.locator != second.locator and first.object_ref.key != second.object_ref.key
    assert first.payload_sha256 != second.payload_sha256
    assert first.storage_authority == second.storage_authority
    assert first.kind == second.kind == "generation_stored_file_v1"
    assert first.object_ref.version == second.object_ref.version == "fixture-owner-v1"
    assert sha256(first.locator.encode()).digest() != sha256(second.locator.encode()).digest()


def test_changed_bytes_for_same_generation_cannot_escape_immutable_locator(tmp_path, monkeypatch):
    original = binding(ArchiveAuthority(tmp_path / "original", monkeypatch, 1), UUID(int=1))
    changed = binding(ArchiveAuthority(tmp_path / "changed", monkeypatch, 1), UUID(int=1), requested_bytes=101)
    assert changed.locator == original.locator
    assert changed.object_ref.key == original.object_ref.key
    assert changed.object_ref.version == original.object_ref.version
    assert encode_native_original_binding(changed) != encode_native_original_binding(original)


def test_reservation_producer_restores_original_constructor_after_failure(tmp_path, monkeypatch):
    original = source_module.ArtifactObjectRef
    authority = ArchiveAuthority(tmp_path, monkeypatch, 1)
    with pytest.raises(RuntimeError, match="producer fault"), authority.reservation_producer():
        assert source_module.ArtifactObjectRef is not original
        raise RuntimeError("producer fault")
    assert source_module.ArtifactObjectRef is original


@pytest.mark.parametrize("fault", ["other-key", "size", "digest"])
def test_reservation_producer_rejects_claims_outside_retained_bytes(tmp_path, monkeypatch, fault):
    authority = ArchiveAuthority(tmp_path, monkeypatch, 1)
    request = generation_request()
    payload = request.request_bytes()
    reference = authority.retain("reservation", payload)
    with pytest.raises(ValueError), authority.reservation_producer():
        source_module.ArtifactObjectRef(
            "other-key" if fault == "other-key" else "fixture/reservation",
            "fixture-owner-v1",
            len(payload) + int(fault == "size"),
            "sha256:" + "0" * 64 if fault == "digest" else reference.sha256,
            "fixture-memory-only",
            "2099-01-01T00:00:00Z",
        )


@pytest.mark.parametrize("layout", ["same_database", "two_database"])
def test_timing_wrapper_reuses_reviewed_source_layout_without_install(layout):
    fixture = TimedSourceFixture(object(), "legacy-correctness-v1", layout=layout)
    assert fixture.layout == layout
    assert (fixture.databases["model"] == fixture.databases["control"]) == (layout == "same_database")
