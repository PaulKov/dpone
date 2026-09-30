"""Canonical raw snapshot identity, marker, and EOF recovery tests."""

import hashlib
import json
from dataclasses import FrozenInstanceError, replace

import pytest

import dpone.contracts.clickhouse_raw_snapshot_bounds as snapshot_bounds
from dpone.contracts.clickhouse_raw_snapshot import (
    ClickHouseRawSnapshotProfileV1,
    ClickHouseRawSourceEofV1,
    raw_snapshot_extension,
    raw_source_query_binding,
    raw_source_query_binding_version,
    restore_raw_snapshot_extension,
)

_A = "a" * 64
_B = "b" * 64
_C = "c" * 64


def _profile() -> ClickHouseRawSnapshotProfileV1:
    return ClickHouseRawSnapshotProfileV1(
        engine_signature="ReplacingMergeTree(version, deleted)",
        relation_uuid="uuid-1",
        database_engine="Atomic",
        ordered_schema=(("id", "UInt64", ""), ("version", "UInt64", "DEFAULT 0")),
        partition_key_sha256=_A,
        sorting_key_sha256=_B,
        primary_key_sha256=_C,
        window=("event_day", "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z"),
        read_settings=(
            ("final", 0),
            ("use_query_cache", 0),
            ("apply_mutations_on_fly", 0),
            ("apply_patch_parts", 1),
            ("apply_deleted_mask", 1),
            ("max_parallel_replicas", 1),
            ("skip_unavailable_shards", 0),
            ("read_overflow_mode", "throw"),
            ("result_overflow_mode", "throw"),
            ("timeout_overflow_mode", "throw"),
        ),
        server_revision="2506001",
        endpoint_authority_sha256=_A,
        replica_scope="single_server",
        replica_identity_sha256=None,
        principal_authority_sha256=_B,
        row_policy_sha256=_C,
        policy_filtered=False,
        query_shape_sha256=_A,
        projection_sha256=_B,
        typed_parameters_sha256=_C,
    )


def _eof() -> ClickHouseRawSourceEofV1:
    return ClickHouseRawSourceEofV1(
        vendor_query_id_sha256=_A,
        rows=2,
        touched_part_coverage_sha256=_B,
        before_physical_profile_sha256=_C,
        after_physical_profile_sha256=_C,
        endpoint_authority_agreement=True,
    )


def _canonical_digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()


def test_profile_and_eof_round_trip_canonical_preimages_and_are_frozen() -> None:
    for value in (_profile(), _eof()):
        document = value.document()
        assert type(value).from_document(json.loads(json.dumps(document))) == value
        assert value.sha256 == _canonical_digest(document)
        with pytest.raises(FrozenInstanceError):
            value.sha256 = _A  # type: ignore[misc]


@pytest.mark.parametrize("field", ["partition_key_sha256", "endpoint_authority_sha256", "typed_parameters_sha256"])
def test_profile_rejects_noncanonical_digest(field: str) -> None:
    document = _profile().document()
    document[field] = _A.upper()
    with pytest.raises(ValueError):
        ClickHouseRawSnapshotProfileV1.from_document(document)


def test_profile_rejects_duplicate_settings_and_unbound_connected_replica() -> None:
    document = _profile().document()
    document["read_settings"] = [["final", 0], ["final", 1]]
    with pytest.raises(ValueError):
        ClickHouseRawSnapshotProfileV1.from_document(document)
    document = _profile().document()
    document["read_settings"] = [["final", False]]
    with pytest.raises(ValueError):
        ClickHouseRawSnapshotProfileV1.from_document(document)
    document = _profile().document()
    document["replica_scope"] = "connected_replica"
    with pytest.raises(ValueError):
        ClickHouseRawSnapshotProfileV1.from_document(document)


@pytest.mark.parametrize(
    "settings",
    [
        (("final", 1),),
        (("final", 0),),
        (("password", "secret"),),
    ],
)
def test_raw_profile_rejects_missing_overridden_or_sensitive_settings(
    settings: tuple[tuple[str, str | int], ...],
) -> None:
    profile = _profile()
    if settings[0][0] == "final" and settings[0][1] == 1:
        candidate = (("final", 1), *profile.read_settings[1:])
    elif settings[0][0] == "final":
        candidate = profile.read_settings[:1]
    else:
        candidate = (*profile.read_settings, *settings)
    with pytest.raises(ValueError):
        replace(profile, read_settings=candidate)


@pytest.mark.parametrize("field", ["vendor_query_id_sha256", "touched_part_coverage_sha256"])
def test_eof_rejects_noncanonical_digest(field: str) -> None:
    document = _eof().document()
    document[field] = "0" * 63
    with pytest.raises(ValueError):
        ClickHouseRawSourceEofV1.from_document(document)


@pytest.mark.parametrize(
    "change", [lambda d: d.pop("engine_signature"), lambda d: d.update(extra="x"), lambda d: d.update(kind="unknown")]
)
def test_profile_rejects_missing_extra_and_wrong_kind(change) -> None:  # type: ignore[no-untyped-def]
    document = _profile().document()
    change(document)
    with pytest.raises(ValueError):
        ClickHouseRawSnapshotProfileV1.from_document(document)


@pytest.mark.parametrize(
    "change",
    [
        lambda d: d.pop("rows"),
        lambda d: d.update(extra="x"),
        lambda d: d.update(rows=True),
        lambda d: d.update(rows=-1),
    ],
)
def test_eof_rejects_missing_extra_and_invalid_rows(change) -> None:  # type: ignore[no-untyped-def]
    document = _eof().document()
    change(document)
    with pytest.raises(ValueError):
        ClickHouseRawSourceEofV1.from_document(document)


def test_binding_has_exact_domain_separated_preimage_and_version() -> None:
    profile = _profile()
    expected = "clickhouse.raw-query.v1:" + _canonical_digest(
        {
            "kind": "dpone.clickhouse-raw-query-binding.v1",
            "legacy_source_binding_sha256": _A,
            "source_snapshot_profile_sha256": profile.sha256,
        }
    )
    assert raw_source_query_binding(_A, profile) == expected
    assert raw_source_query_binding_version(expected) == 1
    assert raw_source_query_binding_version(_A) is None
    with pytest.raises(ValueError):
        raw_source_query_binding_version("clickhouse.raw-query.v2:" + _A)


def test_extension_round_trip_and_rejects_binding_count_and_digest_tampering() -> None:
    profile, eof = _profile(), _eof()
    binding = raw_source_query_binding(_A, profile)
    extension = raw_snapshot_extension(binding, profile, eof)
    assert set(extension) == {
        "kind",
        "mode",
        "source_query_binding",
        "profile",
        "source_snapshot_profile_sha256",
        "source_eof",
        "source_eof_descriptor_sha256",
    }
    assert restore_raw_snapshot_extension(extension, binding=binding, completed_rows=2) == (profile, eof)
    with pytest.raises(ValueError):
        restore_raw_snapshot_extension(extension, binding=binding, completed_rows=3)
    for name, value in (("source_query_binding", "wrong"), ("source_eof_descriptor_sha256", _A), ("kind", "v2")):
        changed = {**extension, name: value}
        with pytest.raises(ValueError):
            restore_raw_snapshot_extension(changed, binding=binding, completed_rows=2)
    with pytest.raises(ValueError):
        restore_raw_snapshot_extension({**extension, "extra": True}, binding=binding, completed_rows=2)
    missing = dict(extension)
    missing.pop("source_eof")
    with pytest.raises(ValueError):
        restore_raw_snapshot_extension(missing, binding=binding, completed_rows=2)


def test_extension_rejects_tampered_preimage_and_oversized_preimage() -> None:
    profile, eof = _profile(), _eof()
    binding = raw_source_query_binding(_A, profile)
    extension = raw_snapshot_extension(binding, profile, eof)
    changed = {**extension, "profile": {**profile.document(), "engine_signature": "other"}}
    with pytest.raises(ValueError):
        restore_raw_snapshot_extension(changed, binding=binding, completed_rows=2)
    with pytest.raises(ValueError):
        replace(profile, engine_signature="x" * 100_000)


def test_profile_preimage_accepts_exact_byte_cap_and_rejects_next_byte() -> None:
    profile = _profile()
    original = len(json.dumps(profile.document(), sort_keys=True, separators=(",", ":")).encode())
    allowed_length = 65_536 - original + len(profile.engine_signature)
    assert len(replace(profile, engine_signature="x" * allowed_length).document()["engine_signature"]) == allowed_length
    with pytest.raises(ValueError):
        replace(profile, engine_signature="x" * (allowed_length + 1))


def test_oversized_nested_extension_is_rejected_before_json_materialization(monkeypatch: pytest.MonkeyPatch) -> None:
    profile, eof = _profile(), _eof()
    binding = raw_source_query_binding(_A, profile)
    extension = raw_snapshot_extension(binding, profile, eof)
    profile_doc = profile.document()
    profile_doc["ordered_schema"] = [["id", "UInt64", ""]] * 100_000
    extension["profile"] = profile_doc

    def serialization_must_not_run(_value: object) -> bytes:
        raise AssertionError("oversized input reached JSON materialization")

    monkeypatch.setattr(snapshot_bounds, "canonical_json_bytes", serialization_must_not_run)
    with pytest.raises(ValueError):
        restore_raw_snapshot_extension(extension, binding=binding, completed_rows=2)


def test_deeply_nested_extension_is_rejected_before_json_materialization(monkeypatch: pytest.MonkeyPatch) -> None:
    profile, eof = _profile(), _eof()
    binding = raw_source_query_binding(_A, profile)
    extension = raw_snapshot_extension(binding, profile, eof)
    nested: object = "x"
    for _ in range(20):
        nested = [nested]
    extension["source_eof"] = nested

    def serialization_must_not_run(_value: object) -> bytes:
        raise AssertionError("deep input reached JSON materialization")

    monkeypatch.setattr(snapshot_bounds, "canonical_json_bytes", serialization_must_not_run)
    with pytest.raises(ValueError):
        restore_raw_snapshot_extension(extension, binding=binding, completed_rows=2)
