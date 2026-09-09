"""Explicit source-snapshot dispatch preserves the published singleton wire."""

from __future__ import annotations

import json
from dataclasses import replace

import pytest

from dpone.contracts.airflow_deployment import canonical_fingerprint
from dpone.contracts.dbt_contract_validation import sha256_bytes
from dpone.contracts.dbt_runtime_payloads import DBT_RUNTIME_WIRE_V1, DBT_RUNTIME_WIRE_V2
from dpone.contracts.dbt_source_snapshot import LegacyDbtSourceSnapshot, read_dbt_source_snapshot
from tests.test_dbt_source_inventory import _inventory


def test_legacy_snapshot_is_byte_identity_compatible() -> None:
    project, manifest = sha256_bytes(b"project"), sha256_bytes(b"manifest")
    source = LegacyDbtSourceSnapshot(project_bundle_sha256=project, manifest_sha256=manifest)
    unsigned = {"schema": "dpone.dbt-source-snapshot.v1", "project_bundle_sha256": project, "manifest_sha256": manifest}
    assert source.to_dict() == {**unsigned, "snapshot_sha256": canonical_fingerprint(unsigned)}
    assert read_dbt_source_snapshot(json.dumps(source.to_dict()).encode(), wire_contract=DBT_RUNTIME_WIRE_V1) == source


def test_workspace_snapshot_uses_explicit_new_wire() -> None:
    source = _inventory()
    assert read_dbt_source_snapshot(json.dumps(source.to_dict()).encode(), wire_contract=DBT_RUNTIME_WIRE_V2) == source


@pytest.mark.parametrize("wire", [DBT_RUNTIME_WIRE_V1, DBT_RUNTIME_WIRE_V2])
def test_wire_snapshot_mismatch_rejected(wire: str) -> None:
    source = (
        _inventory() if wire == DBT_RUNTIME_WIRE_V1 else LegacyDbtSourceSnapshot(sha256_bytes(b"p"), sha256_bytes(b"m"))
    )
    with pytest.raises(ValueError):
        read_dbt_source_snapshot(json.dumps(source.to_dict()).encode(), wire_contract=wire)


@pytest.mark.parametrize("key", ["schema", "project_bundle_sha256", "manifest_sha256", "snapshot_sha256"])
def test_legacy_snapshot_rejects_missing_fields(key: str) -> None:
    value = LegacyDbtSourceSnapshot(sha256_bytes(b"p"), sha256_bytes(b"m")).to_dict()
    value.pop(key)
    with pytest.raises(ValueError):
        LegacyDbtSourceSnapshot.from_mapping(value)


def test_legacy_snapshot_rejects_extras_tampering_and_invalid_construction() -> None:
    source = LegacyDbtSourceSnapshot(sha256_bytes(b"p"), sha256_bytes(b"m"))
    for mutation in ({"unexpected": "x"}, {"snapshot_sha256": sha256_bytes(b"x")}, {"schema": "v1"}):
        with pytest.raises(ValueError):
            LegacyDbtSourceSnapshot.from_mapping({**source.to_dict(), **mutation})
    with pytest.raises(ValueError):
        replace(source, project_bundle_sha256="not-a-digest")


@pytest.mark.parametrize("payload", [b'{"schema":"v1","schema":"v1"}', b"{}", b"[]", b"\xff", b" " * (1024 * 1024 + 1)])
def test_snapshot_reader_rejects_invalid_or_oversized_bytes(payload: bytes) -> None:
    with pytest.raises(ValueError):
        read_dbt_source_snapshot(payload, wire_contract=DBT_RUNTIME_WIRE_V1)


def test_snapshot_reader_rejects_unknown_wire() -> None:
    with pytest.raises(ValueError):
        read_dbt_source_snapshot(b"{}", wire_contract="dpone.dbt-airflow-self-service.v3")
