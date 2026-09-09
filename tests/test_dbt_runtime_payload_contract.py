"""Versioned dbt payload identities must bind exact bytes, kind and ordering."""

from __future__ import annotations

import hashlib
from dataclasses import FrozenInstanceError, replace

import pytest

from dpone.contracts.dbt_runtime_payloads import (
    DBT_RUNTIME_WIRE_V1,
    DBT_RUNTIME_WIRE_V2,
    DbtRuntimePayloadReference,
    dbt_runtime_payload_reference,
    dbt_runtime_payload_trio,
    validate_dbt_runtime_payload_descriptor,
    validate_dbt_runtime_payload_trio,
)


def _digest(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _trio(wire: str = DBT_RUNTIME_WIRE_V2) -> tuple[str, str, str]:
    return dbt_runtime_payload_trio(
        workflow_id="orders_daily",
        project_sha256=_digest(b"project"),
        manifest_sha256=_digest(b"manifest"),
        selection_lock_payload=b'{"selection_sha256": "semantic-not-byte-digest"}\n',
        wire_contract=wire,
    )


def test_legacy_payload_ids_and_paths_remain_exact() -> None:
    ids = _trio(DBT_RUNTIME_WIRE_V1)
    assert ids == ("dbt_project", "dbt_manifest", "dbt_selection_orders_daily")
    assert [dbt_runtime_payload_reference(item, wire_contract=DBT_RUNTIME_WIRE_V1).path for item in ids] == [
        "runtime/dbt/project.tar.gz",
        "runtime/dbt/manifest.json",
        "runtime/dbt/orders_daily.selection-lock.json",
    ]
    validate_dbt_runtime_payload_trio(ids, wire_contract=DBT_RUNTIME_WIRE_V1)


@pytest.mark.parametrize(
    ("position", "payload", "prefix", "kind", "filename", "media_type"),
    [
        (
            0,
            b"project",
            "dbt_project",
            "dbt_project_bundle",
            "project.tar.gz",
            "application/vnd.dpone.dbt-project-bundle+gzip",
        ),
        (1, b"manifest", "dbt_manifest", "dbt_manifest", "manifest.json", "application/vnd.dbt.manifest+json"),
        (
            2,
            b'{"selection_sha256": "semantic-not-byte-digest"}\n',
            "dbt_selection",
            "dbt_selection_lock",
            "selection-lock.json",
            "application/vnd.dpone.dbt-selection-lock+json",
        ),
    ],
)
def test_content_addressed_reference_binds_exact_bytes(
    position: int,
    payload: bytes,
    prefix: str,
    kind: str,
    filename: str,
    media_type: str,
) -> None:
    digest = _digest(payload)
    item_id = _trio()[position]
    assert item_id == f"{prefix}_sha256_{digest[7:]}"
    reference = dbt_runtime_payload_reference(item_id, wire_contract=DBT_RUNTIME_WIRE_V2)
    assert reference.path == f"runtime/dbt/objects/{digest[7:]}/{filename}"
    assert reference.kind == kind
    assert reference.sha256 == digest
    assert reference.descriptor(payload) == {
        "id": item_id,
        "kind": kind,
        "path": reference.path,
        "sha256": digest,
        "bytes": len(payload),
        "media_type": media_type,
    }
    with pytest.raises(ValueError):
        reference.descriptor(payload + b"tampered")
    with pytest.raises(FrozenInstanceError):
        reference.path = "other"  # type: ignore[misc]


@pytest.mark.parametrize("wire", [DBT_RUNTIME_WIRE_V1, DBT_RUNTIME_WIRE_V2])
def test_trio_rejects_reordered_missing_duplicate_or_extra_payloads(wire: str) -> None:
    ids = _trio(wire)
    validate_dbt_runtime_payload_trio(ids, wire_contract=wire)
    for invalid in (ids[::-1], ids[:2], (ids[0], ids[0], ids[2]), (*ids, ids[0]), ()):
        with pytest.raises(ValueError):
            validate_dbt_runtime_payload_trio(invalid, wire_contract=wire)


@pytest.mark.parametrize("wire", [DBT_RUNTIME_WIRE_V1, DBT_RUNTIME_WIRE_V2])
def test_wire_cannot_be_guessed_from_a_mixed_trio(wire: str) -> None:
    other = DBT_RUNTIME_WIRE_V1 if wire == DBT_RUNTIME_WIRE_V2 else DBT_RUNTIME_WIRE_V2
    for position in range(3):
        ids = list(_trio(wire))
        ids[position] = _trio(other)[position]
        with pytest.raises(ValueError):
            validate_dbt_runtime_payload_trio(tuple(ids), wire_contract=wire)


@pytest.mark.parametrize(
    "item_id",
    [
        "dbt_project",
        "dbt_project_sha256_" + "A" * 64,
        "dbt_project_sha256_" + "a" * 63,
        "dbt_project_sha256_" + "a" * 65,
        "dbt_project_sha256_" + "g" * 64,
        "dbt_project_sha256_" + "a" * 64 + "\n",
        "../project",
        "dbt_selection_../../other",
        "dbt_unknown_sha256_" + "a" * 64,
    ],
)
def test_v2_rejects_noncanonical_or_unsafe_ids(item_id: str) -> None:
    with pytest.raises(ValueError):
        dbt_runtime_payload_reference(item_id, wire_contract=DBT_RUNTIME_WIRE_V2)


def test_v2_selection_id_cannot_be_a_valid_legacy_workflow() -> None:
    item_id = "dbt_selection_sha256_" + "a" * 64
    with pytest.raises(ValueError):
        dbt_runtime_payload_reference(item_id, wire_contract=DBT_RUNTIME_WIRE_V1)
    modern = dbt_runtime_payload_reference(item_id, wire_contract=DBT_RUNTIME_WIRE_V2)
    assert modern.sha256 == "sha256:" + "a" * 64


@pytest.mark.parametrize("workflow", ["A", "orders.daily", "orders-daily", "a" * 65, "_orders"])
@pytest.mark.parametrize("wire", [DBT_RUNTIME_WIRE_V1, DBT_RUNTIME_WIRE_V2])
def test_workflow_validation_reuses_execution_contract(workflow: str, wire: str) -> None:
    with pytest.raises(ValueError):
        dbt_runtime_payload_trio(
            workflow_id=workflow,
            project_sha256=_digest(b"project"),
            manifest_sha256=_digest(b"manifest"),
            selection_lock_payload=b"{}",
            wire_contract=wire,
        )
    with pytest.raises(ValueError):
        dbt_runtime_payload_reference("dbt_selection_" + workflow, wire_contract=DBT_RUNTIME_WIRE_V1)


def test_canonical_64_character_workflow_boundary() -> None:
    ids = dbt_runtime_payload_trio(
        workflow_id="a" * 64,
        project_sha256=_digest(b"project"),
        manifest_sha256=_digest(b"manifest"),
        selection_lock_payload=b"{}",
        wire_contract=DBT_RUNTIME_WIRE_V1,
    )
    validate_dbt_runtime_payload_trio(ids, wire_contract=DBT_RUNTIME_WIRE_V1)


def test_direct_reference_cannot_bypass_descriptor_invariants() -> None:
    with pytest.raises(ValueError):
        DbtRuntimePayloadReference(
            id=_trio()[0],
            kind="dbt_manifest",
            path="../untrusted",
            media_type="text/plain",
        ).descriptor(b"arbitrary")


@pytest.mark.parametrize(
    "change",
    [
        {"kind": "dbt_manifest"},
        {"path": "../untrusted"},
        {"media_type": "text/plain"},
        {"sha256": None},
        {"sha256": "sha256:" + "a" * 64},
        {"wire_contract": DBT_RUNTIME_WIRE_V1},
    ],
)
def test_content_reference_rejects_every_forged_descriptor_field(change: dict[str, object]) -> None:
    reference = dbt_runtime_payload_reference(_trio()[0], wire_contract=DBT_RUNTIME_WIRE_V2)
    with pytest.raises(ValueError):
        replace(reference, **change)


def test_legacy_descriptor_preserves_existing_byte_identity() -> None:
    reference = dbt_runtime_payload_reference("dbt_project", wire_contract=DBT_RUNTIME_WIRE_V1)
    assert reference.descriptor(b"project") == {
        "id": "dbt_project",
        "kind": "dbt_project_bundle",
        "path": "runtime/dbt/project.tar.gz",
        "sha256": _digest(b"project"),
        "bytes": 7,
        "media_type": "application/vnd.dpone.dbt-project-bundle+gzip",
    }


@pytest.mark.parametrize("wire", [DBT_RUNTIME_WIRE_V1, DBT_RUNTIME_WIRE_V2])
def test_descriptor_parser_preserves_canonical_reference(wire: str) -> None:
    reference = dbt_runtime_payload_reference(_trio(wire)[0], wire_contract=wire)
    assert validate_dbt_runtime_payload_descriptor(reference.descriptor(b"project"), wire_contract=wire) == reference


@pytest.mark.parametrize(
    "change",
    [
        {"path": "runtime/dbt/wrong.tar.gz"},
        {"kind": "dbt_manifest"},
        {"media_type": "application/octet-stream"},
        {"sha256": _digest(b"wrong")},
        {"bytes": True},
        {"bytes": 0},
        {"bytes": -1},
        {"bytes": 256 * 1024 * 1024 + 1},
        {"bytes": 1.0},
        {"unexpected": "field"},
    ],
)
def test_received_descriptor_cannot_forge_reference_or_size(change: dict[str, object]) -> None:
    reference = dbt_runtime_payload_reference(_trio()[0], wire_contract=DBT_RUNTIME_WIRE_V2)
    with pytest.raises(ValueError):
        validate_dbt_runtime_payload_descriptor(
            {**reference.descriptor(b"project"), **change}, wire_contract=DBT_RUNTIME_WIRE_V2
        )


@pytest.mark.parametrize("wire", ["", "v2", "dpone.dbt-airflow-self-service.v3"])
def test_unsupported_wire_is_rejected_even_for_a_known_id(wire: str) -> None:
    with pytest.raises(ValueError):
        dbt_runtime_payload_reference("dbt_project", wire_contract=wire)
    with pytest.raises(ValueError):
        _trio(wire)


@pytest.mark.parametrize("digest", ["", "a" * 64, "sha256:" + "A" * 64])
def test_trio_builder_rejects_noncanonical_source_digest(digest: str) -> None:
    with pytest.raises(ValueError):
        dbt_runtime_payload_trio(
            workflow_id="orders",
            project_sha256=digest,
            manifest_sha256=_digest(b"manifest"),
            selection_lock_payload=b"{}",
            wire_contract=DBT_RUNTIME_WIRE_V2,
        )


@pytest.mark.parametrize("wire", [DBT_RUNTIME_WIRE_V1, DBT_RUNTIME_WIRE_V2])
def test_trio_builder_rejects_unsafe_workflow_and_empty_selection(wire: str) -> None:
    for workflow, selection in (("../orders", b"{}"), ("orders", b""), ("", b"{}")):
        with pytest.raises(ValueError):
            dbt_runtime_payload_trio(
                workflow_id=workflow,
                project_sha256=_digest(b"project"),
                manifest_sha256=_digest(b"manifest"),
                selection_lock_payload=selection,
                wire_contract=wire,
            )
