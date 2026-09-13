"""Exclusive retained preplan originals are separately rooted and externally pinned."""

import os
from dataclasses import asdict, replace
from hashlib import sha256
from uuid import UUID

import pytest

from dpone.contracts.composition_mssql_binding import stable_operation_document
from dpone.contracts.composition_persistence import encode_attempt_identity
from dpone.contracts.dbt_relation_writes import transfer_relation_write
from dpone.contracts.source_physical_identity import SourcePhysicalIdentity
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.runtime.composition_transfer_preplan_store import (
    CompositionTransferPreplanStore,
    decode_transfer_preplan,
    projection_provenance,
)
from dpone.runtime.etl.mssql_schema_preplan_codec import encode_mssql_schema_preplan
from dpone.runtime.sources.strategies.postgres.postgres_schema_metadata import PostgresFetchedSchema
from dpone.runtime.state.mssql_target_identity_models import MssqlPhysicalTargetIdentity
from dpone.type_system.source_sink.provenance import SourceColumnProvenance
from tests.composition_mssql_gate_helpers import attempt
from tests.test_mssql_retained_preplan_codec import preplan
from tests.test_mssql_schema_preplan import _admission


def originals():
    selected = attempt()
    manifest = {
        "source": {
            "type": "postgres",
            "connection_ref": "source",
            "table": {"database": "source", "schema": "public", "name": "events"},
        },
        "sink": {
            "type": "mssql",
            "connection_ref": "target",
            "table": {"database": "DWH", "schema": "dbo", "name": "events"},
            "strategy": {"mode": "full_refresh"},
        },
    }
    write = transfer_relation_write(
        project_path=selected.constituent_id,
        workflow_id="workflow",
        workload_id=selected.workload_id,
        manifest=manifest,
    )
    physical = MssqlPhysicalTargetIdentity(
        "server",
        "machine",
        "instance",
        "replica",
        "DWH",
        "2026-01-01",
        UUID("12345678-1234-4234-8234-123456789012"),
        "dbo",
        "events",
        10,
    )
    operation = _admission().operation
    operation = replace(
        operation,
        attempt=replace(operation.attempt, request=replace(operation.attempt.request, target_identity=physical.digest)),
    )
    plan = preplan()
    plan = replace(plan, target_mutation_plan=replace(plan.target_mutation_plan, target_identity=physical.digest))
    identity = SourcePhysicalIdentity(
        "postgres",
        "cluster",
        "source",
        "reader",
        "reader",
        topology_role="primary",
        version=2,
        authority_sha256="sha256:" + "a" * 64,
        timeline_id=1,
        database_oid=1,
        effective_principal_oid=2,
        session_principal_oid=2,
        schema="public",
        schema_oid=3,
        relation="events",
        relation_oid=4,
    )
    projection = PostgresFetchedSchema(
        (("id", "bigint"),), (("id", "bigint"),), (SourceColumnProvenance("id", "bigint", True),)
    )
    return selected, manifest, write, physical, operation, plan, identity, projection


def envelope():
    selected, manifest, write, physical, operation, plan, identity, projection = originals()
    document = canonical_json_bytes(
        {
            "schema": "dpone.composition-transfer-preplan.v1",
            "attempt_original": strict_json_object(encode_attempt_identity(selected)),
            "operation_original": strict_json_object(stable_operation_document(operation)),
            "write": asdict(write),
            "plan_sha256": selected.plan_sha256,
            "manifest_sha256": "sha256:" + sha256(canonical_json_bytes(manifest)).hexdigest(),
            "preplan": strict_json_object(encode_mssql_schema_preplan(plan)),
            "source_identity": identity.to_dict(),
            "source_projection": asdict(projection),
            "source_provenance_sha256": projection_provenance(projection),
            "target_identity": {**asdict(physical), "binding_id": str(physical.binding_id)},
            "route_fingerprint": operation.attempt.request.route_fingerprint.hex(),
            "connection_observation": {"observed": "fixture"},
        }
    )
    return selected, document


def test_private_store_requires_owned_directory(tmp_path):
    tmp_path.chmod(0o755)
    with pytest.raises(ValueError):
        CompositionTransferPreplanStore(tmp_path)


def test_capture_is_exclusive_and_does_not_precreate_payload_attempt(tmp_path):
    tmp_path.chmod(0o700)
    selected, document = envelope()
    store = CompositionTransferPreplanStore(tmp_path)
    reference = store.capture(selected, document)
    assert reference.document == document
    assert reference.preplan == originals()[5]
    assert store.load(selected, reference.document_sha256) == reference
    assert not (tmp_path / selected.attempt_sha256[7:]).exists()
    with pytest.raises(FileExistsError):
        store.capture(selected, document)


@pytest.mark.parametrize("damage", ["digest", "contents", "symlink", "hardlink", "mode"])
def test_recovery_rejects_changed_or_nonprivate_original(tmp_path, damage):
    tmp_path.chmod(0o700)
    selected, document = envelope()
    store = CompositionTransferPreplanStore(tmp_path)
    reference = store.capture(selected, document)
    path = tmp_path / "preplans" / (selected.attempt_sha256[7:] + ".json")
    expected = reference.document_sha256
    if damage == "digest":
        expected = b"x" * 32
    elif damage == "contents":
        path.write_bytes(document + b" ")
    elif damage == "symlink":
        path.rename(path.with_suffix(".saved"))
        path.symlink_to(path.with_suffix(".saved"))
    elif damage == "hardlink":
        os.link(path, path.with_suffix(".linked"))
    else:
        path.chmod(0o644)
    with pytest.raises((ValueError, OSError)):
        store.load(selected, expected)


def test_foreign_attempt_or_open_envelope_rejected_before_persistence(tmp_path):
    tmp_path.chmod(0o700)
    selected, document = envelope()
    store = CompositionTransferPreplanStore(tmp_path)
    with pytest.raises(ValueError):
        store.capture(replace(selected, try_number=selected.try_number + 1), document)
    body = strict_json_object(document)
    body["authorized"] = True
    with pytest.raises(ValueError):
        store.capture(selected, canonical_json_bytes(body))
    assert list((tmp_path / "preplans").iterdir()) == []


@pytest.mark.parametrize("path", ["target_identity", "route_fingerprint", "source_provenance_sha256"])
def test_semantic_identity_mismatch_with_fresh_outer_hash_is_rejected(path):
    selected, document = envelope()
    body = strict_json_object(document)
    if path == "target_identity":
        body[path]["binding_id"] = "12345678-1234-4234-8234-123456789013"
    else:
        body[path] = "f" * 64
    changed = canonical_json_bytes(body)
    with pytest.raises(ValueError):
        decode_transfer_preplan(changed, sha256(changed).digest(), attempt=selected)
