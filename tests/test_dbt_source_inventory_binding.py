"""Metadata closure is necessary, but is not byte or live certification."""

from __future__ import annotations

from copy import deepcopy

import pytest

from dpone.contracts.airflow_deployment import release_id
from dpone.contracts.dbt_contract_validation import sha256_bytes
from dpone.contracts.dbt_release import dbt_selection_fingerprint
from dpone.contracts.dbt_runtime_payloads import DBT_RUNTIME_WIRE_V2, dbt_runtime_payload_reference
from dpone.contracts.dbt_source_inventory import DbtSourceInventory
from dpone.contracts.dbt_source_inventory_binding import validate_dbt_source_inventory_binding
from tests.test_dbt_source_inventory import _inventory


def _release(inventory: DbtSourceInventory) -> dict:
    payloads = {}
    packs, dags = [], []
    for project in inventory.projects:
        for workflow in project.workflows:
            for item_id in workflow.runtime_payload_ids:
                reference = dbt_runtime_payload_reference(item_id, wire_contract=DBT_RUNTIME_WIRE_V2)
                payloads[item_id] = {
                    "id": item_id,
                    "path": reference.path,
                    "kind": reference.kind,
                    "media_type": reference.media_type,
                    "sha256": reference.sha256,
                    "bytes": 1,
                }
            packs.append(
                {
                    "id": workflow.workload_id,
                    "path": f"packs/{workflow.workload_id}.airflow-pack.json",
                    "sha256": sha256_bytes(workflow.workload_id.encode()),
                    "bytes": 1,
                    "pack_fingerprint": sha256_bytes(b"pack"),
                    "runtime_payload_ids": list(workflow.runtime_payload_ids),
                }
            )
            dags.append(
                {
                    "id": workflow.dag_id,
                    "path": f"dags/{workflow.dag_id}.dag-spec.json",
                    "sha256": sha256_bytes(workflow.dag_id.encode()),
                    "bytes": 1,
                }
            )
    release = {
        "schema": "dpone.release-set.v2",
        "producer": {"dpone_version": "0.74.28", "wire_contract": DBT_RUNTIME_WIRE_V2},
        "selection_authority": "dbt_cli",
        "artifacts": {
            "workload_packs": packs,
            "dag_specs": dags,
            "runtime_payloads": list(payloads.values()),
            "canonical_schemas": [
                {
                    "id": "fixture",
                    "path": "schemas/dbt/fixture.schema.json",
                    "bytes": 1,
                    "sha256": sha256_bytes(b"s"),
                }
            ],
        },
        "provenance": {
            "source_snapshot_sha256": inventory.snapshot_sha256,
            "selection_fingerprints": [sha256_bytes(b"semantic selection")],
            "route_certifications": [
                {
                    "variant_id": "fixture",
                    "route_id": "fixture",
                    "transport": "fixture",
                    "schema_evolution": "fixture",
                    "airflow_runtime_mode": "fixture",
                    "snapshot_id": sha256_bytes(b"fixture"),
                    "support": "supported",
                    "certification_level": "production-certified",
                    "evidence_status": "PASS",
                    "evidence_refs": [sha256_bytes(b"fixture")],
                    "evidence_reason_codes": [],
                }
            ],
        },
    }
    return _seal(release)


def _seal(release: dict) -> dict:
    release["selection_fingerprint"] = dbt_selection_fingerprint(**release["provenance"])
    release["release_id"] = release_id(release)
    return release


def _validate(release: dict, inventory: DbtSourceInventory) -> None:
    validate_dbt_source_inventory_binding(release, inventory, expected_release_id=release["release_id"])


def test_complete_two_project_metadata_is_bound() -> None:
    inventory = _inventory()
    _validate(_release(inventory), inventory)


def test_complete_membership_validates_each_runtime_descriptor_only_once(monkeypatch):
    import dpone.contracts.dbt_runtime_release_binding as binding

    inventory = _inventory()
    release = _release(inventory)
    observed = []
    validate = binding.validate_dbt_runtime_payload_descriptor

    def counted(descriptor, **kwargs):
        observed.append(descriptor["id"])
        return validate(descriptor, **kwargs)

    monkeypatch.setattr(binding, "validate_dbt_runtime_payload_descriptor", counted)
    _validate(release, inventory)
    expected = [item["id"] for item in release["artifacts"]["runtime_payloads"]]
    assert sorted(observed) == sorted(expected)


def test_removing_project_from_resealed_snapshot_does_not_hide_live_workloads() -> None:
    inventory = _inventory()
    release = _release(inventory)
    reduced = DbtSourceInventory.build(inventory.projects[:1])
    release["provenance"]["source_snapshot_sha256"] = reduced.snapshot_sha256
    with pytest.raises(ValueError):
        _validate(_seal(release), reduced)


@pytest.mark.parametrize("section", ["workload_packs", "dag_specs", "runtime_payloads"])
@pytest.mark.parametrize("mutation", ["missing", "duplicate", "extra"])
def test_missing_duplicate_or_orphan_descriptors_rejected(section: str, mutation: str) -> None:
    inventory = _inventory()
    release = _release(inventory)
    items = release["artifacts"][section]
    if mutation == "missing":
        items.pop()
    else:
        item = deepcopy(items[0])
        if mutation == "extra":
            item["id"] += "_orphan"
        items.append(item)
    with pytest.raises(ValueError):
        _validate(_seal(release), inventory)


@pytest.mark.parametrize("mutation", ["reverse", "foreign", "missing"])
def test_workload_must_reference_its_exact_ordered_trio(mutation: str) -> None:
    inventory = _inventory()
    release = _release(inventory)
    first, second = release["artifacts"]["workload_packs"]
    if mutation == "reverse":
        first["runtime_payload_ids"].reverse()
    elif mutation == "foreign":
        first["runtime_payload_ids"] = second["runtime_payload_ids"]
    else:
        first.pop("runtime_payload_ids")
    with pytest.raises(ValueError):
        _validate(_seal(release), inventory)


def test_transfer_pack_remains_in_release_without_acquiring_dbt_payloads() -> None:
    inventory = _inventory()
    release = _release(inventory)
    transfer = {
        "id": "transfer_alpha",
        "path": "packs/transfer_alpha.airflow-pack.json",
        "sha256": sha256_bytes(b"transfer"),
        "bytes": 1,
        "pack_fingerprint": sha256_bytes(b"transfer"),
    }
    release["artifacts"]["workload_packs"].append(transfer)
    _validate(_seal(release), inventory)
    transfer["runtime_payload_ids"] = list(inventory.projects[0].workflows[0].runtime_payload_ids)
    with pytest.raises(ValueError):
        _validate(_seal(release), inventory)


@pytest.mark.parametrize("field", ["path", "kind", "media_type", "sha256"])
def test_resealed_noncanonical_runtime_descriptor_rejected(field: str) -> None:
    inventory = _inventory()
    release = _release(inventory)
    release["artifacts"]["runtime_payloads"][0][field] = "wrong"
    with pytest.raises(ValueError):
        _validate(_seal(release), inventory)


def test_aggregate_payload_bytes_are_not_multiplied_per_project() -> None:
    inventory = _inventory()
    release = _release(inventory)
    for item in release["artifacts"]["runtime_payloads"]:
        item["bytes"] = 256 * 1024 * 1024
    with pytest.raises(ValueError):
        _validate(_seal(release), inventory)


@pytest.mark.parametrize("mutation", ["wire", "identity", "snapshot", "authority", "certification"])
def test_release_authority_is_required_in_addition_to_inventory(mutation: str) -> None:
    inventory = _inventory()
    release = _release(inventory)
    if mutation == "wire":
        release["producer"]["wire_contract"] = "dpone.dbt-airflow-self-service.v1"
    elif mutation == "snapshot":
        release["provenance"]["source_snapshot_sha256"] = sha256_bytes(b"other")
    elif mutation == "authority":
        release["selection_authority"] = "preview"
    elif mutation == "certification":
        release["provenance"]["route_certifications"] = []
    _seal(release)
    if mutation == "identity":
        release["release_id"] = sha256_bytes(b"wrong")
    with pytest.raises(ValueError):
        _validate(release, inventory)
