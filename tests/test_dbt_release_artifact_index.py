"""One immutable metadata plan; neither signature nor artifact-byte proof."""

import json
from copy import deepcopy
from dataclasses import replace

import pytest

from dpone.contracts.dbt_contract_validation import sha256_bytes
from dpone.contracts.dbt_runtime_release_binding import DbtReleaseArtifactIndex
from tests.test_dbt_source_inventory import _inventory
from tests.test_dbt_source_inventory_binding import _release


def _case():
    release = _release(_inventory())
    release["artifacts"]["canonical_schemas"] = [
        {"id": "fixture", "path": "schemas/dbt/fixture.schema.json", "bytes": 1, "sha256": sha256_bytes(b"s")}
    ]
    return release


def test_index_detaches_nested_membership_from_mutable_release():
    release = _case()
    index = DbtReleaseArtifactIndex.from_release(release)
    source = release["artifacts"]["workload_packs"][0]
    workload_id, ids = source["id"], tuple(source["runtime_payload_ids"])
    source["runtime_payload_ids"].reverse()
    source["path"] = "foreign"
    index.require_workload_trio(workload_id, ids)
    assert index.workloads[workload_id]["path"] != "foreign"
    with pytest.raises(TypeError):
        index.workloads[workload_id]["path"] = "foreign"
    with pytest.raises(TypeError):
        index.workloads[workload_id]["runtime_payload_ids"][0] = "foreign"


def test_unvalidated_constructor_and_replace_cannot_bypass_complete_inventory():
    with pytest.raises(TypeError):
        DbtReleaseArtifactIndex({}, {}, {}, {}, {})
    with pytest.raises((TypeError, ValueError)):
        replace(DbtReleaseArtifactIndex.from_release(_case()), by_path={})


def test_compatibility_descriptor_projection_remains_detached_json():
    from dpone.manifest.dbt_workspace_release_tree import canonical_dbt_workspace_descriptors

    release = _case()
    projected = canonical_dbt_workspace_descriptors(release)
    assert json.loads(json.dumps(projected)) == {
        row["path"]: row for rows in release["artifacts"].values() for row in rows
    }
    row = next(row for row in projected.values() if "runtime_payload_ids" in row)
    row["runtime_payload_ids"].clear()
    assert release["artifacts"]["workload_packs"][0]["runtime_payload_ids"]


def test_index_validates_runtime_descriptors_once_and_rechecks_each_byte_observation(monkeypatch):
    import dpone.contracts.dbt_runtime_release_binding as binding

    release = _case()
    observed = []
    original = binding.validate_dbt_runtime_payload_descriptor

    def counted(value, **kwargs):
        observed.append(value["id"])
        return original(value, **kwargs)

    monkeypatch.setattr(binding, "validate_dbt_runtime_payload_descriptor", counted)
    index = DbtReleaseArtifactIndex.from_release(release)
    for _ in range(2):
        index.require_bytes("schemas/dbt/fixture.schema.json", b"s")
    assert sorted(observed) == sorted(index.payloads)
    with pytest.raises(ValueError):
        index.require_bytes("schemas/dbt/fixture.schema.json", b"changed")
    with pytest.raises(ValueError):
        index.require_bytes("orphan", b"s")


@pytest.mark.parametrize("section", ["workload_packs", "dag_specs", "runtime_payloads", "canonical_schemas"])
def test_identical_duplicate_descriptor_is_not_reader_deduplication(section):
    release = _case()
    release["artifacts"][section].append(deepcopy(release["artifacts"][section][0]))
    with pytest.raises(ValueError):
        DbtReleaseArtifactIndex.from_release(release)


@pytest.mark.parametrize("mutation", ["empty", "foreign-path", "bool-size", "bad-digest", "extra-section"])
def test_complete_plan_retains_canonical_inventory_guards(mutation):
    release = _case()
    row = release["artifacts"]["canonical_schemas"][0]
    if mutation == "empty":
        release["artifacts"]["canonical_schemas"] = []
    elif mutation == "foreign-path":
        row["path"] = "../schema.json"
    elif mutation == "bool-size":
        row["bytes"] = True
    elif mutation == "bad-digest":
        row["sha256"] = "bad"
    else:
        release["artifacts"]["extra"] = []
    with pytest.raises(ValueError):
        DbtReleaseArtifactIndex.from_release(release)
