"""Pure composition authority tests use synthetic, offline metadata only."""

from copy import deepcopy
from pathlib import Path

import pytest

from dpone.contracts.airflow_deployment import canonical_fingerprint, release_id
from dpone.contracts.dbt_contract_validation import sha256_bytes
from dpone.contracts.dbt_release import build_dbt_release_metadata
from dpone.contracts.dbt_runtime_payloads import DBT_RUNTIME_WIRE_V2, dbt_runtime_payload_reference
from dpone.contracts.release_composition import (
    COMPOSITION_PRODUCER,
    COMPOSITION_SCHEMA,
    ReleaseCompositionReport,
    ReleaseCompositionRequest,
)
from dpone.contracts.release_composition_policy import (
    composition_native_release,
    validate_composition_metadata,
)

DIGEST = sha256_bytes(b"synthetic")
PROMOTION = {
    "schema": "dpone.compact-pack-release-promotion.v1",
    "profile": "compact_v2_runtime_connection_context",
}


def descriptor(item_id, path):
    return {"id": item_id, "path": path, "sha256": DIGEST, "bytes": 9}


def composition():
    payloads = [
        dbt_runtime_payload_reference(f"{kind}_sha256_{DIGEST[7:]}", wire_contract=DBT_RUNTIME_WIRE_V2).descriptor(
            b"synthetic"
        )
        for kind in ("dbt_project", "dbt_manifest", "dbt_selection")
    ]
    native = build_dbt_release_metadata(
        dag_specs=[descriptor("native_dag", "dags/native_dag.dag-spec.json")],
        workload_packs=[
            {
                **descriptor("native_work", "packs/native_work.airflow-pack.json"),
                "pack_fingerprint": DIGEST,
                "runtime_payload_ids": [row["id"] for row in payloads],
            }
        ],
        runtime_descriptors=payloads,
        canonical_schema_descriptors=[descriptor("model", "schemas/dbt/model.schema.json")],
        source_snapshot_sha256=DIGEST,
        selection_authority="dbt_cli",
        route_certifications=[
            {
                "variant_id": "synthetic",
                "route_id": "synthetic",
                "transport": "synthetic",
                "schema_evolution": "synthetic",
                "airflow_runtime_mode": "synthetic",
                "snapshot_id": DIGEST,
                "support": "supported",
                "certification_level": "production-certified",
                "evidence_status": "PASS",
                "evidence_refs": [DIGEST],
                "evidence_reason_codes": [],
            }
        ],
        selection_fingerprints=[DIGEST],
        producer_version="0.74.36",
        wire_contract=DBT_RUNTIME_WIRE_V2,
    )
    native["promotion"] = dict(PROMOTION)
    native["release_id"] = release_id(native)
    inventory = {
        "schema": "dpone.workload-inventory.v1",
        "dag_specs": [descriptor("ordinary_dag", "_dags/ordinary_dag.dag-spec.json")],
        "workload_packs": [
            {**descriptor("ordinary_work", "ordinary_work/airflow-pack.json"), "pack_fingerprint": DIGEST}
        ],
    }
    artifacts = deepcopy(native["artifacts"])
    artifacts["dag_specs"].append(descriptor("ordinary_dag", "dags/ordinary_dag.dag-spec.json"))
    artifacts["workload_packs"].append(
        {**descriptor("ordinary_work", "packs/ordinary_work.airflow-pack.json"), "pack_fingerprint": DIGEST}
    )
    paths = [
        f"_composition/native/{name}"
        for name in ("release-set.json", "dbt-source-snapshot.json", "release-subjects.sha256")
    ]
    paths += [
        f"_composition/standalone/{row['path']}" for key in ("dag_specs", "workload_packs") for row in inventory[key]
    ]
    artifacts["composition_sources"] = [descriptor(path, path) for path in paths]
    release = {
        "schema": COMPOSITION_SCHEMA,
        "producer": {"name": COMPOSITION_PRODUCER, "version": "0.75.0"},
        "promotion": dict(PROMOTION),
        "artifacts": artifacts,
        "constituents": [
            {"id": "native", "kind": "dbt_workspace", "release": native},
            {
                "id": "standalone",
                "kind": "workload_inventory",
                "inventory": inventory,
                "inventory_sha256": canonical_fingerprint(inventory),
            },
        ],
    }
    release["release_id"] = release_id(release)
    return release


def test_complete_composition_preserves_selected_native_authority():
    release = composition()
    validate_composition_metadata(release)
    child = composition_native_release(release, workload_id="native_work")
    assert child == release["constituents"][0]["release"]
    child["artifacts"]["workload_packs"].clear()
    assert release["constituents"][0]["release"]["artifacts"]["workload_packs"]
    with pytest.raises(ValueError):
        composition_native_release(release, workload_id="ordinary_work")
    with pytest.raises(ValueError):
        composition_native_release(release, workload_id="absent")


def test_unordered_inventories_keep_identity_but_ordered_trios_do_not():
    release = composition()
    reordered = deepcopy(release)
    reordered["constituents"].reverse()
    for rows in reordered["artifacts"].values():
        rows.reverse()
    assert release_id(reordered) == release["release_id"]
    validate_composition_metadata(reordered)
    reordered["artifacts"]["workload_packs"][1]["runtime_payload_ids"].reverse()
    assert release_id(reordered) != release["release_id"]


@pytest.mark.parametrize(
    "case",
    [
        "unknown_parent",
        "unknown_producer",
        "unknown_constituent",
        "duplicate_constituent",
        "wrong_kind",
        "missing_native",
        "child_identity",
        "child_wire",
        "child_authority",
        "child_promotion",
        "parent_promotion",
        "missing_pack",
        "duplicate_pack",
        "native_mutation",
        "extra_payload",
        "missing_source",
        "extra_source",
        "source_path",
        "source_digest",
        "inventory_identity",
        "inventory_extra",
        "standalone_runtime",
        "ownership_collision",
        "unknown_descriptor",
        "unsafe_id",
        "bytes_bool",
        "bytes_limit",
        "parent_identity",
        "trio_reorder",
        "orphan_payload",
    ],
)
def test_malformed_composition_fails_closed_even_with_fresh_parent_identity(case):
    release = composition()
    native = release["constituents"][0]["release"]
    ordinary = release["constituents"][1]
    artifacts = release["artifacts"]
    if case == "unknown_parent":
        release["extra"] = True
    elif case == "unknown_producer":
        release["producer"]["extra"] = True
    elif case == "unknown_constituent":
        ordinary["extra"] = True
    elif case == "duplicate_constituent":
        release["constituents"].append(deepcopy(ordinary))
    elif case == "wrong_kind":
        ordinary["kind"] = "dbt_workspace"
    elif case == "missing_native":
        release["constituents"].pop(0)
    elif case == "child_identity":
        native["release_id"] = DIGEST
    elif case == "child_wire":
        native["producer"]["wire_contract"] = "unsupported"
    elif case == "child_authority":
        native["selection_authority"] = "preview"
    elif case == "child_promotion":
        native.pop("promotion")
    elif case == "parent_promotion":
        release["promotion"]["profile"] = "unsupported"
    elif case == "missing_pack":
        artifacts["workload_packs"].pop()
    elif case == "duplicate_pack":
        artifacts["workload_packs"].append(deepcopy(artifacts["workload_packs"][0]))
    elif case == "native_mutation":
        artifacts["workload_packs"][0]["bytes"] += 1
    elif case == "extra_payload":
        artifacts["runtime_payloads"].append(deepcopy(artifacts["runtime_payloads"][0]))
    elif case == "missing_source":
        artifacts["composition_sources"].pop()
    elif case == "extra_source":
        artifacts["composition_sources"].append(descriptor("extra", "extra"))
    elif case == "source_path":
        artifacts["composition_sources"][0]["path"] = "../escape"
    elif case == "source_digest":
        artifacts["composition_sources"][0]["sha256"] = "invalid"
    elif case == "inventory_identity":
        ordinary["inventory_sha256"] = "sha256:" + "a" * 64
    elif case == "inventory_extra":
        ordinary["inventory"]["extra"] = True
    elif case == "standalone_runtime":
        artifacts["workload_packs"][1]["runtime_payload_ids"] = []
    elif case == "ownership_collision":
        ordinary["inventory"]["workload_packs"][0]["id"] = "native_work"
    elif case == "unknown_descriptor":
        artifacts["dag_specs"][1]["extra"] = True
    elif case == "unsafe_id":
        artifacts["dag_specs"][1]["id"] = "../unsafe"
    elif case == "bytes_bool":
        artifacts["dag_specs"][1]["bytes"] = True
    elif case == "bytes_limit":
        artifacts["dag_specs"][1]["bytes"] = 8 * 1024 * 1024 + 1
    elif case == "trio_reorder":
        native["artifacts"]["workload_packs"][0]["runtime_payload_ids"].reverse()
        artifacts["workload_packs"][0]["runtime_payload_ids"].reverse()
    elif case == "orphan_payload":
        native["artifacts"]["workload_packs"][0].pop("runtime_payload_ids")
        artifacts["workload_packs"][0].pop("runtime_payload_ids")
    if case != "child_identity":
        native["release_id"] = release_id(native)
    release["release_id"] = release_id(release) if case != "parent_identity" else DIGEST
    with pytest.raises(ValueError):
        validate_composition_metadata(release)


def test_request_and_report_are_typed_and_json_ready():
    request = ReleaseCompositionRequest(
        Path("native"), DIGEST, Path("ordinary"), DIGEST, Path("out"), "registry/sidecar@sha256:" + "a" * 64
    )
    assert request.profile == PROMOTION["profile"]
    report = ReleaseCompositionReport(
        status="rejected",
        release_id=None,
        output_dir=request.output_dir,
        source_release_id=DIGEST,
        inventory_sha256=DIGEST,
        blockers=("INVALID",),
    )
    assert report.to_dict() == {
        "schema": "dpone.release-composition-report.v1",
        "passed": False,
        "status": "rejected",
        "release_id": None,
        "output_dir": "out",
        "source_release_id": DIGEST,
        "inventory_sha256": DIGEST,
        "blockers": ["INVALID"],
    }


def test_aggregate_budget_counts_native_and_source_artifacts_together():
    release = composition()
    native = release["constituents"][0]["release"]
    # Metadata only: nine separately addressed 256 MiB schemas exceed the
    # existing 2 GiB whole-tree bound without allocating synthetic payloads.
    native["artifacts"]["canonical_schemas"] = [
        {**descriptor(f"model_{i}", f"schemas/dbt/model_{i}.schema.json"), "bytes": 256 * 1024 * 1024} for i in range(9)
    ]
    release["artifacts"]["canonical_schemas"] = deepcopy(native["artifacts"]["canonical_schemas"])
    native["release_id"] = release_id(native)
    release["release_id"] = release_id(release)
    with pytest.raises(ValueError, match="aggregate"):
        validate_composition_metadata(release)


@pytest.mark.parametrize("status,passed", [("passed", True), ("rejected", False), ("durability_uncertain", False)])
def test_publication_report_distinguishes_durability_uncertainty(status, passed):
    report = ReleaseCompositionReport(status, DIGEST, Path("out"), DIGEST, DIGEST)
    assert report.passed is passed
    assert report.to_dict()["passed"] is passed
    assert report.to_dict()["release_id"] == DIGEST


def test_native_and_legacy_envelopes_do_not_implicitly_opt_into_composition():
    release = composition()
    for value in (release["constituents"][0]["release"], {"schema": "dpone.release-set.v1"}):
        with pytest.raises(ValueError):
            validate_composition_metadata(value)


@pytest.mark.parametrize("value", [None, [], "invalid", {}, {"schema": COMPOSITION_SCHEMA}])
def test_malformed_top_level_fails_as_value_error(value):
    with pytest.raises(ValueError):
        validate_composition_metadata(value)


def test_composition_source_bytes_are_bound_to_original_inventory():
    release = composition()
    source = next(
        row for row in release["artifacts"]["composition_sources"] if row["path"].startswith("_composition/standalone/")
    )
    source["bytes"] += 1
    release["release_id"] = release_id(release)
    with pytest.raises(ValueError, match="source differs"):
        validate_composition_metadata(release)


def test_standalone_transport_descriptors_can_change_without_relabeling_native():
    release = composition()
    release["artifacts"]["workload_packs"][1].update(
        bytes=12, sha256=sha256_bytes(b"rewritten"), pack_fingerprint=sha256_bytes(b"new fingerprint")
    )
    release["release_id"] = release_id(release)
    # Only byte-verifying service checks the actual transformation; this policy
    # preserves separate source and final descriptors rather than trusting either.
    validate_composition_metadata(release)


def test_combined_file_count_includes_composition_sidecars():
    release = composition()
    ordinary = release["constituents"][1]
    for i in range(25_000):
        item_id = f"ordinary_{i}"
        source = {**descriptor(item_id, f"{item_id}/airflow-pack.json"), "pack_fingerprint": DIGEST}
        ordinary["inventory"]["workload_packs"].append(source)
        release["artifacts"]["workload_packs"].append({**source, "path": f"packs/{item_id}.airflow-pack.json"})
        path = f"_composition/standalone/{source['path']}"
        release["artifacts"]["composition_sources"].append(descriptor(path, path))
    ordinary["inventory_sha256"] = canonical_fingerprint(ordinary["inventory"])
    release["release_id"] = release_id(release)
    with pytest.raises(ValueError, match="aggregate"):
        validate_composition_metadata(release)


def test_ordinary_inventory_reordering_keeps_pinned_identity():
    release = composition()
    ordinary = release["constituents"][1]
    source = {**descriptor("second", "second/airflow-pack.json"), "pack_fingerprint": DIGEST}
    ordinary["inventory"]["workload_packs"].append(source)
    release["artifacts"]["workload_packs"].append({**source, "path": "packs/second.airflow-pack.json"})
    path = "_composition/standalone/second/airflow-pack.json"
    release["artifacts"]["composition_sources"].append(descriptor(path, path))
    ordinary["inventory_sha256"] = canonical_fingerprint(ordinary["inventory"])
    release["release_id"] = release_id(release)
    ordinary["inventory"]["workload_packs"].reverse()
    assert canonical_fingerprint(ordinary["inventory"]) == ordinary["inventory_sha256"]
    assert release_id(release) == release["release_id"]
    validate_composition_metadata(release)
