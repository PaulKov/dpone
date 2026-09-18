"""Offline contract regressions; source admission never certifies SQL execution."""

import pytest

from dpone.manifest.release_composition_ordinary_closure import OrdinaryPackClosureVerifier
from tests.test_release_composition_delivery import composition_request as composition_request


def transfer():
    return {
        "name": "synthetic_copy",
        "source": {"type": "postgres", "connection_ref": "reader", "table": {"schema": "public", "name": "sample"}},
        "sink": {
            "type": "mssql",
            "connection_ref": "writer",
            "table": {"schema": "sample", "name": "copy"},
            "strategy": {"mode": "full_refresh"},
        },
        "state": {
            "type": "mssql",
            "connection_ref": "writer",
            "table": {"schema": "state"},
            "atomicity": "target_atomic",
            "provisioning": "external",
        },
    }


def test_plain_transfer_can_carry_explicit_external_target_atomic_state():
    OrdinaryPackClosureVerifier._require_single_transfer(transfer(), "synthetic_copy")


def test_plain_transfer_uses_canonical_external_default_for_target_atomic_state():
    manifest = transfer()
    manifest["state"].pop("provisioning")

    OrdinaryPackClosureVerifier._require_single_transfer(manifest, "synthetic_copy")


def test_plain_transfer_accepts_canonical_target_atomic_state_table_family():
    manifest = transfer()
    manifest["state"].update(
        {
            "run_table": {"name": "run_state"},
            "receipt_table": {"name": "commit_receipt"},
            "repair_authority_table": {"name": "repair_authority"},
            "repair_consumption_table": {"name": "repair_consumption"},
            "audit_table": {"name": "load_audit"},
        }
    )

    OrdinaryPackClosureVerifier._require_single_transfer(manifest, "synthetic_copy")


@pytest.mark.parametrize(
    "field,value",
    [
        ("run_table", {"name": ""}),
        ("receipt_table", {"unexpected": "table"}),
        ("repair_authority_table", "repair_authority"),
        ("repair_consumption_table", {"name": "bad\x00name"}),
        ("audit_table", {"name": 7}),
    ],
)
def test_plain_transfer_rejects_invalid_auxiliary_state_table_coordinates(field, value):
    manifest = transfer()
    manifest["state"][field] = value

    with pytest.raises(ValueError, match=field):
        OrdinaryPackClosureVerifier._require_single_transfer(manifest, "synthetic_copy")


def test_plain_transfer_accepts_explicit_disabled_state():
    manifest = transfer()
    manifest["state"] = {"type": "disabled"}

    OrdinaryPackClosureVerifier._require_single_transfer(manifest, "synthetic_copy")


def test_plain_transfer_accepts_canonical_runtime_managed_mssql_state():
    manifest = transfer()
    manifest["sink"] = {
        "type": "clickhouse",
        "connection_ref": "warehouse",
        "table": {"schema": "mart", "name": "copy"},
        "strategy": {"mode": "full_refresh"},
    }
    manifest["state"] = {
        "type": "mssql",
        "connection_ref": "state",
        "table": {"schema": "control", "name": "source_state"},
        "run_table": {"schema": "control", "run_name": "run_state"},
        "partition_checkpoint_table": {"schema": "control", "name": "partition_checkpoint"},
    }

    OrdinaryPackClosureVerifier._require_single_transfer(manifest, "synthetic_copy")


@pytest.mark.parametrize(
    "state", [None, {}, {"type": "sqlite"}, {"type": "mssql", "atomicity": "eventual", "provisioning": "external"}]
)
def test_unbounded_state_does_not_enter_verified_ordinary_closure(state):
    manifest = transfer()
    manifest["state"] = state
    with pytest.raises(ValueError):
        OrdinaryPackClosureVerifier._require_single_transfer(manifest, "synthetic_copy")


def test_parent_reader_retains_generated_and_standalone_transfers(composition_request):
    from dataclasses import replace

    from dpone.app.release_composition import build_composition_source_reader, build_release_composition_service

    report = build_release_composition_service().compose(composition_request)
    assert report.passed
    reader = build_composition_source_reader()
    sources = reader.read_sources(composition_request.output_dir, expected_release_id=report.release_id)
    assert sources.release_id != sources.native.release_id
    assert "orders" in dict(sources.transfer_manifests)
    assert len(sources.transfer_manifests) > 1  # Includes the native-generated workload.
    assert {key for key, _ in sources.workload_pins} == (
        {key for key, _ in sources.native.required_workloads}
        | {row["id"] for row in sources.ordinary.inventory["workload_packs"]}
    )
    with pytest.raises(ValueError, match="complete source closure"):
        replace(sources, workload_pins=tuple(pin for pin in sources.workload_pins if pin[0] != "orders"))
    with pytest.raises(ValueError, match="transfer sources are incomplete"):
        replace(sources, transfer_manifests=tuple(row for row in sources.transfer_manifests if row[0] != "orders"))


def test_existing_ordinary_sql_file_delivery_remains_accepted(composition_request, tmp_path):
    from dataclasses import replace

    from dpone.app.release_composition import build_composition_source_reader, build_release_composition_service
    from tests.test_release_composition_ordinary import SIDECAR, ordinary_root

    source = tmp_path / "with_sql"
    source.mkdir()
    ordinary = ordinary_root(source, sql_file=True)
    service = build_release_composition_service()
    inventory = service.inventory(ordinary, xcom_sidecar_image=SIDECAR)
    report = service.compose(
        replace(composition_request, standalone_root=ordinary, expected_inventory_sha256=inventory["inventory_sha256"])
    )
    assert report.passed, report.blockers
    sources = build_composition_source_reader().read_sources(
        composition_request.output_dir, expected_release_id=report.release_id
    )
    assert b"sql_file: query.sql" in dict(sources.transfer_manifests)["orders"]


@pytest.mark.parametrize("state_valid, resources_valid", [(True, True), (True, False), (False, True), (False, False)])
def test_state_and_resource_admission_each_retain_their_own_boundary(state_valid, resources_valid):
    manifest = transfer()
    manifest["gitops"] = {"airflow": {"resources": {"requests": {"cpu": "250m", "memory": "128Mi"}}}}
    if not state_valid:
        manifest["state"]["provisioning"] = "automatic"
    if not resources_valid:
        manifest["gitops"]["airflow"]["runner"] = {"command": "untrusted"}
    if state_valid and resources_valid:
        OrdinaryPackClosureVerifier._require_single_transfer(manifest, "synthetic_copy")
    else:
        with pytest.raises(ValueError):
            OrdinaryPackClosureVerifier._require_single_transfer(manifest, "synthetic_copy")
