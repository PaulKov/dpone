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
