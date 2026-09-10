"""Public mixed release producer and real offline delivery; no SQL certification."""

import json
from dataclasses import replace
from pathlib import Path

import pytest

from dpone.app.release_composition import build_release_composition_service
from dpone.contracts.release_composition import ReleaseCompositionRequest
from dpone.readiness.airflow_compact_pack_release import materialize_compact_pack_release
from tests.dbt_compact_wire_v2_helpers import SIDECAR, prepare_projects, workspace_service
from tests.test_release_composition_ordinary import ordinary_root


@pytest.fixture
def composition_request(tmp_path):
    prepare_projects(tmp_path / "workspace")
    compiled = tmp_path / "compiled"
    assert workspace_service(tmp_path / "profiles").compile(tmp_path / "workspace", output_dir=compiled).passed
    native = materialize_compact_pack_release(
        pack_root=compiled, cache_root=tmp_path / "native-cache", xcom_sidecar_image=SIDECAR
    )
    assert native.passed, native.blockers
    ordinary = ordinary_root(tmp_path)
    service = build_release_composition_service()
    inventory = service.inventory(ordinary, xcom_sidecar_image=SIDECAR)
    return ReleaseCompositionRequest(
        native_root=Path(native.release_dir),
        expected_release_id=native.release_id,
        standalone_root=ordinary,
        expected_inventory_sha256=inventory["inventory_sha256"],
        output_dir=tmp_path / "composed",
        xcom_sidecar_image=SIDECAR,
    )


def test_public_composition_preserves_sources_and_exact_retry(composition_request):
    service = build_release_composition_service()
    report = service.compose(composition_request)
    assert report.passed, report.blockers
    root = composition_request.output_dir
    release = json.loads((root / "release-set.json").read_bytes())
    assert release["schema"] == "dpone.release-set.v3"
    native = next(row["release"] for row in release["constituents"] if row["id"] == "native")
    assert len(native["artifacts"]["dag_specs"]) == 2
    assert len(release["artifacts"]["dag_specs"]) == 3
    assert {row["id"] for row in release["artifacts"]["workload_packs"]} == {
        row["id"] for row in native["artifacts"]["workload_packs"]
    } | {"orders"}
    for rows in native["artifacts"].values():
        for row in rows:
            assert (root / row["path"]).read_bytes() == (composition_request.native_root / row["path"]).read_bytes()
    before = {p.relative_to(root): p.read_bytes() for p in root.rglob("*") if p.is_file()}
    assert service.compose(composition_request).to_dict() == report.to_dict()
    assert {p.relative_to(root): p.read_bytes() for p in root.rglob("*") if p.is_file()} == before


@pytest.mark.parametrize("field", ["expected_release_id", "expected_inventory_sha256"])
def test_composition_rejects_stale_input_identity(composition_request, field):
    request = replace(composition_request, **{field: "sha256:" + "0" * 64})
    report = build_release_composition_service().compose(request)
    assert not report.passed
    assert not request.output_dir.exists()


def test_composed_release_deployment_provider_fetch_and_launcher(composition_request):
    from tests.test_dbt_compact_wire_v2 import verify_delivery

    service = build_release_composition_service()
    report = service.compose(composition_request)
    assert report.passed, report.blockers
    installed = verify_delivery(composition_request.output_dir.parent, composition_request.output_dir)
    release = json.loads((installed / "release-set.json").read_bytes())
    assert release["release_id"] == report.release_id
    assert release["schema"] == "dpone.release-set.v3"
