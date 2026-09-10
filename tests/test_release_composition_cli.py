"""Executable public CLI/API parity and invalid-input side-effect boundaries."""

import json
import subprocess
import sys
from dataclasses import replace

import pytest
import yaml

from dpone.app.release_composition import build_release_composition_service
from dpone.contracts.release_composition_ordinary import OrdinaryReleaseInventoryError
from tests.test_release_composition_delivery import composition_request as composition_request
from tests.test_release_composition_ordinary import ordinary_root


def invoke(*args):
    return subprocess.run(
        [sys.executable, "-c", "from dpone.cli.main import main; main()", "gitops", "airflow", *args],
        capture_output=True,
        text=True,
        check=False,
    )


@pytest.mark.parametrize(
    "sidecar",
    [
        "@sha256:" + "a" * 64,
        "bad name@sha256:" + "a" * 64,
        "sidecar:latest",
        "registry.example:65536/team/xcom@sha256:" + "a" * 64,
    ],
)
def test_inventory_rejects_invalid_sidecar_in_cli_and_api_without_source_writes(tmp_path, sidecar):
    root = ordinary_root(tmp_path)
    before = {p.relative_to(tmp_path): p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}
    with pytest.raises(OrdinaryReleaseInventoryError) as error:
        build_release_composition_service().inventory(root, xcom_sidecar_image=sidecar)
    assert error.value.code == "DPONE_RELEASE_COMPOSITION_ORDINARY_INVALID"
    assert sidecar not in str(error.value)

    result = invoke("release-inventory", "--pack-root", str(root), "--xcom-sidecar-image", sidecar)
    assert result.returncode == 2
    report = json.loads(result.stdout)
    assert report["schema"] == "dpone.workload-inventory-report.v1"
    assert report["passed"] is False and report["inventory_sha256"] is None
    assert report["blockers"][0].startswith("DPONE_COMPOSITION_INVENTORY_INVALID:")
    assert result.stderr == "" and sidecar not in result.stdout
    assert {p.relative_to(tmp_path): p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()} == before


def test_cli_inventory_and_compose_match_public_api(composition_request):
    request = composition_request
    inventory = invoke(
        "release-inventory",
        "--pack-root",
        str(request.standalone_root),
        "--xcom-sidecar-image",
        request.xcom_sidecar_image,
    )
    assert inventory.returncode == 0, inventory.stderr + inventory.stdout
    assert inventory.stderr == ""
    assert json.loads(inventory.stdout)["inventory_sha256"] == request.expected_inventory_sha256
    manifest = request.output_dir.parent / "composition.yaml"
    manifest.write_text(
        yaml.safe_dump(
            {
                "schema": "dpone.release-composition.v1",
                "native_workspace": {
                    "root": str(request.native_root),
                    "expected_release_id": request.expected_release_id,
                },
                "standalone": {
                    "root": str(request.standalone_root),
                    "expected_inventory_sha256": request.expected_inventory_sha256,
                },
                "transport": {"profile": request.profile, "xcom_sidecar_image": request.xcom_sidecar_image},
            }
        )
    )
    result = invoke("release-compose", "--manifest", str(manifest), "--output-dir", str(request.output_dir))
    assert result.returncode == 0, result.stderr + result.stdout
    assert result.stderr == ""
    assert json.loads(result.stdout) == build_release_composition_service().compose(request).to_dict()


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"schema": "dpone.release-composition.v9"},
        {"schema": "dpone.release-composition.v1", "native_workspace": None},
    ],
)
def test_invalid_manifest_is_single_json_failure_without_publication(tmp_path, payload):
    source = tmp_path / "composition.yaml"
    source.write_text(yaml.safe_dump(payload))
    output = tmp_path / "composed"
    result = invoke("release-compose", "--manifest", str(source), "--output-dir", str(output))
    assert result.returncode == 2
    assert not json.loads(result.stdout)["passed"]
    assert result.stderr == "" and not output.exists()


@pytest.mark.parametrize("source", ["native_root", "standalone_root"])
def test_api_rejects_output_source_alias(composition_request, source):
    request = composition_request
    root = getattr(request, source)
    before = {p.relative_to(root): p.read_bytes() for p in root.rglob("*") if p.is_file()}
    report = build_release_composition_service().compose(replace(request, output_dir=root / "derived"))
    assert not report.passed
    assert {p.relative_to(root): p.read_bytes() for p in root.rglob("*") if p.is_file()} == before
