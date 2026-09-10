"""Real source readmission, publication faults and activation isolation."""

import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace

import pytest
from dpone_airflow_pack.pack_identity import compute_pack_fingerprint

from dpone.app.dbt_promotion_composition import build_dbt_release_source_reader
from dpone.app.release_composition import build_ordinary_release_inventory_reader, build_release_composition_service
from dpone.contracts.airflow_deployment import release_id
from dpone.contracts.dbt_contract_validation import artifact_json_bytes, sha256_bytes
from dpone.contracts.release_composition import COMPOSITION_ADMISSION
from dpone.manifest.confined_files import read_confined_file
from dpone.runtime.deployment_cache_common import DeploymentCacheError
from dpone.runtime.deployment_cache_workspace_activation import DeploymentCacheWorkspaceActivation
from dpone.runtime.immutable_local_tree import ImmutableLocalTreeDurabilityError, materialize_immutable_local_tree
from dpone.services.dbt_release_integrity import DbtReleaseIntegrityService
from dpone.services.release_composition import ReleaseCompositionService
from dpone.version import installed_version
from tests.test_release_composition_delivery import composition_request as composition_request


def service_with_writer(writer):
    return ReleaseCompositionService(
        native=build_dbt_release_source_reader(),
        ordinary=build_ordinary_release_inventory_reader(),
        integrity=DbtReleaseIntegrityService(),
        publisher=writer,
        read_file=read_confined_file,
        producer_version=installed_version(),
        durability_error=ImmutableLocalTreeDurabilityError,
    )


def test_resealed_source_to_final_forgery_cannot_be_readmitted(composition_request):
    service = build_release_composition_service()
    report = service.compose(composition_request)
    assert report.passed
    root = composition_request.output_dir
    release = json.loads((root / "release-set.json").read_bytes())
    row = next(item for item in release["artifacts"]["workload_packs"] if item["id"] == "orders")
    pack = json.loads((root / row["path"]).read_bytes())
    pack["provider_execution"]["kpo_kwargs"]["pool"] = "forged_pool"
    pack["pack_fingerprint"] = compute_pack_fingerprint(pack)
    body = artifact_json_bytes(pack)
    (root / row["path"]).write_bytes(body)
    row.update(sha256=sha256_bytes(body), bytes=len(body), pack_fingerprint=pack["pack_fingerprint"])
    release["release_id"] = release_id(release)
    (root / "release-set.json").write_bytes(artifact_json_bytes(release))
    (root / "release-subjects.sha256").unlink()
    DbtReleaseIntegrityService().write(root)  # Adversarial valid checksums, never source authority.
    destination = root.parent / "forged-cache"
    with pytest.raises(ValueError, match="transport differs"):
        service.install(root, cache_root=destination)
    assert not destination.exists()


def test_durability_uncertainty_retains_identity_and_visible_tree(composition_request):
    def uncertain(destination, files):
        materialize_immutable_local_tree(destination, files, allowed_parent=destination.parent, root=destination.parent)
        raise ImmutableLocalTreeDurabilityError("synthetic post-rename fault", path=destination)

    report = service_with_writer(uncertain).compose(composition_request)
    assert not report.passed and report.status == "durability_uncertain"
    assert (
        report.release_id
        == json.loads((composition_request.output_dir / "release-set.json").read_bytes())["release_id"]
    )
    retry = build_release_composition_service().compose(composition_request)
    assert retry.passed and retry.release_id == report.release_id


def test_prepublication_failure_never_reports_success(composition_request):
    def failed(destination, files):
        raise OSError("SENSITIVE_SENTINEL")

    report = service_with_writer(failed).compose(composition_request)
    assert not report.passed and "SENSITIVE_SENTINEL" not in str(report.to_dict())
    assert not composition_request.output_dir.exists()


def test_concurrent_identical_writers_converge(composition_request):
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: build_release_composition_service().compose(composition_request), range(2)))
    assert all(report.passed for report in results), [report.blockers for report in results]
    assert results[0].release_id == results[1].release_id


def test_ancestor_symlink_cannot_expand_source_scope(composition_request):
    request = composition_request
    link = request.output_dir.parent / "source-link"
    link.symlink_to(request.standalone_root.parent, target_is_directory=True)
    report = build_release_composition_service().compose(
        replace(request, standalone_root=link / request.standalone_root.name)
    )
    assert not report.passed and not request.output_dir.exists()


@pytest.mark.parametrize("method", ["prepare_occurrence", "require_existing"])
@pytest.mark.parametrize("configured", [False, True])
def test_composition_never_delegates_to_native_only_activation(tmp_path, method, configured):
    calls = []

    class AcceptingCoordinator:
        def __getattr__(self, name):
            calls.append(name)
            raise AssertionError("native-only coordinator must never be invoked for composition")

    guard = DeploymentCacheWorkspaceActivation(AcceptingCoordinator() if configured else None)
    with pytest.raises(DeploymentCacheError) as caught:
        getattr(guard, method)(
            projection_root=tmp_path,
            dbt_wire=COMPOSITION_ADMISSION,
            activation_id="synthetic",
            environment="test",
            release_id="sha256:" + "a" * 64,
            deployment_id="sha256:" + "b" * 64,
            previous_deployment_id=None,
        )
    assert caught.value.code == "DPONE_COMPOSITION_ADMISSION_UNAVAILABLE"
    assert calls == []
    assert list(tmp_path.iterdir()) == []


def test_transport_rejects_descriptor_replacement_after_metadata_validation(composition_request):
    from dpone.manifest.release_composition_files import verify_composition_transport_files

    report = build_release_composition_service().compose(composition_request)
    assert report.passed
    root = composition_request.output_dir
    release = json.loads((root / "release-set.json").read_bytes())
    replacement = json.loads((root / "release-set.json").read_bytes())
    replacement["producer"]["version"] = "99.0.0"
    replacement["release_id"] = release_id(replacement)
    (root / "release-set.json").write_bytes(artifact_json_bytes(replacement))
    (root / "release-subjects.sha256").unlink()
    DbtReleaseIntegrityService().write(root)
    with pytest.raises(ValueError, match="descriptor changed"):
        verify_composition_transport_files(root, release)
