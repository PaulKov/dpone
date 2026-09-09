"""Real workspace files through cache readers; no SQL or physical certification."""

import json
import os
import shutil
from contextlib import contextmanager
from copy import deepcopy

import pytest

from dpone.app import dbt_publish_composition
from dpone.app.dbt_promotion_composition import RuntimeDbtProjectBundleOperations
from dpone.app.dbt_publish_composition import build_dbt_release_materializer
from dpone.contracts.airflow_deployment import deployment_id as compute_deployment_id
from dpone.contracts.airflow_deployment import release_id as compute_release_id
from dpone.gitops.release_set_validation import release_set_validation_failure
from dpone.readiness import dbt_publish_release_materializer as installation
from dpone.readiness.airflow_deployment_projection import AirflowDeploymentProjectionService
from dpone.runtime.deployment_cache_common import DeploymentCacheError
from dpone.runtime.deployment_cache_materializer import DeploymentCacheMaterializer
from dpone.runtime.deployment_cache_recovery import DeploymentCacheRecoveryApplier
from dpone.runtime.deployment_cache_recovery_models import DeploymentCacheRecoveryApplyError
from dpone.services import dbt_release_source_reader as source_reader_module
from dpone.services.dbt_release_integrity import DbtReleaseIntegrityService
from dpone.services.dbt_release_source_reader import DbtReleaseSourceReader
from tests.airflow_cache_promotion_test_support import write_cache_fixture
from tests.test_dbt_airflow_release_e2e import _compiled_release, _config_map_ref, _write_environment
from tests.test_dbt_workspace_artifact_writer import _case


@pytest.fixture
def workspace(tmp_path, monkeypatch):
    writer, check, _, _ = _case(tmp_path)
    root = tmp_path / "compiled"
    tree = writer.write(check, root=tmp_path, output_dir=root)
    # Structural fixture authority is not a release or route certification.
    monkeypatch.setattr(installation, "installed_version", lambda: "0.74.28")
    return root, tree


def test_workspace_release_passes_shared_public_schema_and_authority(workspace):
    _, tree = workspace
    assert release_set_validation_failure(json.loads(tree.files["release-set.json"])) is None


@pytest.mark.parametrize("mutation", ["unknown-wire", "missing-producer", "preview", "uncertified"])
def test_dispatch_does_not_relax_workspace_authority(workspace, mutation):
    _, tree = workspace
    release = deepcopy(json.loads(tree.files["release-set.json"]))
    if mutation == "unknown-wire":
        release["producer"]["wire_contract"] = "unsupported"
    elif mutation == "missing-producer":
        del release["producer"]
    elif mutation == "preview":
        release["selection_authority"] = "manifest_preview"
    else:
        release["provenance"]["route_certifications"][0]["evidence_status"] = "UNVERIFIED"
    assert release_set_validation_failure(release) is not None


def test_workspace_installation_preserves_complete_bytes_and_retries_without_replacement(workspace, tmp_path):
    root, tree = workspace
    materializer = build_dbt_release_materializer()
    result = materializer.materialize(compiled_root=root, cache_root=tmp_path / ".dpone-cache")
    assert result.release_id == tree.release_id and not result.no_op
    assert {
        p.relative_to(result.release_dir).as_posix(): p.read_bytes()
        for p in result.release_dir.rglob("*")
        if p.is_file()
    } == dict(tree.files)
    assert DbtReleaseIntegrityService().verify(result.release_dir).file_count == len(tree.files) - 1
    inodes = {name: (result.release_dir / name).stat().st_ino for name in tree.files}
    repeated = materializer.materialize(compiled_root=root, cache_root=tmp_path / ".dpone-cache")
    assert repeated.no_op
    assert {name: (result.release_dir / name).stat().st_ino for name in tree.files} == inodes


@pytest.mark.parametrize("mutation", ["source", "subject", "orphan", "producer-version"])
def test_invalid_workspace_never_installs_a_partial_cache(workspace, tmp_path, monkeypatch, mutation):
    root, _ = workspace
    if mutation == "source":
        (root / "_dbt/dbt-source-snapshot.json").write_text("{}")
    elif mutation == "subject":
        (root / "release-subjects.sha256").write_text("invalid")
    elif mutation == "orphan":
        (root / "unreferenced.sql").write_text("select 1")
    else:
        monkeypatch.setattr(installation, "installed_version", lambda: "different")
    with pytest.raises(installation.DbtReleaseMaterializationError):
        build_dbt_release_materializer().materialize(compiled_root=root, cache_root=tmp_path / ".dpone-cache")
    assert not (tmp_path / ".dpone-cache/releases").exists()


@pytest.mark.parametrize("case", ["oversized-pack", "oversized-total"])
def test_workspace_metadata_bounds_precede_artifact_capture(workspace, tmp_path, monkeypatch, case):
    root, tree = workspace
    release = json.loads(tree.files["release-set.json"])
    if case == "oversized-pack":
        release["artifacts"]["workload_packs"][0]["bytes"] = 8 * 1024 * 1024 + 1
    else:
        # Each schema is permitted individually, but the complete declaration
        # exceeds the existing integrity budget. No large files are allocated.
        release["artifacts"]["canonical_schemas"] = [
            {
                "id": f"schema_{i}",
                "path": f"schemas/dbt/schema_{i}.schema.json",
                "sha256": "sha256:" + "a" * 64,
                "bytes": 256 * 1024 * 1024,
            }
            for i in range(9)
        ]
    release["release_id"] = compute_release_id(release)
    (root / "release-set.json").write_text(json.dumps(release))
    original = installation.read_confined_file
    reads = []

    def observed_read(root, path, **kwargs):
        reads.append(path)
        return original(root, path, **kwargs)

    monkeypatch.setattr(installation, "read_confined_file", observed_read)
    monkeypatch.setattr(
        dbt_publish_composition,
        "build_dbt_release_source_reader",
        lambda: DbtReleaseSourceReader(bundle_operations=RuntimeDbtProjectBundleOperations(), read_file=observed_read),
    )
    with pytest.raises(installation.DbtReleaseMaterializationError):
        build_dbt_release_materializer().materialize(compiled_root=root, cache_root=tmp_path / ".dpone-cache")
    assert set(reads) <= {"release-set.json", "_dbt/dbt-source-snapshot.json", "release-subjects.sha256"}
    assert not (tmp_path / ".dpone-cache").exists()


def test_workspace_missing_source_verifier_fails_without_cache_mutation(workspace, tmp_path):
    root, _ = workspace
    with pytest.raises(installation.DbtReleaseMaterializationError, match="complete-source verifier"):
        installation.DbtReleaseMaterializer().materialize(compiled_root=root, cache_root=tmp_path / ".dpone-cache")
    assert not (tmp_path / ".dpone-cache").exists()


def test_same_id_provenance_replacement_cannot_replace_authorized_metadata(workspace, tmp_path):
    root, tree = workspace
    changed = False

    def substitute_provenance(read_root, path, **kwargs):
        nonlocal changed
        if read_root == root and path == "_dbt/dbt-source-snapshot.json" and not changed:
            changed = True
            release = json.loads(tree.files["release-set.json"])
            assert compute_release_id(release) == tree.release_id
            replacement = json.dumps(release, indent=3).encode()
            assert replacement != tree.files["release-set.json"]
            assert release_set_validation_failure(json.loads(replacement)) is None
            (root / "release-set.json").write_bytes(replacement)
            DbtReleaseIntegrityService().write(root)
        return installation.read_confined_file(read_root, path, **kwargs)

    reader = DbtReleaseSourceReader(
        bundle_operations=RuntimeDbtProjectBundleOperations(),
        read_file=substitute_provenance,
    )
    with pytest.raises(installation.DbtReleaseMaterializationError):
        installation.DbtReleaseMaterializer(workspace_source_reader=reader).materialize(
            compiled_root=root, cache_root=tmp_path / ".dpone-cache"
        )
    assert changed and not (tmp_path / ".dpone-cache").exists()


def test_publication_uses_verified_snapshot_not_later_source_bytes(workspace, tmp_path, monkeypatch):
    root, tree = workspace
    publish = installation.materialize_immutable_local_release

    def change_source_then_publish(destination, files):
        (root / "_dbt/dbt-source-snapshot.json").write_text("{}")
        return publish(destination, files)

    monkeypatch.setattr(installation, "materialize_immutable_local_release", change_source_then_publish)
    result = build_dbt_release_materializer().materialize(compiled_root=root, cache_root=tmp_path / ".dpone-cache")
    assert (result.release_dir / "_dbt/dbt-source-snapshot.json").read_bytes() == tree.files[
        "_dbt/dbt-source-snapshot.json"
    ]


@pytest.fixture
def projected_workspace(workspace, tmp_path):
    root, tree = workspace
    cache = tmp_path / ".dpone-cache"
    build_dbt_release_materializer().materialize(compiled_root=root, cache_root=cache)
    return cache, _project_release(tmp_path, tree.release_id)


def test_stage_cleanup_failure_prevents_cache_publication(workspace, tmp_path, monkeypatch):
    root, _ = workspace
    temporary_directory = source_reader_module.TemporaryDirectory
    reached_cleanup = []

    @contextmanager
    def failing_cleanup(*args, **kwargs):
        with temporary_directory(*args, **kwargs) as stage:
            yield stage
        if kwargs.get("prefix") == "dpone-dbt-cache-verification-":
            reached_cleanup.append(True)
            raise OSError("verification stage cleanup failed")

    monkeypatch.setattr(source_reader_module, "TemporaryDirectory", failing_cleanup)
    with pytest.raises(installation.DbtReleaseMaterializationError):
        build_dbt_release_materializer().materialize(compiled_root=root, cache_root=tmp_path / ".dpone-cache")
    assert reached_cleanup == [True] and not (tmp_path / ".dpone-cache").exists()


def _project_release(tmp_path, release_id):
    _write_environment(tmp_path)
    digest = "sha256:" + "d" * 64
    projection = AirflowDeploymentProjectionService(root=tmp_path).materialize(
        release_id=release_id,
        environment="prod",
        trust_tier="production",
        runtime_image_ref=f"registry.example/dpone-runtime@{digest}",
        runtime_image_digest=digest,
        artifact_registry_ref="dpone-prod-artifacts",
        registry_config_ref=_config_map_ref("registry", "1"),
        trust_policy_ref=_config_map_ref("policy", "2"),
        airflow_bundle_ref="git:" + "d" * 40,
    )
    return projection


def test_singleton_dbt_wire_v1_remains_activatable(tmp_path):
    # Actual singleton bytes and projection; fixture route observations are
    # structural test inputs, not live SQL Server certification.
    compiled = _compiled_release(tmp_path)
    cache = tmp_path / ".dpone-cache"
    installed = build_dbt_release_materializer().materialize(compiled_root=compiled, cache_root=cache)
    projection = _project_release(tmp_path, installed.release_id)
    materializer = DeploymentCacheMaterializer(cache)
    observed = materializer.validate_details(projection.deployment_dir, environment="prod")
    assert observed.dbt_runtime_wire_contract == "dpone.dbt-airflow-self-service.v1"
    current = materializer.promote(projection.deployment_dir, environment="prod")
    assert current.release_id == installed.release_id and (cache / "current").is_symlink()


def _control_state(cache):
    current = cache / "current"
    return (
        os.readlink(current) if current.is_symlink() else None,
        *(
            (cache / name).read_bytes() if (cache / name).exists() else None
            for name in ("current-pointer.json", "current-pointer-audit.jsonl")
        ),
    )


def test_validated_projection_carries_wire_from_the_verified_release(projected_workspace):
    cache, projection = projected_workspace
    result = DeploymentCacheMaterializer(cache).validate_details(projection.deployment_dir, environment="prod")
    assert result.dbt_runtime_wire_contract == "dpone.dbt-airflow-self-service.v2"


def test_outer_schema_downgrade_cannot_enable_workspace_activation(projected_workspace):
    cache, projection = projected_workspace
    old_release_id = projection.airflow_index["release_id"]
    old_dir = cache / "releases" / old_release_id.replace(":", "-", 1)
    release = json.loads((old_dir / "release-set.json").read_bytes())
    release["schema"] = "dpone.release-set.v1"
    release["release_id"] = compute_release_id(release)
    release_id = release["release_id"]
    new_dir = cache / "releases" / release_id.replace(":", "-", 1)
    shutil.copytree(old_dir, new_dir)
    (new_dir / "release-set.json").write_text(json.dumps(release))
    deployment = json.loads((projection.deployment_dir / "deployment.json").read_bytes())
    deployment["release_ref"] = release_id
    deployment_id = compute_deployment_id(deployment)
    old_deployment_id = deployment["deployment_id"]
    deployment["deployment_id"] = deployment_id
    index_bytes = (projection.deployment_dir / "airflow-index.json").read_text()
    for before, after in ((old_release_id, release_id), (old_deployment_id, deployment_id)):
        index_bytes = index_bytes.replace(before, after).replace(
            before.replace(":", "-", 1), after.replace(":", "-", 1)
        )
    candidate = cache / "deployments/prod" / deployment_id.replace(":", "-", 1)
    shutil.copytree(projection.deployment_dir, candidate)
    (candidate / "deployment.json").write_text(json.dumps(deployment))
    (candidate / "airflow-index.json").write_text(index_bytes)
    with pytest.raises(DeploymentCacheError) as caught:
        DeploymentCacheMaterializer(cache).promote(candidate, environment="prod")
    assert caught.value.code == "DPONE_RELEASE_SET_INVALID"
    assert _control_state(cache) == (None, None, None)


@pytest.mark.parametrize("operation", ["promote", "recover"])
@pytest.mark.parametrize("existing_current", [False, True])
def test_workspace_activation_requires_physical_admission(projected_workspace, operation, existing_current):
    cache, projection = projected_workspace
    materializer = DeploymentCacheMaterializer(cache)
    previous_id = None
    if existing_current:
        previous = write_cache_fixture(cache)
        materializer.promote(previous.deployment, environment="dev")
        previous_id = previous.deployment_id
    before = _control_state(cache)
    # Repeat after a rejected attempt: an already sealed snapshot is not admission.
    for _ in range(2):
        with pytest.raises(DeploymentCacheError) as caught:
            getattr(materializer, operation)(
                projection.deployment_dir,
                environment="prod",
                promoted_by="test://platform",
                expected_current_deployment_id=previous_id,
            )
        assert caught.value.code == "DPONE_DBT_WORKSPACE_ADMISSION_UNAVAILABLE"
        assert _control_state(cache) == before
        assert not list(cache.glob(".current.*.tmp"))


@pytest.mark.parametrize("through_applier", [False, True])
@pytest.mark.parametrize("sealed", [False, True])
def test_workspace_audit_repair_cannot_restore_unguarded_authority(projected_workspace, through_applier, sealed):
    cache, projection = projected_workspace
    # Model a pre-existing workspace pointer from an ungated reader. Do not
    # bypass or stub the real validator to create supposedly admitted evidence.
    current_target = projection.deployment_dir
    if sealed:
        with pytest.raises(DeploymentCacheError) as rejected:
            DeploymentCacheMaterializer(cache).promote(projection.deployment_dir, environment="prod")
        assert rejected.value.code == "DPONE_DBT_WORKSPACE_ADMISSION_UNAVAILABLE"
        current_target = cache / "activations/prod" / projection.deployment_dir.name
        assert current_target.is_dir()
    (cache / "current").symlink_to(current_target.relative_to(cache), target_is_directory=True)
    pointer = {
        "schema": "dpone.current-pointer.v1",
        "activation_id": "164a3c74-cf85-4a4a-a087-07c9b07050ff",
        "environment": "prod",
        "deployment_id": projection.airflow_index["deployment_id"],
        "release_id": projection.airflow_index["release_id"],
        "current_path": "current",
        "promoted_by": "test://previous-reader",
        "promoted_at": "2026-08-29T00:00:00+00:00",
        "previous_deployment_id": None,
    }
    (cache / "current-pointer.json").write_text(json.dumps(pointer))
    before = _control_state(cache)
    with pytest.raises((DeploymentCacheError, DeploymentCacheRecoveryApplyError)) as caught:
        if through_applier:
            DeploymentCacheRecoveryApplier(cache).apply(
                environment="prod",
                deployment_id=pointer["deployment_id"],
                confirm_repair=True,
                promoted_by="test://platform",
                expected_current_deployment_id=pointer["deployment_id"],
            )
        else:
            DeploymentCacheMaterializer(cache).repair_audit(
                environment="prod",
                recovery_actor="test://platform",
                expected_current_deployment_id=pointer["deployment_id"],
            )
    assert caught.value.code == "DPONE_DBT_WORKSPACE_ADMISSION_UNAVAILABLE"
    assert _control_state(cache) == before


@pytest.mark.parametrize("operation", ["promote", "recover"])
def test_workspace_cas_failure_precedes_admission(projected_workspace, operation):
    cache, projection = projected_workspace
    with pytest.raises(DeploymentCacheError) as caught:
        getattr(DeploymentCacheMaterializer(cache), operation)(
            projection.deployment_dir,
            environment="prod",
            promoted_by="test://platform",
            expected_current_deployment_id="sha256:" + "a" * 64,
        )
    assert caught.value.code == "DPONE_CURRENT_POINTER_CAS_MISMATCH"
    assert _control_state(cache) == (None, None, None)


@pytest.mark.parametrize("precommit_failure", [False, True])
def test_optional_precommit_is_not_workspace_admission(projected_workspace, precommit_failure):
    cache, projection = projected_workspace
    calls = []
    postcommit_calls = []

    def precommit():
        calls.append(True)
        if precommit_failure:
            raise DeploymentCacheError("DPONE_TEST_PRECOMMIT_FAILURE", "remote CAS rejected")
        # A changed candidate cannot alter the already verified sealed wire.
        (projection.deployment_dir / "airflow-index.json").write_text("{}")

    with pytest.raises(DeploymentCacheError) as caught:
        DeploymentCacheMaterializer(cache).promote(
            projection.deployment_dir,
            environment="prod",
            precommit_check=precommit,
            postcommit_action=postcommit_calls.append,
        )
    expected = "DPONE_TEST_PRECOMMIT_FAILURE" if precommit_failure else "DPONE_DBT_WORKSPACE_ADMISSION_UNAVAILABLE"
    assert caught.value.code == expected and calls == [True]
    assert postcommit_calls == []
    assert _control_state(cache) == (None, None, None)
