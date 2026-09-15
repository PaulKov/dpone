"""Full policy member identity is required before upstream original admission."""

from dataclasses import replace
from hashlib import sha256

import pytest

from dpone.contracts.dbt_contract_validation import DbtPublishingError
from dpone.contracts.dbt_project_bundle import DbtProjectBundle, DbtProjectFile
from dpone.contracts.native_delivery import NativeOriginalsRefV1, NativePolicyDocumentRef
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_policy_document import verify_native_policy_member

D = "sha256:" + "a" * 64
BODY = b'{"schema":"dpone.dbt-publish-policy.v4"}'


def descriptor():
    return NativePolicyDocumentRef("dpone/native-policy.json", "sha256:" + sha256(BODY).hexdigest(), len(BODY))


def bundle():
    ref = descriptor()
    return DbtProjectBundle(D, 10, ref.bytes, (DbtProjectFile(ref.path, ref.sha256, ref.bytes),))


def test_local_refs_preserve_exact_coordinates_without_filesystem_access(tmp_path):
    root = tmp_path / "not-created"
    refs = NativeOriginalsRefV1(
        root, OriginalRef("release.json", D), OriginalRef("deployment.json", D), OriginalRef("pack.yml", D)
    )
    assert refs.projection_root == root
    assert not root.exists()
    with pytest.raises(ValueError):
        replace(refs, projection_root=type(root)("relative"))


@pytest.mark.parametrize(
    "field,value", [("path", "../escape"), ("sha256", "bad"), ("bytes", True), ("bytes", 0), ("bytes", -1)]
)
def test_policy_descriptor_rejects_coercion_or_invalid_coordinates(field, value):
    with pytest.raises((ValueError, DbtPublishingError)):
        replace(descriptor(), **{field: value})


def test_full_exact_member_verifies_but_is_not_full_schema_or_runtime_qualification():
    # This intentionally incomplete policy body proves member identity only.
    # The v4 schema validator remains an additional mandatory verifier stage.
    assert verify_native_policy_member(BODY, descriptor(), bundle(), max_bytes=len(BODY)) is None


@pytest.mark.parametrize("changed", ["over-bound", "bytes", "digest", "missing", "inventory-size", "noncanonical"])
def test_member_mismatch_rejects_before_any_io(changed):
    payload, ref, inventory, maximum = BODY, descriptor(), bundle(), len(BODY)
    if changed == "over-bound":
        maximum -= 1
    elif changed == "bytes":
        payload += b" "
    elif changed == "digest":
        ref = replace(ref, sha256=D)
    elif changed == "missing":
        inventory = replace(inventory, files=(replace(inventory.files[0], path="different/policy.json"),))
    elif changed == "inventory-size":
        inventory = replace(
            inventory, extracted_bytes=len(BODY) + 1, files=(replace(inventory.files[0], bytes=len(BODY) + 1),)
        )
    else:
        payload = b" " + BODY
        ref = replace(ref, bytes=len(payload), sha256="sha256:" + sha256(payload).hexdigest())
        inventory = replace(
            inventory, extracted_bytes=len(payload), files=(DbtProjectFile(ref.path, ref.sha256, ref.bytes),)
        )
        maximum += 1
    with pytest.raises(ValueError):
        verify_native_policy_member(payload, ref, inventory, max_bytes=maximum)


def test_verifier_rejects_invalid_ref_before_active_lookup(tmp_path):
    from dpone.adapters.native_delivery_originals import NativeOriginalVerifier
    from dpone.contracts.airflow_run_identity import AirflowDeploymentIdentity
    from dpone.manifest.confined_files import read_confined_file

    calls = []
    verifier = NativeOriginalVerifier(
        inputs=None,
        invocation_identity=AirflowDeploymentIdentity(D, D, "11111111-1111-4111-8111-111111111111"),
        require_active=lambda: calls.append("active"),
        bundles=None,
        read_file=read_confined_file,
        release_root=tmp_path / "release",
        max_policy_bytes=1024,
        max_bundle_bytes=1024,
    )
    with pytest.raises(ValueError):
        verifier.resolve(None)
    assert calls == []
    verifier.close()


def _real_native_projection(tmp_path, monkeypatch):
    from types import SimpleNamespace

    from dpone.app.dbt_promotion_composition import RuntimeDbtProjectBundleOperations
    from dpone.app.deployment_cache_workspace_activation_inputs import DeploymentCacheWorkspaceActivationInputs
    from dpone.contracts.airflow_deployment import deployment_id
    from dpone.contracts.airflow_run_identity import AirflowDeploymentIdentity
    from dpone.contracts.dbt_publish_models import DbtPublishIntent
    from dpone.contracts.native_delivery_json import encode_native_delivery_json
    from dpone.manifest.confined_files import read_confined_file
    from dpone.runtime.native_project_documents import NativeProjectDocuments
    from dpone.services.dbt_release_source_reader import DbtReleaseSourceReader
    from tests import test_airflow_runtime_init_fetch_cli as fixture_module
    from tests.test_dbt_native_policy_v4 import native_policy
    from tests.test_dbt_release_source_reader import _tree
    from tests.test_deployment_cache_workspace_activation_inputs import _bytes, _projection, _ResolverFactory

    bundles = RuntimeDbtProjectBundleOperations(package_environment={})
    documents = NativeProjectDocuments(bundles=bundles, read_file=read_confined_file, max_policy_bytes=1024 * 1024)

    def capture(project):
        path = project / "dbt_project.yml"
        path.write_text(path.read_text() + "asset-paths: [dpone]\n")
        policy = native_policy()
        policy["workflows"] = {project.name: {"owner": "synthetic"}}
        return documents.build(
            project, policy=encode_native_delivery_json(policy), intent=DbtPublishIntent(True, "local", project.name)
        )

    monkeypatch.setattr(fixture_module, "build_dbt_project_bundle", capture)
    compiled, release, _ = _tree(tmp_path)
    cache, old_projection, old_release_root, deployment, index = _projection(tmp_path)
    release_root = cache / "releases" / release["release_id"].replace(":", "-")
    compiled.rename(release_root)
    old_release_root.rmdir()
    deployment["release_ref"] = release["release_id"]
    deployment["deployment_id"] = deployment_id(deployment)
    projection = old_projection.parent / deployment["deployment_id"].replace(":", "-")
    old_projection.rename(projection)
    deployment_bytes = _bytes(deployment)
    (projection / "deployment.json").write_bytes(deployment_bytes)
    release_bytes = (release_root / "release-set.json").read_bytes()

    def digest(content):
        return "sha256:" + sha256(content).hexdigest()

    index.update(
        release_id=release["release_id"],
        deployment_id=deployment["deployment_id"],
        release={"sha256": digest(release_bytes)},
        deployment={"sha256": digest(deployment_bytes)},
    )
    (projection / "airflow-index.json").write_bytes(_bytes(index))
    resolver_factory = _ResolverFactory()
    inputs = DeploymentCacheWorkspaceActivationInputs(
        cache_root=cache,
        source_reader=DbtReleaseSourceReader(bundle_operations=bundles, read_file=read_confined_file),
        resolver_factory=resolver_factory,
    )
    selected = next(item for item in release["artifacts"]["workload_packs"] if item["id"] == "dbt__alpha")
    refs = NativeOriginalsRefV1(
        projection,
        OriginalRef("release-set.json", digest(release_bytes)),
        OriginalRef("deployment.json", digest(deployment_bytes)),
        OriginalRef(selected["path"], selected["sha256"]),
    )
    identity = AirflowDeploymentIdentity(
        release["release_id"], deployment["deployment_id"], "11111111-1111-4111-8111-111111111111"
    )
    return SimpleNamespace(
        refs=refs,
        identity=identity,
        inputs=inputs,
        bundles=bundles,
        release_root=release_root,
        resolver_factory=resolver_factory,
    )


def _native_verifier(fixture, calls, *, active_change=None):
    from dpone.adapters.native_delivery_originals import NativeOriginalVerifier
    from dpone.contracts.dbt_workspace_activation import DbtWorkspaceActivationRequest, DbtWorkspaceActiveActivation
    from dpone.manifest.confined_files import read_confined_file
    from tests.test_dbt_workspace_activation_contract import _receipt, _resource

    def active():
        calls.append("active")
        sources = fixture.inputs.load_sources(
            projection_root=fixture.refs.projection_root, release_id=fixture.identity.release_id
        )
        authority = fixture.inputs.load_runtime_authority(
            projection_root=fixture.refs.projection_root,
            environment="prod",
            release_id=fixture.identity.release_id,
            deployment_id=fixture.identity.deployment_id,
        )
        resource = _resource()
        fields = dict(
            activation_id=fixture.identity.activation_id,
            environment="prod",
            release_id=fixture.identity.release_id,
            deployment_id=fixture.identity.deployment_id,
            previous_deployment_id=None,
            source_inventory_sha256=sources.inventory.snapshot_sha256,
            runtime_context_sha256=authority.authority_subject_sha256,
            write_subjects=resource.write_subjects,
            resources=(resource,),
        )
        if active_change:
            fields[active_change[0]] = active_change[1]
        request = DbtWorkspaceActivationRequest.build(**fields)
        return DbtWorkspaceActiveActivation(request, _receipt(request, "ACTIVE"))

    return NativeOriginalVerifier(
        inputs=fixture.inputs,
        invocation_identity=fixture.identity,
        require_active=active,
        bundles=fixture.bundles,
        read_file=read_confined_file,
        release_root=fixture.release_root,
        max_policy_bytes=1024 * 1024,
        max_bundle_bytes=16 * 1024 * 1024,
    )


def test_verifier_authenticates_real_release_bundle_and_preserves_owned_directory_lifetime(tmp_path, monkeypatch):
    fixture = _real_native_projection(tmp_path, monkeypatch)
    calls = []
    with _native_verifier(fixture, calls) as verifier:
        result = verifier.resolve(fixture.refs)
        assert result.project_directory.is_dir()
        assert result.policy_sha256 == "sha256:" + sha256(result.policy_document).hexdigest()
        assert calls == ["active"]
        assert all(not resolver.calls for resolver in fixture.resolver_factory.resolvers)
        project_directory = result.project_directory
    assert not project_directory.exists()


@pytest.mark.parametrize("changed", ["release", "deployment", "pack"])
def test_corrupt_original_rejects_before_active_or_credentials(tmp_path, monkeypatch, changed):
    fixture = _real_native_projection(tmp_path, monkeypatch)
    calls = []
    refs = replace(fixture.refs, **{changed: replace(getattr(fixture.refs, changed), sha256=D)})
    with _native_verifier(fixture, calls) as verifier, pytest.raises(ValueError):
        verifier.resolve(refs)
    assert calls == []
    assert all(not resolver.calls for resolver in fixture.resolver_factory.resolvers)


@pytest.mark.parametrize(
    "change",
    [
        ("activation_id", "22222222-2222-4222-8222-222222222222"),
        ("environment", "other"),
        ("source_inventory_sha256", D),
        ("runtime_context_sha256", D),
    ],
)
def test_verifier_rejects_active_observation_switch_after_offline_preflight(tmp_path, monkeypatch, change):
    fixture = _real_native_projection(tmp_path, monkeypatch)
    calls = []
    with (
        _native_verifier(fixture, calls, active_change=change) as verifier,
        pytest.raises(ValueError, match="readback differs"),
    ):
        verifier.resolve(fixture.refs)
    assert calls == ["active"]


def test_native_transfer_owner_uses_real_release_dag_and_manifest_membership(tmp_path):
    import json

    from dpone.adapters.native_delivery_originals import _owner
    from tests.test_dbt_release_source_reader import _read, _write
    from tests.test_dbt_workspace_release_assembly import _assemble, _project

    tree = _assemble([_project(tmp_path, name) for name in ("alpha", "beta")])
    root = tmp_path / "release"
    for path, body in tree.files.items():
        _write(root, path, body)
    release = json.loads(tree.files["release-set.json"])
    sources = _read(root, release)
    selected = next(item for item in release["artifacts"]["workload_packs"] if item["id"].startswith("dbt_beta_"))
    pack = json.loads((root / selected["path"]).read_bytes())
    owner = _owner(sources, selected["id"], pack)
    assert owner.project.project_path == "beta"
    with pytest.raises(ValueError):
        _owner(sources, selected["id"], {**pack, "runtime_payload_ids": list(owner.source.runtime_payload_ids)})
    with pytest.raises(ValueError):
        _owner(replace(sources, relation_writes=()), selected["id"], pack)


def test_all_declared_project_archives_are_bounded_before_whole_source_acquisition(tmp_path, monkeypatch):
    import json

    from dpone.contracts.airflow_deployment import deployment_id, release_id

    fixture = _real_native_projection(tmp_path, monkeypatch)
    release_path = fixture.release_root / "release-set.json"
    release = json.loads(release_path.read_bytes())
    projects = [item for item in release["artifacts"]["runtime_payloads"] if item["kind"] == "dbt_project_bundle"]
    projects[-1]["bytes"] = 17 * 1024 * 1024
    release["release_id"] = release_id(release)
    release_bytes = json.dumps(release, sort_keys=True).encode()
    release_path.write_bytes(release_bytes)
    deployment_path = fixture.refs.projection_root / "deployment.json"
    deployment = json.loads(deployment_path.read_bytes())
    deployment["release_ref"] = release["release_id"]
    deployment["deployment_id"] = deployment_id(deployment)
    deployment_bytes = json.dumps(deployment, sort_keys=True).encode()
    deployment_path.write_bytes(deployment_bytes)
    fixture.identity = replace(
        fixture.identity, release_id=release["release_id"], deployment_id=deployment["deployment_id"]
    )
    fixture.refs = replace(
        fixture.refs,
        release=replace(fixture.refs.release, sha256="sha256:" + sha256(release_bytes).hexdigest()),
        deployment=replace(fixture.refs.deployment, sha256="sha256:" + sha256(deployment_bytes).hexdigest()),
    )

    def reject_read(**kwargs):
        pytest.fail("whole-source acquisition started before archive admission bounds")

    monkeypatch.setattr(fixture.inputs, "load_sources", reject_read)
    calls = []
    with _native_verifier(fixture, calls) as verifier, pytest.raises(ValueError, match="archive"):
        verifier.resolve(fixture.refs)
    assert calls == []
