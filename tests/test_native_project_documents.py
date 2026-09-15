"""Native document production round-trips full policy through the real archive."""

from hashlib import sha256

import pytest

from dpone.app.dbt_promotion_composition import RuntimeDbtProjectBundleOperations
from dpone.contracts.dbt_publish_models import DbtPublishIntent
from dpone.contracts.native_delivery_json import encode_native_delivery_json
from dpone.contracts.native_project_documents import NATIVE_INTENT_MEMBER, NATIVE_POLICY_MEMBER
from dpone.manifest.confined_files import read_confined_file
from dpone.runtime.native_project_documents import NativeProjectDocuments
from tests.test_dbt_native_policy_v4 import native_policy


def project(tmp_path, *, assets=True):
    root = tmp_path / "project"
    root.mkdir()
    (root / "dbt_project.yml").write_text(
        "name: example\nversion: '1.0'\nconfig-version: 2\nprofile: example\n"
        + ("asset-paths: [dpone]\n" if assets else "")
    )
    (root / "models").mkdir()
    (root / "models/orders.sql").write_text("select 1 as id\n")
    return root


def service():
    return NativeProjectDocuments(
        bundles=RuntimeDbtProjectBundleOperations(package_environment={}),
        read_file=read_confined_file,
        max_policy_bytes=1024 * 1024,
    )


def test_real_archive_preserves_full_policy_identity_without_modifying_source(tmp_path):
    root = project(tmp_path)
    bundles = RuntimeDbtProjectBundleOperations(package_environment={})
    before = bundles.build(root)
    policy = encode_native_delivery_json(native_policy())
    output = service().build(root, policy=policy, intent=DbtPublishIntent(True, "local", "orders"))
    assert bundles.build(root) == before
    destination = tmp_path / "verified"
    bundle = bundles.extract(output.archive, destination)
    bundles.verify(output.archive, destination)
    assert read_confined_file(destination, NATIVE_POLICY_MEMBER, max_bytes=len(policy)) == policy
    member = next(item for item in bundle.files if item.path == NATIVE_POLICY_MEMBER)
    assert member.sha256 == "sha256:" + sha256(policy).hexdigest()
    document = service().read(destination, bundle)
    assert document[0] == policy
    assert document[1]["model_storage"] == "rowstore_none"
    assert document[1]["native_policy_document"]["sha256"] == member.sha256
    assert any(item.path == NATIVE_INTENT_MEMBER for item in bundle.files)


def test_missing_explicit_asset_admission_is_rejected_without_rewriting_project(tmp_path):
    root = project(tmp_path, assets=False)
    original = (root / "dbt_project.yml").read_bytes()
    with pytest.raises(ValueError):
        service().build(
            root, policy=encode_native_delivery_json(native_policy()), intent=DbtPublishIntent(True, "local", "orders")
        )
    assert (root / "dbt_project.yml").read_bytes() == original


def test_changed_existing_policy_cannot_be_silently_replaced(tmp_path):
    root = project(tmp_path)
    (root / "dpone").mkdir()
    (root / NATIVE_POLICY_MEMBER).write_bytes(b"{}")
    with pytest.raises(ValueError, match="existing"):
        service().build(
            root, policy=encode_native_delivery_json(native_policy()), intent=DbtPublishIntent(True, "local", "orders")
        )
    assert (root / NATIVE_POLICY_MEMBER).read_bytes() == b"{}"


def test_layout_outside_selected_profile_rejects_before_bundle_capture(tmp_path):
    class NoIo:
        def build(self, root):
            pytest.fail("invalid policy selection reached bundle capture")

    native = NativeProjectDocuments(bundles=NoIo(), read_file=read_confined_file, max_policy_bytes=1024 * 1024)
    with pytest.raises(ValueError, match="layout"):
        native.build(
            tmp_path,
            policy=encode_native_delivery_json(native_policy()),
            intent=DbtPublishIntent(True, "local", "orders"),
            model_storage="columnstore",
        )


@pytest.mark.parametrize("changed", ["policy-bytes", "intent-bytes", "missing", "symlink"])
def test_reader_rejects_changed_or_unconfined_members(tmp_path, changed):
    from dpone.manifest.confined_files import ConfinedFileError

    root = project(tmp_path)
    policy = encode_native_delivery_json(native_policy())
    native = service()
    produced = native.build(root, policy=policy, intent=DbtPublishIntent(True, "local", "orders"))
    target = tmp_path / "target"
    bundle = RuntimeDbtProjectBundleOperations(package_environment={}).extract(produced.archive, target)
    if changed == "policy-bytes":
        (target / NATIVE_POLICY_MEMBER).write_bytes(b"{}")
    elif changed == "intent-bytes":
        (target / NATIVE_INTENT_MEMBER).write_bytes(b"{}")
    elif changed == "missing":
        (target / NATIVE_POLICY_MEMBER).unlink()
    else:
        (target / NATIVE_POLICY_MEMBER).unlink()
        outside = tmp_path / "outside.json"
        outside.write_bytes(policy)
        (target / NATIVE_POLICY_MEMBER).symlink_to(outside)
    with pytest.raises((ValueError, ConfinedFileError)):
        native.read(target, bundle)
