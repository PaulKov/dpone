"""Exact aggregate staging; immutable fixtures do not certify a database route."""

from dataclasses import replace
from pathlib import Path

import pytest

from dpone.app.dbt_promotion_composition import RuntimeDbtProjectBundleOperations
from dpone.contracts.dbt_runtime_payloads import DBT_RUNTIME_WIRE_V2, dbt_runtime_payload_reference
from dpone.contracts.dbt_source_inventory import DbtSourceInventory
from dpone.services.dbt_prod_mirror_content import DbtWorkspaceMirrorContent
from dpone.services.dbt_prod_promotion_contract import DbtProdMirrorError
from tests.test_dbt_release_source_reader import _tree


def _content(tmp_path: Path):
    compiled, _, inventory = _tree(tmp_path)
    bundles = {
        project.project_path: (
            compiled
            / dbt_runtime_payload_reference(
                project.workflows[0].runtime_payload_ids[0], wire_contract=DBT_RUNTIME_WIRE_V2
            ).path
        ).read_bytes()
        for project in inventory.projects
    }
    return inventory, bundles, RuntimeDbtProjectBundleOperations(package_environment={})


def test_stages_both_projects_with_exact_tree_and_immutable_input(tmp_path: Path) -> None:
    inventory, bundles, operations = _content(tmp_path)
    content = DbtWorkspaceMirrorContent(inventory=inventory, project_bundles=bundles, bundle_operations=operations)
    bundles.clear()
    destination = tmp_path / "mirror"
    assert not content.matches(destination)
    content.stage(destination)
    content.verify(destination)
    assert content.matches(destination)
    assert (destination / "dbt/alpha/dbt_project.yml").is_file()
    assert (destination / "dbt/beta/dbt_project.yml").is_file()


@pytest.mark.parametrize("extra", ["old_project", "stray.txt", "dbt/old", "dbt/alpha/target", "dbt/beta/logs/run.log"])
def test_does_not_ignore_unlisted_or_generated_paths(tmp_path: Path, extra: str) -> None:
    inventory, bundles, operations = _content(tmp_path)
    content = DbtWorkspaceMirrorContent(inventory=inventory, project_bundles=bundles, bundle_operations=operations)
    destination = tmp_path / "mirror"
    content.stage(destination)
    path = destination / extra
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix:
        path.write_text("unexpected", encoding="utf-8")
    else:
        path.mkdir()
    with pytest.raises(ValueError):
        content.verify(destination)
    assert not content.matches(destination)


def test_missing_project_fails_complete_verification(tmp_path: Path) -> None:
    inventory, bundles, operations = _content(tmp_path)
    content = DbtWorkspaceMirrorContent(inventory=inventory, project_bundles=bundles, bundle_operations=operations)
    destination = tmp_path / "mirror"
    content.stage(destination)
    (destination / "dbt/beta/dbt_project.yml").unlink()
    with pytest.raises(ValueError):
        content.verify(destination)
    assert not content.matches(destination)


def test_git_checkout_file_modes_do_not_change_source_identity(tmp_path: Path) -> None:
    inventory, bundles, operations = _content(tmp_path)
    content = DbtWorkspaceMirrorContent(inventory=inventory, project_bundles=bundles, bundle_operations=operations)
    destination = tmp_path / "mirror"
    content.stage(destination)
    for path in destination.rglob("*"):
        if path.is_file():
            path.chmod(0o644)
    before = {str(path): path.stat().st_mode for path in destination.rglob("*")}
    content.verify(destination)
    assert content.matches(destination)
    assert {str(path): path.stat().st_mode for path in destination.rglob("*")} == before


@pytest.mark.parametrize("change", ["missing", "extra", "swapped"])
def test_bundles_must_exactly_match_the_complete_inventory(tmp_path: Path, change: str) -> None:
    inventory, bundles, operations = _content(tmp_path)
    if change == "missing":
        bundles.pop("dbt/beta")
    elif change == "extra":
        bundles["other"] = bundles["dbt/alpha"]
    else:
        bundles["dbt/beta"] = bundles["dbt/alpha"]
    with pytest.raises(DbtProdMirrorError):
        DbtWorkspaceMirrorContent(inventory=inventory, project_bundles=bundles, bundle_operations=operations)


def test_root_project_is_staged_without_an_artificial_parent(tmp_path: Path) -> None:
    inventory, bundles, operations = _content(tmp_path)
    root_inventory = DbtSourceInventory((replace(inventory.projects[0], project_path="."),))
    content = DbtWorkspaceMirrorContent(
        inventory=root_inventory, project_bundles={".": bundles["dbt/alpha"]}, bundle_operations=operations
    )
    destination = tmp_path / "mirror"
    content.stage(destination)
    assert (destination / "dbt_project.yml").is_file()
    assert content.matches(destination)


@pytest.mark.parametrize("path", ["mirror", "mirror/dbt", "mirror/dbt/alpha"])
def test_symlinked_tree_components_are_not_followed(tmp_path: Path, path: str) -> None:
    inventory, bundles, operations = _content(tmp_path)
    content = DbtWorkspaceMirrorContent(inventory=inventory, project_bundles=bundles, bundle_operations=operations)
    content.stage(tmp_path / "mirror")
    component = tmp_path / path
    original = tmp_path / "moved"
    component.rename(original)
    component.symlink_to(original, target_is_directory=True)
    with pytest.raises((ValueError, OSError)):
        content.verify(tmp_path / "mirror")
    assert (original / "dbt_project.yml").exists() if path.endswith("alpha") else original.is_dir()
