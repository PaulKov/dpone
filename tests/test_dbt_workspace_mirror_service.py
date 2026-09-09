"""Workspace preparation protects ownership before any active tree replacement."""

import json
from dataclasses import replace
from pathlib import Path

import pytest

from dpone.app.dbt_promotion_composition import build_dbt_workspace_mirror_service
from dpone.contracts.dbt_workspace_promotion import DbtWorkspacePromotionDescriptor
from dpone.services.dbt_prod_mirror_journal import TRANSACTION_DIRECTORY, locked_prod_mirror
from dpone.services.dbt_prod_promotion_contract import DbtProdMirrorError
from dpone.services.dbt_release_integrity import DbtReleaseIntegrityService
from tests.test_dbt_prod_mirror import _repository
from tests.test_dbt_release_source_reader import _tree
from tests.test_dbt_workspace_promotion_contract import _descriptor


def _arguments(tmp_path: Path):
    compiled, release, inventory = _tree(tmp_path)
    DbtReleaseIntegrityService().write(compiled)
    repository = _repository(tmp_path)
    descriptor = _descriptor()
    return dict(
        compiled_root=compiled,
        repository_root=repository,
        mirror_root=descriptor.mirror_root,
        source_snapshot_path=descriptor.source_snapshot_path,
        descriptor_path=".dpone/dbt/promotion.json",
        expected_release_id=release["release_id"],
        dev_deployment_id=descriptor.dev_deployment_id,
        dev_evidence_ref=descriptor.dev_evidence_ref,
        trust=descriptor.trust,
    ), inventory


def _files(root: Path):
    return {p.relative_to(root).as_posix(): p.read_bytes() for p in root.rglob("*") if p.is_file()}


def test_workspace_prepare_installs_every_project_and_is_idempotent(tmp_path: Path) -> None:
    arguments, inventory = _arguments(tmp_path)
    service = build_dbt_workspace_mirror_service()
    first = service.prepare(**arguments)
    assert first.no_op is False
    assert first.projects == tuple(project.project_path for project in inventory.projects)
    repository = arguments["repository_root"]
    descriptor = DbtWorkspacePromotionDescriptor.from_payload((repository / arguments["descriptor_path"]).read_bytes())
    assert descriptor.release_id == arguments["expected_release_id"]
    assert descriptor.source_snapshot_sha256 == inventory.snapshot_sha256
    assert descriptor.trust == arguments["trust"]
    assert descriptor.promotion_id == first.promotion_id
    assert service.prepare(**arguments).no_op
    for project in inventory.projects:
        assert (repository / arguments["mirror_root"] / project.project_path / "dbt_project.yml").is_file()


@pytest.mark.parametrize("existing", ["mirror_root", "source_snapshot_path", "descriptor_path"])
def test_bootstrap_never_adopts_an_existing_unowned_destination(tmp_path: Path, existing: str) -> None:
    arguments, _ = _arguments(tmp_path)
    repository = arguments["repository_root"]
    path = repository / arguments[existing]
    path.parent.mkdir(parents=True, exist_ok=True)
    if existing == "mirror_root":
        path.mkdir()
        (path / "keep.sql").write_bytes(b"author-owned SQL")
    else:
        path.write_bytes(b"author-owned metadata")
    before = _files(repository)
    with pytest.raises(DbtProdMirrorError, match="ownership"):
        build_dbt_workspace_mirror_service().prepare(**arguments)
    assert _files(repository) == before
    assert not (repository / TRANSACTION_DIRECTORY).exists()


@pytest.mark.parametrize(
    "change", ["missing_descriptor", "missing_snapshot", "snapshot_hash", "wrong_layout", "old_descriptor"]
)
def test_existing_mirror_requires_valid_matching_ownership_metadata(tmp_path: Path, change: str) -> None:
    arguments, _ = _arguments(tmp_path)
    service = build_dbt_workspace_mirror_service()
    service.prepare(**arguments)
    repository = arguments["repository_root"]
    descriptor_path = repository / arguments["descriptor_path"]
    snapshot_path = repository / arguments["source_snapshot_path"]
    if change == "missing_descriptor":
        descriptor_path.unlink()
    elif change == "missing_snapshot":
        snapshot_path.unlink()
    elif change == "snapshot_hash":
        descriptor = DbtWorkspacePromotionDescriptor.from_payload(descriptor_path.read_bytes())
        descriptor_path.write_text(
            json.dumps(replace(descriptor, source_snapshot_sha256="sha256:" + "9" * 64).to_dict())
        )
    elif change == "wrong_layout":
        descriptor = DbtWorkspacePromotionDescriptor.from_payload(descriptor_path.read_bytes())
        descriptor_path.write_text(json.dumps(replace(descriptor, mirror_root="other-owned-root").to_dict()))
    else:
        descriptor = json.loads(descriptor_path.read_bytes())
        descriptor["schema"] = "dpone.dbt-prod-promotion.v2"
        descriptor_path.write_text(json.dumps(descriptor))
    before = _files(repository)
    with pytest.raises(DbtProdMirrorError, match="ownership"):
        service.prepare(**arguments)
    assert _files(repository) == before
    assert not (repository / TRANSACTION_DIRECTORY).exists()


def test_owned_source_drift_can_be_repaired_without_changing_unowned_files(tmp_path: Path) -> None:
    arguments, _ = _arguments(tmp_path)
    service = build_dbt_workspace_mirror_service()
    service.prepare(**arguments)
    repository = arguments["repository_root"]
    sql = repository / arguments["mirror_root"] / "dbt/beta/models/orders.sql"
    original = sql.read_bytes()
    sql.write_bytes(b"manual drift")
    unowned = repository / "author.sql"
    unowned.write_bytes(b"do not touch")
    assert not service.prepare(**arguments).no_op
    assert sql.read_bytes() == original
    assert unowned.read_bytes() == b"do not touch"


def test_incomplete_compiled_candidate_never_creates_destinations(tmp_path: Path) -> None:
    arguments, _ = _arguments(tmp_path)
    (arguments["compiled_root"] / "_dbt/dbt-source-snapshot.json").unlink()
    before = _files(arguments["repository_root"])
    with pytest.raises(DbtProdMirrorError):
        build_dbt_workspace_mirror_service().prepare(**arguments)
    assert _files(arguments["repository_root"]) == before


def test_valid_ownership_metadata_allows_restoring_a_missing_mirror(tmp_path: Path) -> None:
    arguments, _ = _arguments(tmp_path)
    service = build_dbt_workspace_mirror_service()
    service.prepare(**arguments)
    mirror = arguments["repository_root"] / arguments["mirror_root"]
    moved = tmp_path / "retained-copy"
    mirror.rename(moved)
    before = _files(moved)
    assert not service.prepare(**arguments).no_op
    assert _files(mirror) == before
    assert _files(moved) == before


def test_other_lock_owner_prevents_workspace_preparation(tmp_path: Path) -> None:
    arguments, _ = _arguments(tmp_path)
    before = _files(arguments["repository_root"])
    with locked_prod_mirror(arguments["repository_root"]):
        with pytest.raises(DbtProdMirrorError, match="another.*active"):
            build_dbt_workspace_mirror_service().prepare(**arguments)
    assert _files(arguments["repository_root"]) == before
