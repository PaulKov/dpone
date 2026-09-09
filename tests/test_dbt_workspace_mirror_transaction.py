"""One workspace, three replacements: no partial update on exceptions or crash."""

import json
from pathlib import Path, PurePosixPath

import pytest

from dpone.contracts.dbt_source_inventory import DbtSourceInventory
from dpone.services import dbt_prod_mirror_transaction as transaction_module
from dpone.services.dbt_prod_mirror_content import DbtWorkspaceMirrorContent
from dpone.services.dbt_prod_mirror_journal import TRANSACTION_DIRECTORY, serialized_prod_mirror
from dpone.services.dbt_prod_mirror_transaction import DbtProdMirrorTransaction
from dpone.services.dbt_prod_promotion_contract import DbtProdMirrorError
from tests.test_dbt_workspace_mirror_content import _content


class SimulatedProcessCrash(BaseException):
    """Bypass exception rollback like loss of the installing process."""


def _fixture(tmp_path: Path, *, install: bool = True):
    inventory, bundles, operations = _content(tmp_path)
    repository = tmp_path / "repository"
    repository.mkdir()
    arguments = dict(
        repository_root=repository,
        mirror_path=PurePosixPath("mirror"),
        content=DbtWorkspaceMirrorContent(inventory=inventory, project_bundles=bundles, bundle_operations=operations),
        source_snapshot_path=PurePosixPath("metadata/snapshot.json"),
        source_snapshot_bytes=json.dumps(inventory.to_dict()).encode(),
        descriptor_path=PurePosixPath("metadata/promotion.json"),
        descriptor_bytes=b"old descriptor",
    )
    transaction = DbtProdMirrorTransaction(bundle_operations=operations)
    if install:
        transaction.install_content(**arguments)
    return repository, transaction, arguments, inventory, bundles, operations


def _files(root: Path) -> dict[str, bytes]:
    return {path.relative_to(root).as_posix(): path.read_bytes() for path in root.rglob("*") if path.is_file()}


def test_complete_workspace_is_no_op_and_project_removal_is_confined(tmp_path: Path) -> None:
    repository, transaction, arguments, inventory, bundles, operations = _fixture(tmp_path)
    outside = repository / "author-owned.sql"
    outside.write_bytes(b"keep me")
    assert transaction.install_content(**arguments).no_op
    reduced = DbtSourceInventory((inventory.projects[0],))
    arguments.update(
        content=DbtWorkspaceMirrorContent(
            inventory=reduced, project_bundles={"dbt/alpha": bundles["dbt/alpha"]}, bundle_operations=operations
        ),
        source_snapshot_bytes=json.dumps(reduced.to_dict()).encode(),
        descriptor_bytes=b"new descriptor",
    )
    report = transaction.install_content(**arguments)
    assert not report.no_op
    assert (repository / "mirror/dbt/alpha/dbt_project.yml").is_file()
    assert not (repository / "mirror/dbt/beta").exists()
    assert outside.read_bytes() == b"keep me"
    assert transaction.install_content(**arguments).no_op


def test_ownership_guard_runs_under_lock_before_even_an_identical_no_op(tmp_path: Path) -> None:
    repository, transaction, arguments, _, _, _ = _fixture(tmp_path)
    before = _files(repository)
    observed = []

    class RejectOwnership:
        def require_owned_or_absent(self, *, repository_root, mirror, snapshot, descriptor):
            observed.append((repository_root, mirror, snapshot, descriptor))
            with pytest.raises(DbtProdMirrorError, match="another.*active"):
                with serialized_prod_mirror(repository_root):
                    pytest.fail("ownership checked outside the transaction lock")
            raise DbtProdMirrorError("ownership rejected")

    with pytest.raises(DbtProdMirrorError, match="ownership rejected"):
        transaction.install_content(**arguments, ownership=RejectOwnership())
    assert observed == [
        (
            repository,
            repository / "mirror",
            repository / "metadata/snapshot.json",
            repository / "metadata/promotion.json",
        )
    ]
    assert _files(repository) == before
    assert not (repository / TRANSACTION_DIRECTORY).exists()


def test_project_b_staging_failure_never_changes_existing_outputs(tmp_path: Path, monkeypatch) -> None:
    repository, transaction, arguments, _, _, operations = _fixture(tmp_path)
    (repository / "mirror/dbt/alpha/models/orders.sql").write_bytes(b"old manual state")
    arguments.update(source_snapshot_bytes=b"new snapshot", descriptor_bytes=b"new descriptor")
    before = _files(repository)
    original = operations.extract

    def fail_beta(archive, destination):
        if destination.name == "beta":
            raise ValueError("project B failed extraction")
        return original(archive, destination)

    monkeypatch.setattr(operations, "extract", fail_beta)
    with pytest.raises(DbtProdMirrorError):
        transaction.install_content(**arguments)
    assert _files(repository) == before
    assert not (repository / TRANSACTION_DIRECTORY).exists()


@pytest.mark.parametrize(
    "paths",
    [
        ("mirror", "mirror/snapshot", "metadata/promotion"),
        ("mirror", "metadata", "metadata/promotion"),
        ("mirror", "Mirror/snapshot", "metadata/promotion"),
        (".", "metadata/snapshot", "metadata/promotion"),
        (".git", "metadata/snapshot", "metadata/promotion"),
    ],
)
def test_unsafe_or_overlapping_outputs_fail_before_writes(tmp_path: Path, paths) -> None:
    repository, transaction, arguments, _, _, _ = _fixture(tmp_path)
    before = _files(repository)
    arguments.update(zip(("mirror_path", "source_snapshot_path", "descriptor_path"), map(PurePosixPath, paths)))
    with pytest.raises(DbtProdMirrorError):
        transaction.install_content(**arguments)
    assert _files(repository) == before
    assert not (repository / TRANSACTION_DIRECTORY).exists()


def test_metadata_symlink_parent_is_rejected_even_for_identical_outputs(tmp_path: Path) -> None:
    repository, transaction, arguments, _, _, _ = _fixture(tmp_path)
    metadata = repository / "metadata"
    moved = repository / "moved-metadata"
    metadata.rename(moved)
    metadata.symlink_to(moved, target_is_directory=True)
    before = _files(moved)
    with pytest.raises(DbtProdMirrorError, match="unsafe"):
        transaction.install_content(**arguments)
    assert metadata.is_symlink()
    assert _files(moved) == before
    assert not (repository / TRANSACTION_DIRECTORY).exists()


@pytest.mark.parametrize("boundary", range(1, 4))
@pytest.mark.parametrize("crash", [False, True])
def test_failed_bootstrap_removes_every_partial_output_and_preserves_existing_empty_parents(
    tmp_path: Path, monkeypatch, boundary: int, crash: bool
) -> None:
    repository, transaction, arguments, _, _, _ = _fixture(tmp_path, install=False)
    # Recovery must not guess that this pre-existing empty directory is disposable.
    (repository / "metadata").mkdir()
    original = transaction_module._replace
    calls = 0

    def fail_after_install(source: Path, destination: Path) -> None:
        nonlocal calls
        original(source, destination)
        calls += 1
        if calls == boundary:
            if crash:
                raise SimulatedProcessCrash()
            raise OSError("injected bootstrap failure")

    monkeypatch.setattr(transaction_module, "_replace", fail_after_install)
    with pytest.raises(SimulatedProcessCrash if crash else DbtProdMirrorError):
        transaction.install_content(**arguments)
    if crash:
        with serialized_prod_mirror(repository):
            pass
    assert _files(repository) == {}
    assert not (repository / "mirror").exists()
    assert (repository / "metadata").is_dir()
    assert not (repository / TRANSACTION_DIRECTORY).exists()


@pytest.mark.parametrize("boundary", range(1, 7))
@pytest.mark.parametrize("crash", [False, True])
def test_every_backup_and_install_boundary_restores_the_whole_previous_workspace(
    tmp_path: Path, monkeypatch, boundary: int, crash: bool
) -> None:
    repository, transaction, arguments, _, _, _ = _fixture(tmp_path)
    (repository / "mirror/dbt/alpha/models/orders.sql").write_bytes(b"old A")
    (repository / "mirror/dbt/beta/models/orders.sql").write_bytes(b"old B")
    before = _files(repository)
    arguments.update(source_snapshot_bytes=b"new snapshot", descriptor_bytes=b"new descriptor")
    original = transaction_module._replace
    calls = 0

    def fail_after_rename(source: Path, destination: Path) -> None:
        nonlocal calls
        original(source, destination)
        calls += 1
        journal = json.loads((repository / TRANSACTION_DIRECTORY / "journal.json").read_bytes())
        assert len(journal["replacements"]) == 3
        if calls == boundary:
            if crash:
                raise SimulatedProcessCrash()
            raise OSError("injected rename boundary failure")

    monkeypatch.setattr(transaction_module, "_replace", fail_after_rename)
    with pytest.raises(SimulatedProcessCrash if crash else DbtProdMirrorError):
        transaction.install_content(**arguments)
    if crash:
        assert (repository / TRANSACTION_DIRECTORY).is_dir()
        with serialized_prod_mirror(repository):
            pass
    assert _files(repository) == before
    assert not (repository / TRANSACTION_DIRECTORY).exists()
    monkeypatch.setattr(transaction_module, "_replace", original)
    assert not transaction.install_content(**arguments).no_op
    assert transaction.install_content(**arguments).no_op
