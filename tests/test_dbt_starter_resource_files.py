"""Fixed resource path and owned-copy filesystem adapter boundaries."""

import os
import stat
from pathlib import Path
from uuid import uuid4

import pytest
from tools.dbt_self_service.starter_resource_files import preserve_mode, snapshot, validate_path, verify_creation
from tools.dbt_self_service.starter_resource_journal import RESOURCE_PATHS

from dpone.manifest.project_root import inspect_project_root
from dpone.readiness.airflow_pipeline_source import ConfinedAuthoringFileSystem


@pytest.mark.parametrize(
    "path",
    [
        "/tmp/foreign",
        "../foreign",
        "src//dpone/_assets/dbt_dpone/INSTALL.md",
        "src/dpone/_assets/dbt_dpone/../INSTALL.md",
        "unknown",
        "src\\dpone",
        ".dpone-starter-resource-transactions/not-a-uuid/new/000.bin",
    ],
)
def test_noninventory_paths_reject_without_io(tmp_path, path):
    identity = inspect_project_root(tmp_path)
    with pytest.raises(ValueError):
        snapshot(identity, path)
    assert list(tmp_path.iterdir()) == []


def test_only_exact_operation_auxiliary_paths_are_admitted():
    operation = str(uuid4())
    validate_path(f".dpone-starter-resource-transactions/{operation}/old/000.bin")
    target = Path(RESOURCE_PATHS[0])
    validate_path(str(target.with_name(f".{target.name}.{operation}.new")))
    validate_path(str(target.with_name(f".{target.name}.{operation}.restore")))
    with pytest.raises(ValueError):
        validate_path(f".dpone-starter-resource-transactions/{operation}/old/016.bin")


def test_mode_update_requires_original_creation_inode(tmp_path):
    identity = inspect_project_root(tmp_path)
    filesystem = ConfinedAuthoringFileSystem(tmp_path, root_identity=identity)
    created = filesystem.create(Path(RESOURCE_PATHS[0]), b"synthetic")
    preserve_mode(identity, created, 0o755)
    assert stat.S_IMODE((tmp_path / created.path).stat().st_mode) == 0o755
    foreign = tmp_path / "foreign"
    foreign.write_bytes(created.content)
    foreign.chmod(0o600)
    os.replace(foreign, tmp_path / created.path)
    with pytest.raises(ValueError):
        preserve_mode(identity, created, 0o755)
    assert stat.S_IMODE((tmp_path / created.path).stat().st_mode) == 0o600


def test_same_bytes_with_foreign_identity_are_not_owned(tmp_path):
    identity = inspect_project_root(tmp_path)
    filesystem = ConfinedAuthoringFileSystem(tmp_path, root_identity=identity)
    created = filesystem.create(Path(RESOURCE_PATHS[0]), b"synthetic")
    assert verify_creation(identity, created).content == b"synthetic"
    foreign = tmp_path / "foreign"
    foreign.write_bytes(created.content)
    os.replace(foreign, tmp_path / created.path)
    with pytest.raises(ValueError):
        verify_creation(identity, created)
