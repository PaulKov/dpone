"""Real file reads remain bound to the captured build output identity."""

from dataclasses import replace
from uuid import UUID

import pytest

from dpone.contracts.native_trusted_dbt_environment import TrustedDbtOwnedRoot
from dpone.runtime.native_generation_build_artifacts import CapturedBuildArtifactReader


def captured(tmp_path):
    root = tmp_path / "output"
    target = root / "build" / "target"
    target.mkdir(parents=True)
    (target / "manifest.json").write_bytes(b'{"manifest":1}')
    (target / "run_results.json").write_bytes(b'{"results":1}')
    stat = root.stat()
    identity = TrustedDbtOwnedRoot(
        "dpone.trusted-dbt-owned-root.v1", "OUTPUT", UUID(int=1), "owned-output", stat.st_dev, stat.st_ino, None
    )
    return root, target, identity


def test_reader_captures_two_fixed_bounded_files(tmp_path):
    root, target, identity = captured(tmp_path)
    reader = CapturedBuildArtifactReader(root=root, target=target, identity=identity, max_bytes=32)
    assert reader(root, "build/target/manifest.json", max_bytes=32) == b'{"manifest":1}'
    assert reader(root, "build/target/run_results.json", max_bytes=32) == b'{"results":1}'


@pytest.mark.parametrize(
    "relative", ["other.json", "build/target/../manifest.json", "/manifest.json", "build/target/secrets.json"]
)
def test_reader_rejects_nonadmitted_files(tmp_path, relative):
    root, target, identity = captured(tmp_path)
    reader = CapturedBuildArtifactReader(root=root, target=target, identity=identity, max_bytes=32)
    with pytest.raises(ValueError):
        reader(root, relative, max_bytes=32)


@pytest.mark.parametrize("bound", [True, 0, -1, 33])
def test_reader_cannot_expand_captured_byte_allowance(tmp_path, bound):
    root, target, identity = captured(tmp_path)
    reader = CapturedBuildArtifactReader(root=root, target=target, identity=identity, max_bytes=32)
    with pytest.raises(ValueError):
        reader(root, "build/target/manifest.json", max_bytes=bound)


def test_reader_rejects_replaced_root_even_with_same_path(tmp_path):
    root, target, identity = captured(tmp_path)
    reader = CapturedBuildArtifactReader(root=root, target=target, identity=identity, max_bytes=32)
    root.rename(tmp_path / "old-output")
    target.mkdir(parents=True)
    (target / "manifest.json").write_bytes(b'{"manifest":1}')
    with pytest.raises(OSError):
        reader(root, "build/target/manifest.json", max_bytes=32)


def test_reader_rejects_symlink_and_oversized_output(tmp_path):
    root, target, identity = captured(tmp_path)
    reader = CapturedBuildArtifactReader(root=root, target=target, identity=identity, max_bytes=32)
    (target / "manifest.json").unlink()
    (target / "manifest.json").symlink_to(target / "run_results.json")
    with pytest.raises(OSError):
        reader(root, "build/target/manifest.json", max_bytes=32)
    (target / "run_results.json").write_bytes(b"x" * 33)
    with pytest.raises(OSError):
        reader(root, "build/target/run_results.json", max_bytes=32)


def test_reader_rejects_wrong_role_root_or_external_target(tmp_path):
    root, target, identity = captured(tmp_path)
    with pytest.raises(ValueError):
        CapturedBuildArtifactReader(root=root, target=target, identity=replace(identity, role="PROFILE"), max_bytes=32)
    with pytest.raises(ValueError):
        CapturedBuildArtifactReader(root=root, target=tmp_path / "elsewhere", identity=identity, max_bytes=32)
