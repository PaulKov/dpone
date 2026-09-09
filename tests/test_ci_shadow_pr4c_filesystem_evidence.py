from __future__ import annotations

from pathlib import Path

import pytest

from dpone.adapters.filesystem_evidence import DescriptorPinnedCreateOnlyEvidenceWriter, EvidenceWriteError


def test_create_only_writer_persists_and_revalidates_identical_retry(tmp_path: Path) -> None:
    writer = DescriptorPinnedCreateOnlyEvidenceWriter(tmp_path)

    writer.write("capacity.json", b"evidence")
    writer.write("capacity.json", b"evidence")

    assert (tmp_path / "capacity.json").read_bytes() == b"evidence"
    assert not (tmp_path / ".capacity.json.stage").exists()


def test_create_only_writer_rejects_different_or_unsafe_targets(tmp_path: Path) -> None:
    writer = DescriptorPinnedCreateOnlyEvidenceWriter(tmp_path)
    writer.write("capacity.json", b"first")

    with pytest.raises(EvidenceWriteError, match="different bytes"):
        writer.write("capacity.json", b"second")
    with pytest.raises(EvidenceWriteError, match="confined filename"):
        writer.write("../capacity.json", b"x")


def test_create_only_writer_rejects_symlink_target(tmp_path: Path) -> None:
    writer = DescriptorPinnedCreateOnlyEvidenceWriter(tmp_path)
    outside = tmp_path / "outside"
    outside.write_bytes(b"evidence")
    (tmp_path / "capacity.json").symlink_to(outside)

    with pytest.raises(EvidenceWriteError):
        writer.write("capacity.json", b"evidence")
