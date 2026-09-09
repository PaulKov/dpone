from __future__ import annotations

import hashlib

from dpone.runtime.file_artifacts import FileExportArtifact, PartitionedFileExportArtifact
from dpone.runtime.lineage.artifact_checksum import ArtifactChecksumService


def test_artifact_checksum_service_hashes_single_file(tmp_path) -> None:
    path = tmp_path / "part.tsv"
    path.write_bytes(b"a\tb\n")

    checksum = ArtifactChecksumService().checksum(FileExportArtifact(str(path), ["a", "b"]))

    assert checksum == "sha256:" + hashlib.sha256(b"a\tb\n").hexdigest()


def test_artifact_checksum_service_hashes_partitioned_artifact_deterministically(tmp_path) -> None:
    first = tmp_path / "p0.tsv"
    second = tmp_path / "p1.tsv"
    first.write_bytes(b"1\n")
    second.write_bytes(b"2\n")
    artifact = PartitionedFileExportArtifact(
        [FileExportArtifact(str(first), ["id"]), FileExportArtifact(str(second), ["id"])],
        ["id"],
    )

    checksum = ArtifactChecksumService().checksum(artifact)

    expected = hashlib.sha256()
    expected.update(hashlib.sha256(b"1\n").hexdigest().encode("utf-8"))
    expected.update(hashlib.sha256(b"2\n").hexdigest().encode("utf-8"))
    assert checksum == "sha256:" + expected.hexdigest()
