"""Fail-closed internal Core Metadata checks for token publication."""

from __future__ import annotations

import io
import os
import random
import stat
import tarfile
import zipfile
from pathlib import Path
from types import ModuleType

import pytest
from typing_extensions import Buffer

from tests.pypi_prepublication_test_support import (
    NAMES,
    VERSION,
    candidate_bytes,
    load_gate,
    package_for,
    publication_context,
    replace_candidate,
    write_inventory,
)


def _evaluate(module: ModuleType, inventory: Path, dist: Path) -> None:
    module.evaluate_prepublication(
        inventory,
        dist,
        expected_version=VERSION,
        context=publication_context(module),
        fetcher=lambda *_: (_ for _ in ()).throw(AssertionError("network reached")),
    )


@pytest.mark.parametrize(
    ("filename", "metadata_name", "metadata_version"),
    [
        (NAMES[0], "dpone", VERSION),
        (NAMES[0], package_for(NAMES[0]), "0.74.1"),
        (NAMES[3], "dpone-airflow-pack", VERSION),
        (NAMES[3], "DPONE", "0.74.1"),
    ],
)
def test_hash_valid_archive_with_mismatched_internal_identity_fails_before_network(
    tmp_path: Path,
    filename: str,
    metadata_name: str,
    metadata_version: str,
) -> None:
    module = load_gate()
    inventory, dist, _ = write_inventory(tmp_path)
    content = candidate_bytes(
        filename,
        metadata_name=metadata_name,
        metadata_version=metadata_version,
    )
    replace_candidate(inventory, dist, filename, content)

    with pytest.raises(module.PrepublicationGateError, match="CORE_METADATA_IDENTITY_INVALID"):
        _evaluate(module, inventory, dist)


@pytest.mark.parametrize(
    "metadata",
    [
        b"Metadata-Version: 2.4\nName: dpone\xff\nVersion: 0.74.0\n\n",
        b"Metadata-Version: 2.4\nName: dpone\n Version: 0.74.0\n\n",
        b"Metadata-Version: 2.4\nName: dpone\nName: dpone\nVersion: 0.74.0\n\n",
    ],
)
def test_invalid_utf8_folded_or_duplicate_identity_header_fails_closed(
    tmp_path: Path,
    metadata: bytes,
) -> None:
    module = load_gate()
    inventory, dist, _ = write_inventory(tmp_path)
    filename = NAMES[2]
    replace_candidate(
        inventory,
        dist,
        filename,
        candidate_bytes(filename, metadata_name="dpone", metadata_version=VERSION, metadata=metadata),
    )

    with pytest.raises(module.PrepublicationGateError, match="CORE_METADATA_(ENCODING|HEADER)_INVALID"):
        _evaluate(module, inventory, dist)


def test_repeatable_nonidentity_core_metadata_headers_remain_supported(tmp_path: Path) -> None:
    module = load_gate()
    inventory, dist, _ = write_inventory(tmp_path)
    filename = NAMES[2]
    metadata = b"Metadata-Version: 2.4\nName: dpone\nVersion: 0.74.0\nRequires-Dist: first\nRequires-Dist: second\n\n"
    replace_candidate(
        inventory,
        dist,
        filename,
        candidate_bytes(filename, metadata_name="dpone", metadata_version=VERSION, metadata=metadata),
    )

    module.evaluate_prepublication(
        inventory,
        dist,
        expected_version=VERSION,
        context=publication_context(module),
        fetcher=lambda *_: None,
    )


def _wheel_with_unsafe_member(filename: str, mutation: str) -> bytes:
    package = package_for(filename)
    parent = f"{filename.split('-', 1)[0]}-{VERSION}.dist-info"
    core = f"Metadata-Version: 2.4\nName: {package}\nVersion: {VERSION}\n\n".encode()
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
        metadata_name = f"{parent}/METADATA"
        if mutation == "metadata-symlink":
            info = zipfile.ZipInfo(metadata_name)
            info.external_attr = (stat.S_IFLNK | 0o777) << 16
            archive.writestr(info, b"target")
        else:
            archive.writestr(metadata_name, core)
        if mutation == "traversal":
            archive.writestr("../escape.py", b"")
        elif mutation == "nested-metadata":
            archive.writestr(f"nested/{parent}/METADATA", core)
    return buffer.getvalue()


def _sdist_with_hardlinked_metadata(filename: str) -> bytes:
    parent = filename[:-7]
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
        directory = tarfile.TarInfo(parent)
        directory.type = tarfile.DIRTYPE
        archive.addfile(directory)
        metadata = tarfile.TarInfo(f"{parent}/PKG-INFO")
        metadata.type = tarfile.LNKTYPE
        metadata.linkname = f"{parent}/foreign"
        archive.addfile(metadata)
    return buffer.getvalue()


class _ReadBudget(io.BytesIO):
    """Fail if a streaming parser advances into a deliberately large payload."""

    def __init__(self, value: bytes, *, budget: int) -> None:
        super().__init__(value)
        self.budget = budget
        self.bytes_read = 0

    def read(self, size: int | None = -1) -> bytes:
        value = super().read(size)
        self._record(len(value))
        return value

    def readinto(self, buffer: Buffer) -> int:
        size = super().readinto(buffer)
        self._record(size)
        return size

    def _record(self, size: int) -> None:
        self.bytes_read += size
        if self.bytes_read > self.budget:
            raise AssertionError("sdist parser advanced into the rejected member payload")


def _sdist_with_oversized_payload(filename: str, payload: bytes) -> bytes:
    parent = filename[:-7]
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz", format=tarfile.PAX_FORMAT) as archive:
        directory = tarfile.TarInfo(parent)
        directory.type = tarfile.DIRTYPE
        archive.addfile(directory)
        member = tarfile.TarInfo(f"{parent}/large.bin")
        member.size = len(payload)
        archive.addfile(member, io.BytesIO(payload))
    return buffer.getvalue()


@pytest.mark.parametrize("mutation", ["metadata-symlink", "traversal", "nested-metadata"])
def test_wheel_link_path_or_ambiguous_metadata_fails_closed(tmp_path: Path, mutation: str) -> None:
    module = load_gate()
    inventory, dist, _ = write_inventory(tmp_path)
    filename = NAMES[2]
    replace_candidate(inventory, dist, filename, _wheel_with_unsafe_member(filename, mutation))

    with pytest.raises(module.PrepublicationGateError, match="ARCHIVE_|CORE_METADATA_"):
        _evaluate(module, inventory, dist)


def test_sdist_hardlinked_metadata_fails_closed(tmp_path: Path) -> None:
    module = load_gate()
    inventory, dist, _ = write_inventory(tmp_path)
    filename = NAMES[3]
    replace_candidate(inventory, dist, filename, _sdist_with_hardlinked_metadata(filename))

    with pytest.raises(module.PrepublicationGateError, match="ARCHIVE_LINK_OR_SPECIAL"):
        _evaluate(module, inventory, dist)


def test_sdist_rejects_oversized_header_before_streaming_member_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tools import pypi_core_metadata

    filename = NAMES[3]
    content = _sdist_with_oversized_payload(filename, random.Random(0).randbytes(2 * 1024 * 1024))
    stream = _ReadBudget(content, budget=192 * 1024)
    monkeypatch.setattr(pypi_core_metadata, "MAX_MEMBER_BYTES", 1024)

    with pytest.raises(pypi_core_metadata.PrepublicationGateError, match="ARCHIVE_MEMBER_SIZE_INVALID"):
        pypi_core_metadata._sdist_metadata(  # noqa: SLF001 - exercises the streaming boundary directly.
            stream,
            package="dpone",
            version=VERSION,
            archive_size=len(content),
        )

    assert stream.bytes_read < len(content) // 8


@pytest.mark.parametrize(
    ("limit", "error"),
    [
        ("MAX_ARCHIVE_MEMBERS", "ARCHIVE_MEMBER_COUNT_INVALID"),
        ("MAX_MEMBER_BYTES", "ARCHIVE_MEMBER_SIZE_INVALID"),
        ("MAX_TOTAL_UNCOMPRESSED_BYTES", "ARCHIVE_TOTAL_SIZE_INVALID"),
        ("MAX_COMPRESSION_RATIO", "ARCHIVE_COMPRESSION_INVALID"),
    ],
)
@pytest.mark.parametrize("filename", [NAMES[2], NAMES[3]])
def test_archive_resource_limits_fail_before_network(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    limit: str,
    error: str,
    filename: str,
) -> None:
    module = load_gate()
    from tools import pypi_core_metadata

    package = package_for(filename)
    metadata = f"Metadata-Version: 2.4\nName: {package}\nVersion: {VERSION}\n\n".encode() + b"x" * 10_000
    content = candidate_bytes(
        filename,
        metadata_name=package,
        metadata_version=VERSION,
        metadata=metadata,
    )
    candidate = tmp_path / filename
    candidate.write_bytes(content)
    monkeypatch.setattr(pypi_core_metadata, limit, 1)

    file_fd = os.open(candidate, os.O_RDONLY)
    try:
        with pytest.raises(module.PrepublicationGateError, match=error):
            pypi_core_metadata.verify_archive_core_metadata(
                file_fd,
                filename=filename,
                package=package,
                version=VERSION,
                artifact_type="wheel" if filename.endswith(".whl") else "sdist",
                archive_size=len(content),
            )
    finally:
        os.close(file_fd)
