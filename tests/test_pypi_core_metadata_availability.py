"""Early bounded-read contracts for candidate archive control structures."""

from __future__ import annotations

import io
import random
import struct
import tarfile
from pathlib import Path

import pytest
from tools import pypi_core_metadata

from tests.pypi_prepublication_test_support import (
    NAMES,
    VERSION,
    candidate_bytes,
    load_gate,
    publication_context,
    replace_candidate,
    write_inventory,
)
from tests.test_pypi_core_metadata import _ReadBudget


def _hidden_control_sdist(kind: str) -> bytes:
    parent = NAMES[3][:-7]
    core = f"Metadata-Version: 2.4\nName: dpone\nVersion: {VERSION}\n\n".encode()
    noise = random.Random(1).randbytes(512 * 1024).hex()
    buffer = io.BytesIO()
    archive_format = tarfile.PAX_FORMAT if kind == "pax" else tarfile.GNU_FORMAT
    with tarfile.open(fileobj=buffer, mode="w:gz", format=archive_format) as archive:
        member = tarfile.TarInfo(f"{parent}/PKG-INFO" if kind == "pax" else f"{parent}/{noise}")
        member.size = len(core) if kind == "pax" else 0
        if kind == "pax":
            member.pax_headers = {"comment": noise}
        archive.addfile(member, io.BytesIO(core) if member.size else None)
    return buffer.getvalue()


def _small_pax_sdist() -> bytes:
    parent = NAMES[3][:-7]
    core = f"Metadata-Version: 2.4\nName: dpone\nVersion: {VERSION}\n\n".encode()
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz", format=tarfile.PAX_FORMAT) as archive:
        member = tarfile.TarInfo(f"{parent}/PKG-INFO")
        member.size = len(core)
        member.pax_headers = {"comment": "bounded release metadata"}
        archive.addfile(member, io.BytesIO(core))
    return buffer.getvalue()


def _fake_zip_directory(*, count: int, directory_size: int) -> bytes:
    directory = random.Random(2).randbytes(2 * 1024 * 1024)
    return directory + struct.pack("<4s4H2IH", b"PK\x05\x06", 0, 0, count, count, directory_size, 0, 0)


def _zip64_compact_decoy(comment_size: int) -> bytes:
    directory = b"PK\x01\x02" + b"\0" * 42
    record = struct.pack("<4sQ2H2I4Q", b"PK\x06\x06", 44, 45, 45, 0, 0, 5000, 5000, 32 * 1024 * 1024, 0)
    locator = struct.pack("<4sIQI", b"PK\x06\x07", 0, len(directory), 1)
    size = len(directory) + len(record) + len(locator)
    eocd = struct.pack("<4s4H2IH", b"PK\x05\x06", 0, 0, 1, 1, size, 0, comment_size)
    return directory + record + locator + eocd + b"\0" * comment_size


@pytest.mark.parametrize("kind", ["pax", "gnu"])
def test_hidden_tar_control_is_bounded_before_visible_member(kind: str) -> None:
    content = _hidden_control_sdist(kind)
    stream = _ReadBudget(content, budget=256 * 1024)
    with pytest.raises(pypi_core_metadata.PrepublicationGateError, match="ARCHIVE_CONTROL_SIZE_INVALID"):
        pypi_core_metadata._sdist_metadata(  # noqa: SLF001
            stream, package="dpone", version=VERSION, archive_size=len(content)
        )
    assert stream.bytes_read < len(content) // 2


def test_standard_pax_sdist_remains_supported(tmp_path: Path) -> None:
    module = load_gate()
    inventory, dist, _ = write_inventory(tmp_path)
    replace_candidate(inventory, dist, NAMES[3], _small_pax_sdist())
    module.evaluate_prepublication(
        inventory, dist, expected_version=VERSION, context=publication_context(module), fetcher=lambda *_: None
    )


@pytest.mark.parametrize(
    ("count", "directory_size", "limit", "error"),
    [(5000, 46, None, "MEMBER_COUNT_INVALID"), (1, 2 * 1024 * 1024, 1024, "DIRECTORY_INVALID")],
)
def test_zip_count_and_size_are_rejected_from_bounded_tail(
    monkeypatch: pytest.MonkeyPatch, count: int, directory_size: int, limit: int | None, error: str
) -> None:
    content = _fake_zip_directory(count=count, directory_size=directory_size)
    stream = _ReadBudget(content, budget=96 * 1024)
    if limit is not None:
        monkeypatch.setattr(pypi_core_metadata, "MAX_ZIP_CENTRAL_DIRECTORY_BYTES", limit)
    with pytest.raises(pypi_core_metadata.PrepublicationGateError, match=error):
        pypi_core_metadata._wheel_metadata(  # noqa: SLF001
            stream, package="dpone", version=VERSION, archive_size=len(content)
        )
    assert stream.bytes_read <= 96 * 1024


def test_zip_observed_count_must_equal_eocd() -> None:
    content = bytearray(candidate_bytes(NAMES[2], metadata_name="dpone", metadata_version=VERSION))
    eocd = content.rfind(b"PK\x05\x06")
    assert eocd >= 0
    struct.pack_into("<HH", content, eocd + 8, 1, 1)
    with pytest.raises(pypi_core_metadata.PrepublicationGateError, match="MEMBER_COUNT_INVALID"):
        pypi_core_metadata._wheel_metadata(  # noqa: SLF001
            io.BytesIO(content), package="dpone", version=VERSION, archive_size=len(content)
        )


@pytest.mark.parametrize("comment_size", [0, 65535])
def test_zip64_locator_overrides_are_always_checked(comment_size: int) -> None:
    content = _zip64_compact_decoy(comment_size)
    with pytest.raises(pypi_core_metadata.PrepublicationGateError, match="DIRECTORY_INVALID"):
        pypi_core_metadata._zip_directory_preflight(  # noqa: SLF001
            io.BytesIO(content), archive_size=len(content), package="dpone"
        )
