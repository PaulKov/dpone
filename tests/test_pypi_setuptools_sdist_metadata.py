"""Generated setuptools egg-info is redundant, never an identity authority."""

from __future__ import annotations

import io
import tarfile

import pytest
from tools import pypi_core_metadata as metadata

VERSION = "1.2.3"
PARENT = "sample_package-1.2.3"
CORE = b"Metadata-Version: 2.4\nName: sample-package\nVersion: 1.2.3\n\n"
AUX = f"{PARENT}/src/sample_package.egg-info/PKG-INFO"
ROOT = f"{PARENT}/PKG-INFO"


def _read(entries: list[tuple[str, bytes]]) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
        for name, value in entries:
            member = tarfile.TarInfo(name)
            member.size = len(value)
            archive.addfile(member, io.BytesIO(value))
    size = buffer.tell()
    buffer.seek(0)
    return metadata._sdist_metadata(buffer, package="sample-package", version=VERSION, archive_size=size)  # noqa: SLF001


@pytest.mark.parametrize("reverse", [False, True])
def test_identical_setuptools_metadata_preserves_root_authority(reverse: bool) -> None:
    entries = [(ROOT, CORE), (AUX, CORE)]
    assert _read(list(reversed(entries)) if reverse else entries) == CORE


@pytest.mark.parametrize(
    "entries",
    [
        [(AUX, CORE)],
        [(ROOT, CORE), (AUX, CORE + b"different description\n")],
        [(ROOT, CORE), (f"{PARENT}/src/foreign.egg-info/PKG-INFO", CORE)],
        [(ROOT, CORE), (f"{PARENT}/other/sample_package.egg-info/PKG-INFO", CORE)],
        [(ROOT, CORE), (ROOT, CORE)],
        [(ROOT, CORE), (AUX, CORE), (AUX, CORE)],
        [(ROOT, CORE), (AUX, CORE), (f"{PARENT}/src/sample-package.egg-info/PKG-INFO", CORE)],
    ],
)
def test_auxiliary_metadata_cannot_replace_or_conflict_with_root(entries: list[tuple[str, bytes]]) -> None:
    with pytest.raises(metadata.PrepublicationGateError, match="CORE_METADATA_|ARCHIVE_MEMBER_DUPLICATE"):
        _read(entries)
