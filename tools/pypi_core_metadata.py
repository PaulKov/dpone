"""Bounded stdlib proof of internal Core Metadata for PyPI candidates."""

from __future__ import annotations

import gzip
import io
import os
import re
import stat
import struct
import tarfile
import zipfile
from pathlib import PurePosixPath
from typing import IO, BinaryIO, Final

if __package__:
    from .pypi_prepublication_contract import PrepublicationGateError, fail
else:
    from pypi_prepublication_contract import PrepublicationGateError, fail

MAX_ARCHIVE_MEMBERS: Final = 4096
MAX_MEMBER_BYTES: Final = 128 * 1024 * 1024
MAX_TOTAL_UNCOMPRESSED_BYTES: Final = 512 * 1024 * 1024
MAX_CORE_METADATA_BYTES: Final = 1024 * 1024
MAX_HEADER_LINE_BYTES: Final = 16 * 1024
MAX_COMPRESSION_RATIO: Final = 1000
MAX_ZIP_CENTRAL_DIRECTORY_BYTES: Final = 16 * 1024 * 1024
MAX_TAR_CONTROL_BYTES: Final = 64 * 1024
_TAR_WINDOW = MAX_TAR_CONTROL_BYTES + tarfile.RECORDSIZE
_MAX_TAR_STREAM_BYTES = MAX_TOTAL_UNCOMPRESSED_BYTES + (MAX_ARCHIVE_MEMBERS + 1) * _TAR_WINDOW
_ZIP_TAIL_BYTES = 22 + 65535
_EOCD = struct.Struct("<4s4H2IH")
_ZIP64_LOCATOR = struct.Struct("<4sIQI")
_ZIP64_EOCD = struct.Struct("<4sQ2H2I4Q")
_NAME = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?$")
_FIELD = re.compile(r"^[A-Za-z0-9-]+$")
_BUILD_TAG = re.compile(r"^[0-9][0-9A-Za-z_.]*$")


def canonical_distribution_name(value: str) -> str:
    """Return the PyPA normalized project name after strict syntax validation."""

    if _NAME.fullmatch(value) is None:
        raise ValueError("invalid distribution name")
    return re.sub(r"[-_.]+", "-", value).lower()


def verify_archive_core_metadata(
    file_fd: int,
    *,
    filename: str,
    package: str,
    version: str,
    artifact_type: str,
    archive_size: int,
) -> None:
    """Bind filename, inventory identity, and unique internal Core Metadata."""

    _verify_filename(filename, package=package, version=version, artifact_type=artifact_type)
    try:
        os.lseek(file_fd, 0, os.SEEK_SET)
        with os.fdopen(os.dup(file_fd), "rb", closefd=True) as stream:
            if artifact_type == "wheel":
                metadata = _wheel_metadata(stream, package=package, version=version, archive_size=archive_size)
            elif artifact_type == "sdist":
                metadata = _sdist_metadata(
                    stream,
                    package=package,
                    version=version,
                    archive_size=archive_size,
                )
            else:
                raise fail("PYPI_PREPUBLICATION_CORE_METADATA_FORMAT_INVALID", package=package, filename=filename)
        _verify_core_metadata(metadata, package=package, version=version, filename=filename)
    except PrepublicationGateError:
        raise
    except (EOFError, OSError, RuntimeError, tarfile.TarError, zipfile.BadZipFile, zipfile.LargeZipFile) as exc:
        raise fail("PYPI_PREPUBLICATION_ARCHIVE_MALFORMED", package=package, filename=filename) from exc


def _verify_filename(filename: str, *, package: str, version: str, artifact_type: str) -> None:
    try:
        if artifact_type == "wheel" and filename.endswith(".whl"):
            parts = filename[:-4].split("-")
            valid_count = len(parts) == 5 or (len(parts) == 6 and _BUILD_TAG.fullmatch(parts[2]) is not None)
            filename_name, filename_version = parts[0], parts[1]
        elif artifact_type == "sdist" and filename.endswith(".tar.gz"):
            filename_name, filename_version = filename[:-7].rsplit("-", 1)
            valid_count = True
        else:
            raise ValueError("unsupported candidate filename")
        if not valid_count or canonical_distribution_name(filename_name) != package or filename_version != version:
            raise ValueError("candidate filename identity differs")
    except (IndexError, ValueError) as exc:
        raise fail("PYPI_PREPUBLICATION_FILENAME_IDENTITY_INVALID", package=package, filename=filename) from exc


def _wheel_metadata(stream: BinaryIO, *, package: str, version: str, archive_size: int) -> bytes:
    expected_count = _zip_directory_preflight(stream, archive_size=archive_size, package=package)
    with zipfile.ZipFile(stream, mode="r") as archive:
        members = archive.infolist()
        if len(members) != expected_count:
            raise fail("PYPI_PREPUBLICATION_ARCHIVE_MEMBER_COUNT_INVALID", package=package)
        names: set[str] = set()
        metadata_members: list[zipfile.ZipInfo] = []
        total = 0
        for member in members:
            parts = _safe_parts(member.filename)
            if member.filename in names:
                raise fail("PYPI_PREPUBLICATION_ARCHIVE_MEMBER_DUPLICATE", package=package)
            names.add(member.filename)
            mode = member.external_attr >> 16
            file_type = stat.S_IFMT(mode)
            is_directory = member.is_dir()
            inconsistent_type = (file_type == stat.S_IFDIR and not is_directory) or (
                file_type == stat.S_IFREG and is_directory
            )
            if file_type not in {0, stat.S_IFREG, stat.S_IFDIR} or inconsistent_type:
                raise fail("PYPI_PREPUBLICATION_ARCHIVE_LINK_OR_SPECIAL", package=package)
            if member.flag_bits & 0x1:
                raise fail("PYPI_PREPUBLICATION_ARCHIVE_ENCRYPTED", package=package)
            if not is_directory:
                _member_size(member.file_size, package=package)
                total += member.file_size
                _compression_bound(member.file_size, member.compress_size, archive_size, package=package)
            if parts[-1] == "METADATA" and len(parts) >= 2 and parts[-2].endswith(".dist-info"):
                if len(parts) != 2:
                    raise fail("PYPI_PREPUBLICATION_CORE_METADATA_AMBIGUOUS", package=package)
                if is_directory or file_type not in {0, stat.S_IFREG}:
                    raise fail("PYPI_PREPUBLICATION_CORE_METADATA_NONREGULAR", package=package)
                _metadata_parent(parts[0][:-10], package=package, version=version)
                metadata_members.append(member)
        _total_size(total, package=package)
        if len(metadata_members) != 1:
            raise fail("PYPI_PREPUBLICATION_CORE_METADATA_AMBIGUOUS", package=package)
        member = metadata_members[0]
        if member.file_size > MAX_CORE_METADATA_BYTES:
            raise fail("PYPI_PREPUBLICATION_CORE_METADATA_OVERSIZED", package=package)
        with archive.open(member, mode="r") as metadata_stream:
            return _bounded_metadata_read(metadata_stream, expected_size=member.file_size, package=package)


def _sdist_metadata(stream: BinaryIO, *, package: str, version: str, archive_size: int) -> bytes:
    with gzip.GzipFile(fileobj=stream, mode="rb") as decompressed:
        budget = _TarBudgetReader(decompressed, package=package)
        return _stream_sdist_metadata(budget, package=package, version=version, archive_size=archive_size)


def _stream_sdist_metadata(
    budget: _TarBudgetReader,
    *,
    package: str,
    version: str,
    archive_size: int,
) -> bytes:
    with tarfile.open(fileobj=budget, mode="r|") as archive:
        member_count = 0
        names: set[str] = set()
        metadata: bytes | None = None
        top_level: str | None = None
        total = 0
        for member in archive:
            member_count += 1
            if member_count > MAX_ARCHIVE_MEMBERS:
                raise fail("PYPI_PREPUBLICATION_ARCHIVE_MEMBER_COUNT_INVALID")
            parts = _safe_parts(member.name)
            if member.name in names:
                raise fail("PYPI_PREPUBLICATION_ARCHIVE_MEMBER_DUPLICATE", package=package)
            names.add(member.name)
            top_level = parts[0] if top_level is None else top_level
            if parts[0] != top_level:
                raise fail("PYPI_PREPUBLICATION_SDIST_LAYOUT_INVALID", package=package)
            if member.issym() or member.islnk() or not (member.isfile() or member.isdir()):
                raise fail("PYPI_PREPUBLICATION_ARCHIVE_LINK_OR_SPECIAL", package=package)
            if member.isfile():
                _member_size(member.size, package=package)
                total += member.size
                _total_size(total, package=package)
                _compression_bound(total, archive_size, archive_size, package=package)
            elif member.size != 0:
                raise fail("PYPI_PREPUBLICATION_ARCHIVE_MEMBER_SIZE_INVALID", package=package)
            budget.authorize(member)
            if parts[-1] == "PKG-INFO":
                if len(parts) != 2:
                    raise fail("PYPI_PREPUBLICATION_CORE_METADATA_AMBIGUOUS", package=package)
                if not member.isfile():
                    raise fail("PYPI_PREPUBLICATION_CORE_METADATA_NONREGULAR", package=package)
                if metadata is not None:
                    raise fail("PYPI_PREPUBLICATION_CORE_METADATA_AMBIGUOUS", package=package)
                _metadata_parent(parts[0], package=package, version=version)
                if member.size > MAX_CORE_METADATA_BYTES:
                    raise fail("PYPI_PREPUBLICATION_CORE_METADATA_OVERSIZED", package=package)
                metadata_stream = archive.extractfile(member)
                if metadata_stream is None:
                    raise fail("PYPI_PREPUBLICATION_CORE_METADATA_NONREGULAR", package=package)
                with metadata_stream:
                    metadata = _bounded_metadata_read(metadata_stream, expected_size=member.size, package=package)
        if member_count == 0:
            raise fail("PYPI_PREPUBLICATION_ARCHIVE_MEMBER_COUNT_INVALID")
        if metadata is None:
            raise fail("PYPI_PREPUBLICATION_CORE_METADATA_AMBIGUOUS", package=package)
        return metadata


class _TarBudgetReader(io.RawIOBase):
    """Expose only decompressed bytes authorized by validated visible headers."""

    def __init__(self, stream: gzip.GzipFile, *, package: str) -> None:
        self._stream = stream
        self._package = package
        self._position = 0
        self._limit = _TAR_WINDOW

    def readable(self) -> bool:
        return True

    def tell(self) -> int:
        return self._position

    def read(self, size: int | None = -1) -> bytes:
        remaining = self._limit - self._position
        requested = remaining + 1 if size is None or size < 0 else min(size, remaining + 1)
        value = self._stream.read(requested)
        if len(value) > remaining:
            raise fail("PYPI_PREPUBLICATION_ARCHIVE_CONTROL_SIZE_INVALID", package=self._package)
        self._position += len(value)
        return value

    def authorize(self, member: tarfile.TarInfo) -> None:
        size = member.size if member.isfile() else 0
        padded_size = ((size + tarfile.BLOCKSIZE - 1) // tarfile.BLOCKSIZE) * tarfile.BLOCKSIZE
        limit = member.offset_data + padded_size + _TAR_WINDOW
        if limit < self._position or limit > _MAX_TAR_STREAM_BYTES:
            raise fail("PYPI_PREPUBLICATION_ARCHIVE_CONTROL_SIZE_INVALID", package=self._package)
        self._limit = limit


def _zip_directory_preflight(stream: BinaryIO, *, archive_size: int, package: str) -> int:
    if archive_size < _EOCD.size:
        raise fail("PYPI_PREPUBLICATION_ARCHIVE_DIRECTORY_INVALID", package=package)
    tail_size = min(archive_size, _ZIP_TAIL_BYTES)
    tail_offset = archive_size - tail_size
    tail = _read_exact(stream, tail_offset, tail_size, package=package)
    relative = tail.rfind(b"PK\x05\x06")
    if relative < 0 or relative + _EOCD.size > len(tail):
        raise fail("PYPI_PREPUBLICATION_ARCHIVE_DIRECTORY_INVALID", package=package)
    eocd_offset = tail_offset + relative
    _, disk, directory_disk, disk_count, count, size, offset, comment_size = _EOCD.unpack_from(tail, relative)
    if eocd_offset + _EOCD.size + comment_size != archive_size or disk != 0 or directory_disk != 0:
        raise fail("PYPI_PREPUBLICATION_ARCHIVE_DIRECTORY_INVALID", package=package)
    directory_end = eocd_offset
    compact = (disk_count, count, size, offset)
    sentinels = (0xFFFF, 0xFFFF, 0xFFFFFFFF, 0xFFFFFFFF)
    has_sentinel = any(value == sentinel for value, sentinel in zip(compact, sentinels, strict=True))
    has_zip64 = (
        eocd_offset >= _ZIP64_LOCATOR.size
        and _read_exact(stream, eocd_offset - _ZIP64_LOCATOR.size, 4, package=package) == b"PK\x06\x07"
    )
    if has_zip64:
        disk_count, count, size, offset, directory_end = _zip64_directory(
            stream,
            eocd_offset=eocd_offset,
            compact=compact,
            package=package,
        )
    elif has_sentinel:
        raise fail("PYPI_PREPUBLICATION_ARCHIVE_DIRECTORY_INVALID", package=package)
    if disk_count != count or not 0 < count <= MAX_ARCHIVE_MEMBERS:
        raise fail("PYPI_PREPUBLICATION_ARCHIVE_MEMBER_COUNT_INVALID", package=package)
    if size < count * 46 or size > MAX_ZIP_CENTRAL_DIRECTORY_BYTES or offset + size != directory_end:
        raise fail("PYPI_PREPUBLICATION_ARCHIVE_DIRECTORY_INVALID", package=package)
    if _read_exact(stream, offset, 4, package=package) != b"PK\x01\x02":
        raise fail("PYPI_PREPUBLICATION_ARCHIVE_DIRECTORY_INVALID", package=package)
    stream.seek(0)
    return count


def _zip64_directory(
    stream: BinaryIO,
    *,
    eocd_offset: int,
    compact: tuple[int, int, int, int],
    package: str,
) -> tuple[int, int, int, int, int]:
    locator_offset = eocd_offset - _ZIP64_LOCATOR.size
    locator = _read_exact(stream, locator_offset, _ZIP64_LOCATOR.size, package=package)
    signature, record_disk, record_offset, disk_total = _ZIP64_LOCATOR.unpack(locator)
    record = _read_exact(stream, record_offset, _ZIP64_EOCD.size, package=package)
    fields = _ZIP64_EOCD.unpack(record)
    if (
        signature != b"PK\x06\x07"
        or record_disk != 0
        or disk_total != 1
        or fields[0] != b"PK\x06\x06"
        or fields[1] != 44
        or fields[4] != 0
        or fields[5] != 0
        or record_offset + _ZIP64_EOCD.size != locator_offset
    ):
        raise fail("PYPI_PREPUBLICATION_ARCHIVE_DIRECTORY_INVALID", package=package)
    authoritative = (fields[6], fields[7], fields[8], fields[9])
    if any(
        old != sentinel and old != new
        for old, sentinel, new in zip(compact, (0xFFFF,) * 2 + (0xFFFFFFFF,) * 2, authoritative, strict=True)
    ):
        raise fail("PYPI_PREPUBLICATION_ARCHIVE_DIRECTORY_INVALID", package=package)
    return (*authoritative, record_offset)


def _read_exact(stream: BinaryIO, offset: int, size: int, *, package: str) -> bytes:
    if offset < 0:
        raise fail("PYPI_PREPUBLICATION_ARCHIVE_DIRECTORY_INVALID", package=package)
    stream.seek(offset)
    value = stream.read(size)
    if len(value) != size:
        raise fail("PYPI_PREPUBLICATION_ARCHIVE_DIRECTORY_INVALID", package=package)
    return value


def _verify_core_metadata(raw: bytes, *, package: str, version: str, filename: str) -> None:
    if len(raw) > MAX_CORE_METADATA_BYTES or b"\x00" in raw or b"\r" in raw.replace(b"\r\n", b""):
        raise fail("PYPI_PREPUBLICATION_CORE_METADATA_ENCODING_INVALID", package=package, filename=filename)
    try:
        text = raw.decode("utf-8", "strict").replace("\r\n", "\n")
    except UnicodeDecodeError as exc:
        raise fail("PYPI_PREPUBLICATION_CORE_METADATA_ENCODING_INVALID", package=package, filename=filename) from exc
    identity_fields: dict[str, str] = {}
    for line in text.split("\n"):
        if not line:
            break
        if line[0] in " \t" or len(line.encode("utf-8")) > MAX_HEADER_LINE_BYTES or ":" not in line:
            raise fail("PYPI_PREPUBLICATION_CORE_METADATA_HEADER_INVALID", package=package, filename=filename)
        key, raw_value = line.split(":", 1)
        lowered = key.lower()
        value = raw_value.strip(" \t")
        duplicate_identity = lowered in {"name", "version"} and lowered in identity_fields
        if _FIELD.fullmatch(key) is None or duplicate_identity or not value or any(ord(char) < 32 for char in value):
            raise fail("PYPI_PREPUBLICATION_CORE_METADATA_HEADER_INVALID", package=package, filename=filename)
        if lowered in {"name", "version"}:
            identity_fields[lowered] = value
    name, observed_version = identity_fields.get("name"), identity_fields.get("version")
    try:
        normalized_name = canonical_distribution_name(name or "")
    except ValueError as exc:
        raise fail("PYPI_PREPUBLICATION_CORE_METADATA_IDENTITY_INVALID", package=package, filename=filename) from exc
    if normalized_name != package or observed_version != version:
        raise fail("PYPI_PREPUBLICATION_CORE_METADATA_IDENTITY_INVALID", package=package, filename=filename)


def _safe_parts(name: str) -> tuple[str, ...]:
    candidate = name[:-1] if name.endswith("/") else name
    raw_parts = candidate.split("/")
    path = PurePosixPath(candidate)
    parts = path.parts
    if (
        not name
        or not candidate
        or "\\" in name
        or path.is_absolute()
        or not parts
        or any(part in {"", ".", ".."} for part in raw_parts)
        or tuple(raw_parts) != parts
    ):
        raise fail("PYPI_PREPUBLICATION_ARCHIVE_PATH_INVALID")
    return parts


def _metadata_parent(value: str, *, package: str, version: str) -> None:
    try:
        name, observed_version = value.rsplit("-", 1)
        if canonical_distribution_name(name) != package or observed_version != version:
            raise ValueError("metadata parent identity differs")
    except ValueError as exc:
        raise fail("PYPI_PREPUBLICATION_CORE_METADATA_PARENT_INVALID", package=package) from exc


def _member_size(size: int, *, package: str) -> None:
    if size < 0 or size > MAX_MEMBER_BYTES:
        raise fail("PYPI_PREPUBLICATION_ARCHIVE_MEMBER_SIZE_INVALID", package=package)


def _total_size(size: int, *, package: str) -> None:
    if size > MAX_TOTAL_UNCOMPRESSED_BYTES:
        raise fail("PYPI_PREPUBLICATION_ARCHIVE_TOTAL_SIZE_INVALID", package=package)


def _compression_bound(uncompressed: int, compressed: int, archive_size: int, *, package: str) -> None:
    if uncompressed > MAX_COMPRESSION_RATIO * max(1, compressed) or uncompressed > MAX_COMPRESSION_RATIO * archive_size:
        raise fail("PYPI_PREPUBLICATION_ARCHIVE_COMPRESSION_INVALID", package=package)


def _bounded_metadata_read(stream: IO[bytes], *, expected_size: int, package: str) -> bytes:
    raw = stream.read(MAX_CORE_METADATA_BYTES + 1)
    if len(raw) != expected_size or len(raw) > MAX_CORE_METADATA_BYTES or stream.read(1):
        raise fail("PYPI_PREPUBLICATION_CORE_METADATA_SIZE_INVALID", package=package)
    return raw


__all__ = ["canonical_distribution_name", "verify_archive_core_metadata"]
