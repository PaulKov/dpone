"""Bounded in-memory inspection of strict ZIP, gzip and USTAR containers.

Unknown metadata is rejected, never ignored. No member is extracted, imported or
unmarshalled. Container names, headers, comments and raw bytes remain scan inputs.
"""

from __future__ import annotations

import hashlib
import io
import re
import stat
import struct
import zipfile
import zlib

from tools.agent_policy import tenant_hygiene as limits
from tools.agent_policy.public_clean_policy import Scanner
from tools.agent_policy.public_clean_receipts import GateError

_ARCHIVE_SUFFIXES = (".zip", ".whl", ".tar", ".gz", ".tgz", ".bz2", ".xz", ".7z", ".rar", ".tbz2", ".txz")


def _kind(raw: bytes) -> str | None:
    if raw.startswith((b"PK\x03\x04", b"PK\x05\x06")):
        return "zip"
    if raw.startswith(b"\x1f\x8b"):
        return "gzip"
    if raw[257:263] in (b"ustar\0", b"ustar "):
        return "tar"
    return None


def _name(value: str, seen: set[str]) -> None:
    path = value.removesuffix("/")
    if (
        not path
        or path.startswith("/")
        or "\\" in path
        or ":" in path
        or any(part in {"", ".", ".."} for part in path.split("/"))
        or any(ord(char) < 32 or ord(char) == 127 for char in path)
        or path in seen
        or len(value.encode()) > limits.MAX_PATH_BYTES
    ):
        raise GateError("ARCHIVE_PATH_UNSAFE")
    seen.add(path)


def _member(label: str, name: str, raw: bytes, scanner: Scanner) -> None:
    if _kind(raw) or name.lower().endswith(_ARCHIVE_SUFFIXES):
        raise GateError("NESTED_ARCHIVE_UNSUPPORTED")
    inspect_content(f"{label}!{name}", raw, scanner)


def _inflate(raw: bytes, scanner: Scanner, *, wbits: int, size: int | None = None) -> bytes:
    remaining = min(
        limits.MAX_ARCHIVE_MEMBER_BYTES, limits.MAX_ARCHIVE_UNCOMPRESSED_BYTES - scanner.budget.archive_bytes
    )
    if size is not None and size > remaining:
        raise GateError("ARCHIVE_EXPANSION_LIMIT")
    limit = remaining if size is None else size
    stream = zlib.decompressobj(wbits)
    output = bytearray()
    for start in range(0, len(raw), limits.READ_CHUNK_BYTES):
        scanner.budget.tick()
        output.extend(stream.decompress(raw[start : start + limits.READ_CHUNK_BYTES], limit + 1 - len(output)))
        if len(output) > limit or stream.unconsumed_tail:
            raise GateError("ARCHIVE_EXPANSION_LIMIT")
    if not stream.eof or stream.unused_data or (size is not None and len(output) != size):
        raise GateError("ARCHIVE_MALFORMED")
    if len(output) > max(1, len(raw)) * limits.MAX_COMPRESSION_RATIO:
        raise GateError("ARCHIVE_RATIO_LIMIT")
    return bytes(output)


def _gzip(label: str, raw: bytes, scanner: Scanner) -> None:
    if len(raw) < 18 or raw[2] != 8 or raw[3] & ~0x1B:
        raise GateError("ARCHIVE_METADATA_UNSUPPORTED")
    flags, cursor = raw[3], 10
    for flag in (8, 16):
        if flags & flag:
            end = raw.find(b"\0", cursor)
            if end < cursor or end > len(raw) - 8:
                raise GateError("ARCHIVE_MALFORMED")
            if flag == 8:
                _name(raw[cursor:end].decode("utf-8"), set())
            scanner.scan(f"{label}!header-{flag}", raw[cursor:end])
            cursor = end + 1
    if flags & 2:
        if cursor + 2 > len(raw) - 8 or zlib.crc32(raw[:cursor]) & 0xFFFF != int.from_bytes(
            raw[cursor : cursor + 2], "little"
        ):
            raise GateError("ARCHIVE_MALFORMED")
    expanded = _inflate(raw, scanner, wbits=31)
    scanner.budget.member(len(expanded))
    if _kind(expanded) == "tar":
        scanner.scan(f"{label}!expanded", expanded, textual=False)
        _tar(label, expanded, scanner)
    else:
        _member(label, "expanded", expanded, scanner)


def _zip(label: str, raw: bytes, scanner: Scanner) -> None:
    end = raw.rfind(b"PK\x05\x06")
    if end < 0 or end + 22 > len(raw):
        raise GateError("ARCHIVE_MALFORMED")
    _, disk, cd_disk, disk_count, count, cd_size, cd_start, comment = struct.unpack_from("<4s4H2IH", raw, end)
    if (
        disk
        or cd_disk
        or disk_count != count
        or cd_start + cd_size != end
        or end + 22 + comment != len(raw)
        or count > limits.MAX_ARCHIVE_MEMBERS
    ):
        raise GateError("ARCHIVE_MALFORMED")
    scanner.scan(f"{label}!comment", raw[end + 22 :])
    cursor, local_end = cd_start, 0
    seen: set[str] = set()
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        members = archive.infolist()
        if len(members) != count:
            raise GateError("ARCHIVE_MALFORMED")
        for member in members:
            scanner.budget.tick()
            if cursor + 46 > end:
                raise GateError("ARCHIVE_MALFORMED")
            fields = struct.unpack_from("<4s6H3I5H2I", raw, cursor)
            (
                signature,
                _,
                needed,
                flags,
                method,
                _,
                _,
                crc,
                compressed,
                size,
                nlen,
                xlen,
                clen,
                start_disk,
                internal,
                external,
                offset,
            ) = fields
            if (
                signature != b"PK\x01\x02"
                or flags & ~0x800
                or method not in {0, 8}
                or needed > 20
                or xlen
                or start_disk
                or internal & ~1
            ):
                raise GateError("ARCHIVE_METADATA_UNSUPPORTED")
            next_cursor = cursor + 46 + nlen + xlen + clen
            if next_cursor > end or offset != local_end or offset + 30 > cd_start:
                raise GateError("ARCHIVE_MALFORMED")
            name_bytes = raw[cursor + 46 : cursor + 46 + nlen]
            name = name_bytes.decode("utf-8" if flags & 0x800 else "cp437")
            _name(name, seen)
            scanner.scan(f"{label}!name", name.encode())
            scanner.scan(f"{label}!{name}!comment", raw[cursor + 46 + nlen : next_cursor])
            local = struct.unpack_from("<4s5H3I2H", raw, offset)
            if local != (b"PK\x03\x04", needed, flags, method, fields[5], fields[6], crc, compressed, size, nlen, 0):
                raise GateError("ARCHIVE_MALFORMED")
            begin = offset + 30 + nlen
            local_end = begin + compressed
            if local_end > cd_start or raw[offset + 30 : begin] != name_bytes:
                raise GateError("ARCHIVE_MALFORMED")
            file_type = stat.S_IFMT(external >> 16)
            directory = name.endswith("/")
            if (
                file_type not in {0, stat.S_IFREG, stat.S_IFDIR}
                or (file_type and directory != (file_type == stat.S_IFDIR))
                or (directory and size)
            ):
                raise GateError("ARCHIVE_MEMBER_UNSUPPORTED")
            if member.filename != name or member.header_offset != offset:
                raise GateError("ARCHIVE_MALFORMED")
            scanner.budget.member(size)
            if size > max(1, compressed) * limits.MAX_COMPRESSION_RATIO:
                raise GateError("ARCHIVE_RATIO_LIMIT")
            packed = raw[begin:local_end]
            content = _inflate(packed, scanner, wbits=-15, size=size) if method == 8 else packed
            if len(content) != size or zlib.crc32(content) != crc:
                raise GateError("ARCHIVE_MALFORMED")
            if not directory:
                _member(label, name, content, scanner)
            cursor = next_cursor
    if cursor != end or local_end != cd_start:
        raise GateError("ARCHIVE_MALFORMED")


def _octal(raw: bytes) -> int:
    value = raw.strip(b"\0 ")
    if not value or any(char not in b"01234567" for char in value):
        raise GateError("ARCHIVE_METADATA_UNSUPPORTED")
    return int(value, 8)


def _field(raw: bytes) -> str:
    value, separator, padding = raw.partition(b"\0")
    if separator and any(padding):
        raise GateError("ARCHIVE_METADATA_UNSUPPORTED")
    return value.decode("utf-8")


def _tar(label: str, raw: bytes, scanner: Scanner) -> None:
    cursor = 0
    seen: set[str] = set()
    if len(raw) % 512:
        raise GateError("ARCHIVE_MALFORMED")
    while cursor + 512 <= len(raw):
        scanner.budget.tick()
        header = raw[cursor : cursor + 512]
        if not any(header):
            if len(raw) - cursor < 1024 or any(raw[cursor:]):
                raise GateError("ARCHIVE_MALFORMED")
            return
        if (
            header[257:265] != b"ustar\x0000"
            or header[156:157] not in {b"0", b"\0", b"5"}
            or any(header[157:257])
            or any(header[500:])
        ):
            raise GateError("ARCHIVE_METADATA_UNSUPPORTED")
        if sum(header[:148]) + 8 * 32 + sum(header[156:]) != _octal(header[148:156]):
            raise GateError("ARCHIVE_MALFORMED")
        scanner.scan(f"{label}!header", header, textual=False)
        for start, end in ((100, 108), (108, 116), (116, 124), (136, 148)):
            _octal(header[start:end])
        for start, end in ((265, 297), (297, 329)):
            scanner.scan(f"{label}!owner-{start}", _field(header[start:end]).encode())
        if any(header[329:345].strip(b"\0 0")):
            raise GateError("ARCHIVE_METADATA_UNSUPPORTED")
        name, prefix = _field(header[:100]), _field(header[345:500])
        name = f"{prefix}/{name}" if prefix else name
        _name(name, seen)
        scanner.scan(f"{label}!name", name.encode())
        size = _octal(header[124:136])
        scanner.budget.member(size)
        begin, end = cursor + 512, cursor + 512 + size
        padded_end = ((end + 511) // 512) * 512
        if padded_end > len(raw) or any(raw[end:padded_end]):
            raise GateError("ARCHIVE_MALFORMED")
        if header[156:157] == b"5":
            if size:
                raise GateError("ARCHIVE_MALFORMED")
        else:
            _member(label, name, raw[begin:end], scanner)
        cursor = padded_end
    raise GateError("ARCHIVE_MALFORMED")


def inspect_content(label: str, raw: bytes, scanner: Scanner) -> None:
    """Inspect a full blob, refusing unsupported content without partial PASS."""
    compiled = label.lower().endswith((".pyc", ".pyo")) or (len(raw) >= 4 and raw[2:4] == b"\r\n" and raw[0] >= 128)
    if compiled:
        scanner.scan(label, raw, textual=False)
        for match in re.finditer(rb"[\x20-\x7e]{4,}", raw):
            scanner.scan(f"{label}!printable-{match.start()}", match.group())
        scanner.add("COMPILED_ARTIFACT", label, content_digest=hashlib.sha256(raw).hexdigest())
        return
    kind = _kind(raw)
    if kind:
        scanner.budget.archives += 1
        if scanner.budget.archives > limits.MAX_ARCHIVES or len(raw) > limits.MAX_ARCHIVE_FILE_BYTES:
            raise GateError("ARCHIVE_COUNT_OR_SIZE_LIMIT")
        scanner.scan(label, raw, textual=False)
        try:
            {"zip": _zip, "gzip": _gzip, "tar": _tar}[kind](label, raw, scanner)
        except GateError:
            raise
        except (ValueError, UnicodeError, struct.error, zipfile.BadZipFile, zlib.error, OverflowError) as exc:
            raise GateError("ARCHIVE_MALFORMED") from exc
    elif label.lower().endswith(_ARCHIVE_SUFFIXES):
        raise GateError("ARCHIVE_FORMAT_UNSUPPORTED")
    else:
        scanner.scan(label, raw)
