"""Validate wheel RECORD checksums before excluding digest text from policy scans.

The caller enforces archive/path/size limits and scans every member body. This
module holds only bounded RECORD bytes and per-member SHA-256/size observations;
it never extracts files, imports package code, or loads the private policy.
"""

from __future__ import annotations

import base64
import csv
import hashlib
import io
import struct
from collections.abc import Callable
from typing import BinaryIO, cast


def check_directory(stream: BinaryIO, archive_size: int, *, max_members: int) -> None:
    """Bound the ZIP central directory before allocating its member inventory."""
    tail_size = min(archive_size, 65_557)
    stream.seek(archive_size - tail_size)
    tail = stream.read(tail_size)
    offset = tail.rfind(b"PK\x05\x06")
    if offset < 0 or len(tail) - offset < 22:
        raise ValueError
    _, disk, start_disk, disk_entries, entries, size, _, comment = struct.unpack_from("<4s4H2IH", tail, offset)
    if disk or start_disk or disk_entries != entries or offset + 22 + comment != len(tail):
        raise ValueError
    if entries > max_members:
        raise OverflowError
    if size > 64 * 1024**2:
        raise ValueError
    stream.seek(0)


class _DigestReader:
    def __init__(self, stream: BinaryIO) -> None:
        self.stream = stream
        self.digest = hashlib.sha256()
        self.size = 0

    def read(self, size: int = -1) -> bytes:
        block = self.stream.read(size)
        self.digest.update(block)
        self.size += len(block)
        return block


class WheelRecordInspector:
    """Accumulate observations during the existing bounded archive scan."""

    def __init__(self) -> None:
        self.members: dict[str, tuple[str, str]] = {}
        self.record: tuple[str, bytes] | None = None

    def scan(
        self,
        name: str,
        stream: BinaryIO,
        size: int,
        scan_body: Callable[[BinaryIO, int], tuple[bool, bool]],
    ) -> tuple[bool, bool]:
        parts = name.split("/")
        if len(parts) == 2 and parts[0].endswith(".dist-info") and parts[1] == "RECORD":
            if self.record is not None:
                raise ValueError("Multiple wheel records")
            body = stream.read(size + 1)
            if len(body) != size:
                raise ValueError("Invalid record size")
            self.record = name, body
            return False, False
        reader = _DigestReader(stream)
        result = scan_body(cast(BinaryIO, reader), size)
        checksum = base64.urlsafe_b64encode(reader.digest.digest()).decode("ascii").rstrip("=")
        self.members[name] = "sha256=" + checksum, str(reader.size)
        return result

    def verified_text(self) -> bytes:
        """Return CSV without proven digests, or fail closed on any ambiguity.

        SHA-256 is the supported release builder format. No checksum-shaped
        string is trusted without exact equality to the scanned member bytes.
        Archive fragments without RECORD retain their existing scan behavior.
        """
        if self.record is None:
            return b""
        name, body = self.record
        expected = {**self.members, name: ("", "")}
        seen: set[str] = set()
        text = io.StringIO(newline="")
        writer = csv.writer(text)
        rows = csv.reader(io.StringIO(body.decode("utf-8"), newline=""), strict=True)
        for row in rows:
            if len(row) != 3:
                raise ValueError("Invalid record row")
            path, checksum, size = row
            if path in seen or expected.get(path) != (checksum, size):
                raise ValueError("Unverified record entry")
            seen.add(path)
            writer.writerow([path, "", size])
        if seen != set(expected):
            raise ValueError("Incomplete record")
        return text.getvalue().encode("utf-8")
